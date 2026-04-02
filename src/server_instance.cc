#include "server_instance.h"

#include <linux/if_packet.h>
#include <net/if.h>
#include <pcap.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <optional>

#include "lib/utils.h"

using namespace c_m_proto;

ServerInstance::ServerInstance(
    const ServerInfo& server_info,
    std::shared_ptr<const SockConfig> sock_config,
    std::shared_ptr<const ControllerInfo> controller_info,
    std::shared_ptr<const ClusterInfo> clusters_info)
    : server_info_(server_info),
      sock_config_(std::move(sock_config)),
      controller_info_(std::move(controller_info)),
      clusters_info_(std::move(clusters_info)),
      fsm_(MIGRATION_NO) {
  server_ip_in_ = inet_addr(server_info_.ip.c_str());
  controller_ip_in_ = inet_addr(controller_info_->ip.c_str());
  index_base_ = server_info_.rack_id * CACHE_SIZE;
  index_limit_ = index_base_ + CACHE_SIZE;
}

ServerInstance::~ServerInstance() {}

void ServerInstance::SetKvMigrationRing(rte_ring* ring) {
  kv_migration_ring_ = ring;
}

void ServerInstance::CacheMigrate(const std::string_view& key,
                                  uint32_t migration_id) {
  if (key.empty()) {
    std::cerr << "Error: key is empty!\n";
    return;
  }
  std::string migrate_key = std::string(key);
  if (migrate_key.find('\0') != std::string::npos ||
      migrate_key.size() < KEY_LENGTH)
    return;

  auto cache_migrate = std::make_unique<struct CacheMigrate>();
  uint32_t req_id = utils::generate_request_id();
  cache_migrate->request_id = req_id;
  cache_migrate->migration_id = migration_id;
  std::memset(cache_migrate->key.data(), 0, KEY_LENGTH);
  std::memcpy(cache_migrate->key.data(), migrate_key.data(), KEY_LENGTH);

  auto packet = ConstructPacket(std::move(cache_migrate), controller_ip_in_,
                                server_ip_in_);

  if (!SendPacket(packet)) {
    std::cerr << "Failed to send CacheMigrate\n";
    return;
  }
}

template <typename PayloadType>
auto ServerInstance::ConstructPacket(std::unique_ptr<PayloadType> payload,
                                     uint32_t dst_ip, uint32_t src_ip,
                                     const uint8_t* dst_mac,
                                     const uint8_t* src_mac) -> Packet {
  if (!payload) {
    throw std::invalid_argument("Payload cannot be null");
  }
  constexpr uint8_t protocol = PacketTraits<PayloadType>::Protocol;
  constexpr uint16_t src_port = PacketTraits<PayloadType>::SrcPort;
  constexpr uint16_t dst_port = PacketTraits<PayloadType>::DstPort;

  size_t payload_size = sizeof(PayloadType);
  size_t total_size = ETH_HLEN + IPV4_HDR_LEN + UDP_HDR_LEN + payload_size;
  Packet packet(total_size);

  const uint8_t* d_mac = dst_mac ? dst_mac : controller_info_->mac.data();
  const uint8_t* s_mac = src_mac ? src_mac : server_info_.mac.data();

  ethhdr* eth_hdr = reinterpret_cast<ethhdr*>(packet.data());
  memcpy(eth_hdr->h_dest, d_mac, ETH_ALEN);
  memcpy(eth_hdr->h_source, s_mac, ETH_ALEN);
  eth_hdr->h_proto = htons(ETHERTYPE_IP);

  iphdr* ip_hdr = reinterpret_cast<iphdr*>(packet.data() + ETH_HLEN);
  static const iphdr default_hdr = CreateDefaultIpHdr();
  *ip_hdr = default_hdr;  // 整段复制默认模板
  // 修改特定值
  ip_hdr->protocol = protocol;
  ip_hdr->tot_len = htons(total_size);
  ip_hdr->saddr = src_ip;
  ip_hdr->daddr = dst_ip;
  ip_hdr->check = utils::IpChecksum(reinterpret_cast<uint16_t*>(ip_hdr), IPV4_HDR_LEN);

  udphdr* udp_hdr =
      reinterpret_cast<udphdr*>(packet.data() + ETH_HLEN + IPV4_HDR_LEN);
  udp_hdr->source = htons(src_port);
  udp_hdr->dest = htons(dst_port);
  udp_hdr->len = htons(UDP_HDR_LEN + payload_size);
  udp_hdr->check = 0;

  std::memcpy(packet.data() + ETH_HLEN + IPV4_HDR_LEN + UDP_HDR_LEN,
              payload.get(), payload_size);

  return packet;
}

bool ServerInstance::SendPacket(const Packet& packet) {
  if (packet.size() < ETH_HLEN + IPV4_HDR_LEN) {
    std::cerr << "Packet too small to extract headers." << std::endl;
    return false;
  }

  const ethhdr* eth_hdr = reinterpret_cast<const ethhdr*>(packet.data());
  sockaddr_ll dest_addr = utils::MakeSockAddrLl(sock_config_->ifindex,
                                                eth_hdr->h_dest);

  ssize_t bytes_sent = sendto(sock_config_->sockfd, packet.data(),
                              packet.size(), 0,
                              reinterpret_cast<sockaddr*>(&dest_addr),
                              sizeof(dest_addr));
  if (bytes_sent < 0) {
    perror("sendto failed");
    return false;
  }
  return true;
}

void ServerInstance::HandlePacket(const Packet& packet) {
  const udphdr* udp_hdr =
      reinterpret_cast<const udphdr*>(packet.data() + ETH_HLEN + IPV4_HDR_LEN);
  switch (udp_hdr->dest) {
    case UDP_PORT_KV:
      HandleMigrationInfo(packet);
      break;
    default:
      std::cerr << "[ProcessPacket] Unknown protocol: "
                << static_cast<int>(udp_hdr->dest) << "\n";
      break;
  }
}

void ServerInstance::HandleMigrationInfo(const Packet& packet) {
  const MigrationInfo* migration_info_hdr =
      reinterpret_cast<const MigrationInfo*>(packet.data() +
                                             c_m_proto::KV_HEADER_OFFSET);
  switch (migration_info_hdr->migration_status) {
    case MIGRATION_START:
      StartMigration(packet);
      break;

    case MIGRATION_DONE:

      break;
    default:
      break;
  }
}

auto ServerInstance::HashToIps(const std::vector<uint32_t>& indices,
                               const std::vector<std::string>& ip_list)
    -> std::vector<std::pair<std::string, uint32_t>> {
  std::hash<int> hasher;
  std::vector<std::pair<std::string, uint>> result;
  result.reserve(indices.size());
  if (ip_list.empty()) {
    std::cerr << "IP list is empty, cannot hash indices.\n";
    return result;
  }
  if (indices.empty()) {
    std::cerr << "Indices list is empty, cannot hash to IPs.\n";
    return result;
  }
  for (uint index : indices) {
    size_t hash_val = hasher(index);
    size_t idx = hash_val % ip_list.size();
    result.push_back({ip_list[idx], index});
  }
  return result;
}

inline auto ServerInstance::ConstructMigratePacket(
    uint32_t dst_ip, uint32_t src_ip, uint16_t index, uint32_t migration_id,
    uint8_t dst_rack_id, uint16_t index_size) -> Packet {
  uint8_t s_mac[ETH_ALEN];
  uint8_t d_mac[ETH_ALEN];
  rte_eth_random_addr(s_mac);
  rte_eth_random_addr(d_mac);

  auto kv_migrate = std::make_unique<KVMigrate>();
  kv_migrate->dev_info =
      static_cast<uint8_t>(ENCODE_DEV_INFO(server_info_.rack_id, DEV_LEAF));
  kv_migrate->request_id = utils::generate_request_id();
  kv_migrate->combined = ENCODE_COMBINED(CACHE_MIGRATE, WRITE_REQUEST);
  kv_migrate->migration_id = migration_id;
  kv_migrate->src_rack_id = server_info_.rack_id;
  kv_migrate->dst_rack_id = dst_rack_id;
  kv_migrate->cache_index = index;
  kv_migrate->total_keys = index_size;

  return ConstructPacket(std::move(kv_migrate), dst_ip, src_ip, d_mac, s_mac);
}

void ServerInstance::StartMigration(const Packet& packet) {
  // if (fsm_ != MIGRATION_NO) {
  //   std::cerr << "Migration already in progress or completed.\n";
  //   return;
  // }
  std::cout << "[Rack " << server_info_.rack_id
            << "] Get a StartMigration packet\n";

  const MigrationInfo* migration_info_hdr =
      reinterpret_cast<const MigrationInfo*>(packet.data() +
                                             c_m_proto::KV_HEADER_OFFSET);

  const auto& dst_cluster = (*clusters_info_)[migration_info_hdr->dst_rack_id];

  auto index_to_ips = HashToIps(
      utils::SampleIndices(index_base_, index_limit_, CHUNK_SIZE), dst_cluster);

  for (const auto& it : index_to_ips) {
    Packet c_mpacket = ConstructMigratePacket(
        inet_addr(it.first.c_str()), server_ip_in_, it.second,
        migration_info_hdr->migration_id, migration_info_hdr->dst_rack_id,
        index_to_ips.size());
    auto* packet_data = new Packet(std::move(c_mpacket));

    int ret = rte_ring_enqueue(kv_migration_ring_, packet_data);
    if (ret < 0) {
      std::cerr << "Failed to enqueue migration packet to ring: " << ret
                << std::endl;
      delete packet_data;  // 释放内存防止泄漏
      continue;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}
