#include "server_instance.h"

#include <linux/if_packet.h>
#include <net/if.h>
#include <pcap.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <optional>

#include "lib/utils.h"

using namespace c_m_proto;

static inline uint16_t Checksum(uint16_t* buffer, int size) {
  unsigned long sum = 0;
  while (size > 1) {
    sum += *buffer++;
    size -= 2;
  }
  if (size > 0) {
    sum += htons(*(uint8_t*)buffer << 8);
  }
  sum = (sum >> 16) + (sum & 0xFFFF);
  sum += (sum >> 16);
  return static_cast<uint16_t>(~sum);
}

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

void ServerInstance::SetKvMigrationRing(struct rte_ring* ring) {
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
                                     uint32_t dst_ip, uint32_t src_ip)
    -> std::vector<uint8_t> {
  if (!payload) {
    throw std::invalid_argument("Payload cannot be null");
  }
  constexpr uint8_t protocol = PacketTraits<PayloadType>::Protocol;
  constexpr uint16_t src_port = PacketTraits<PayloadType>::SrcPort;
  constexpr uint16_t dst_port = PacketTraits<PayloadType>::DstPort;

  size_t payload_size = sizeof(PayloadType);
  size_t total_size = ETH_HLEN + IPV4_HDR_LEN + UDP_HDR_LEN + payload_size;
  std::vector<uint8_t> packet(total_size);

  struct ethhdr* eth_hdr = reinterpret_cast<struct ethhdr*>(packet.data());
  memcpy(eth_hdr->h_dest, controller_info_->mac.data(), ETH_ALEN);
  memcpy(eth_hdr->h_source, server_info_.mac.data(), ETH_ALEN);
  eth_hdr->h_proto = htons(ETHERTYPE_IP);

  struct iphdr* ip_hdr =
      reinterpret_cast<struct iphdr*>(packet.data() + ETH_HLEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->tos = 0;
  ip_hdr->tot_len = htons(total_size);
  ip_hdr->id = htons(54321);
  ip_hdr->ttl = 64;
  ip_hdr->protocol = protocol;
  ip_hdr->saddr = src_ip;
  ip_hdr->daddr = dst_ip;
  ip_hdr->check = Checksum(reinterpret_cast<uint16_t*>(ip_hdr), IPV4_HDR_LEN);

  struct udphdr* udp_hdr =
      reinterpret_cast<struct udphdr*>(packet.data() + ETH_HLEN + IPV4_HDR_LEN);
  udp_hdr->source = htons(src_port);
  udp_hdr->dest = htons(dst_port);
  udp_hdr->len = htons(UDP_HDR_LEN + payload_size);
  udp_hdr->check = 0;

  std::memcpy(packet.data() + ETH_HLEN + IPV4_HDR_LEN + UDP_HDR_LEN,
              payload.get(), payload_size);

  return packet;
}

bool ServerInstance::SendPacket(const std::vector<uint8_t>& packet) {
  if (packet.size() < IPV4_HDR_LEN) {
    std::cerr << "Packet too small to extract IP header." << std::endl;
    return false;
  }

  const struct ethhdr* eth_hdr =
      reinterpret_cast<const struct ethhdr*>(packet.data());

  struct sockaddr_ll dest_addr = {};
  socklen_t addrlen = sizeof(dest_addr);
  memset(&dest_addr, 0, addrlen);
  dest_addr.sll_family = AF_PACKET;
  dest_addr.sll_ifindex = sock_config_->ifindex;
  dest_addr.sll_halen = ETH_ALEN;
  memcpy(dest_addr.sll_addr, eth_hdr->h_dest, ETH_ALEN);

  ssize_t bytes_sent =
      sendto(sock_config_->sockfd, packet.data(), packet.size(), 0,
             (struct sockaddr*)&dest_addr, addrlen);
  if (bytes_sent < 0) {
    perror("sendto failed");
    return false;
  }
  return true;
}

void ServerInstance::HandlePacket(const std::vector<uint8_t>& packet) {
  const struct udphdr* udp_hdr = reinterpret_cast<const struct udphdr*>(
      packet.data() + ETH_HLEN + IPV4_HDR_LEN);
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

void ServerInstance::HandleMigrationInfo(const std::vector<uint8_t>& packet) {
  const struct MigrationInfo* migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo*>(packet.data() + ETH_HLEN +
                                                    IPV4_HDR_LEN);
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
    uint8_t dst_rack_id, uint16_t index_size) -> std::vector<uint8_t> {
  size_t payload_size = sizeof(KVMigrate);
  size_t total_size = ETH_HLEN + IPV4_HDR_LEN + UDP_HDR_LEN + payload_size;
  std::vector<uint8_t> packet(total_size);

  uint8_t s_mac[ETH_ALEN];
  uint8_t d_mac[ETH_ALEN];
  rte_eth_random_addr(s_mac);
  rte_eth_random_addr(d_mac);
  struct ethhdr* eth_hdr = reinterpret_cast<struct ethhdr*>(packet.data());
  memcpy(eth_hdr->h_source, s_mac, ETH_ALEN);
  memcpy(eth_hdr->h_dest, d_mac, ETH_ALEN);
  eth_hdr->h_proto = htons(ETHERTYPE_IP);

  struct iphdr* ip_hdr =
      reinterpret_cast<struct iphdr*>(packet.data() + ETH_HLEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->tos = 0;
  ip_hdr->tot_len = htons(total_size);
  ip_hdr->id = htons(54321);
  ip_hdr->ttl = 64;
  ip_hdr->protocol = IPPROTO_UDP;
  ip_hdr->saddr = src_ip;
  ip_hdr->daddr = dst_ip;
  ip_hdr->check = Checksum(reinterpret_cast<uint16_t*>(ip_hdr), IPV4_HDR_LEN);

  struct udphdr* udp_hdr =
      reinterpret_cast<struct udphdr*>(packet.data() + ETH_HLEN + IPV4_HDR_LEN);
  udp_hdr->source = htons(UDP_PORT_KV);
  udp_hdr->dest = htons(UDP_PORT_KV);
  udp_hdr->len = htons(UDP_HDR_LEN + payload_size);
  udp_hdr->check = 0;

  struct KVMigrate* kv_migrate = reinterpret_cast<struct KVMigrate*>(
      packet.data() + ETH_HLEN + IPV4_HDR_LEN);
  kv_migrate->request_id = utils::generate_request_id();
  uint16_t combined = ENCODE_COMBINED(CACHE_MIGRATE, WRITE_REQUEST);
  kv_migrate->combined = htons(combined);
  kv_migrate->migration_id = migration_id;
  kv_migrate->src_rack_id = server_info_.rack_id;
  kv_migrate->dst_rack_id = dst_rack_id;
  kv_migrate->cache_index = htons(index);
  kv_migrate->total_keys = htons(index_size);

  return packet;
}

void ServerInstance::StartMigration(const std::vector<uint8_t>& packet) {
  // if (fsm_ != MIGRATION_NO) {
  //   std::cerr << "Migration already in progress or completed.\n";
  //   return;
  // }
  std::cout << "[Rack " << server_info_.rack_id
            << "] Get a StartMigration packet\n";

  const struct MigrationInfo* migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo*>(packet.data() + ETH_HLEN +
                                                    IPV4_HDR_LEN);

  const auto& dst_cluster = (*clusters_info_)[migration_info_hdr->dst_rack_id];

  auto index_to_ips = HashToIps(
      utils::SampleIndices(index_base_, index_limit_, CHUNK_SIZE), dst_cluster);

  for (const auto& it : index_to_ips) {
    std::vector<uint8_t> c_mpacket = ConstructMigratePacket(
        inet_addr(it.first.c_str()), server_ip_in_, it.second,
        migration_info_hdr->migration_id, migration_info_hdr->dst_rack_id,
        index_to_ips.size());
    auto* packet_data = new std::vector<uint8_t>(std::move(c_mpacket));

    int ret = rte_ring_enqueue(kv_migration_ring_, packet_data);
    if (ret < 0) {
      std::cerr << "Failed to enqueue migration packet to ring: " << ret
                << std::endl;
      continue;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}
