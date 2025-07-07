#include "server_instance.h"

#include <linux/if_packet.h>
#include <net/if.h>
#include <openssl/sha.h>
#include <pcap.h>
#include <sw/redis++/errors.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <chrono>
#include <optional>
#include <thread>

#include "lib/utils.h"

static inline uint32_t generate_request_id() {
  static std::atomic<uint32_t> counter{0};
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, UINT32_MAX);

  return htonl(dis(gen) ^ counter.fetch_add(1, std::memory_order_relaxed));
}

static inline uint16_t Checksum(uint16_t *buffer, int size) {
  unsigned long sum = 0;
  while (size > 1) {
    sum += *buffer++;
    size -= 2;
  }
  if (size > 0) {
    sum += htons(*(uint8_t *)buffer << 8);
  }
  sum = (sum >> 16) + (sum & 0xFFFF);
  sum += (sum >> 16);
  return static_cast<uint16_t>(~sum);
}

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(500 * (1 << (attempt - 1)), 4000);
  if (attempt == 0) wait_ms = 0;

  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

ServerInstance::ServerInstance(
    const std::string &server_ip, int rack_id, int db,
    const std::string &iface_to_controller, const std::string &controller_mac,
    const std::string &controller_ip,
    std::shared_ptr<const std::vector<ClusterInfo>> clusters_info)
    : server_ip_(server_ip),
      rack_id_(rack_id),
      db_(db),
      iface_to_controller_(iface_to_controller),
      controller_mac_(utils::parse_mac(controller_mac)),
      controller_ip_(controller_ip),
      clusters_info_(std::move(clusters_info)),
      fsm_(MIGRATION_NO) {
  sockfd_ = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_IP));
  if (sockfd_ < 0) {
    throw std::runtime_error("Failed to create raw socket");
  }

  int buf_size = 2 * 1024 * 1024;
  setsockopt(sockfd_, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

  int one = 1;
  if (setsockopt(sockfd_, SOL_PACKET, PACKET_IGNORE_OUTGOING, &one,
                 sizeof(one)) < 0) {
    close(sockfd_);
    throw std::runtime_error("Failed to set socket IP_HDRINCL");
  }

  server_ip_in_ = inet_addr(server_ip_.c_str());
  controller_ip_in_ = inet_addr(controller_ip_.c_str());

  index_base_ = rack_id_ * CACHE_SIZE;
  index_limit_ = index_base_ + CACHE_SIZE;

  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s",
           iface_to_controller_.c_str());

  if (ioctl(sockfd_, SIOCGIFINDEX, &ifr) < 0) {
    close(sockfd_);
    throw std::runtime_error("Failed to get interface index");
  }

  ifindex_ = ifr.ifr_ifindex;

  if (ioctl(sockfd_, SIOCGIFHWADDR, &ifr) < 0) {
    close(sockfd_);
    throw std::runtime_error("ioctl(SIOCGIFHWADDR) failed");
  }
  memcpy(src_mac_, ifr.ifr_hwaddr.sa_data, ETH_ALEN);
}

ServerInstance::~ServerInstance() {
  if (running_) Stop();
}

bool ServerInstance::Start() {
  try {
    if (running_) {
      std::cerr << "[Rack " << rack_id_ << "] Server " << server_ip_ << " already running." << std::endl;
      return false;
    }

    running_ = true;

    recv_thread_ = std::thread(&ServerInstance::ReceiveThread, this);

    return running_;
  } catch (const std::exception &e) {
    std::cerr << "Failed to start server: " << e.what() << std::endl;
    running_ = false;
    return running_;
  } catch (...) {
    std::cerr << "An unknown error occurred while starting the server."
              << std::endl;
    running_ = false;
    return running_;
  }
}

void ServerInstance::Stop() {
  if (running_) {
    running_ = false;
  }
}

void ServerInstance::SetKvMigrationRing(
    struct rte_ring *ring, std::shared_ptr<int> kv_migration_event_fd_ptr) {
  kv_migration_ring_ = ring;
  kv_migration_event_fd_ptr_ = kv_migration_event_fd_ptr;
}

void ServerInstance::CacheMigrate(const std::string_view &key,
                                  uint32_t migration_id) {
  if (key.empty()) {
    std::cerr << "Error: key is empty!\n";
    return;
  }
  std::string migrate_key = std::string(key);
  if (migrate_key.find('\0') != std::string::npos ||
      migrate_key.size() < KEY_LENGTH)
    return;

  auto cache_migrate = std::make_unique<struct CacheMigrateHeader>();
  uint32_t req_id = generate_request_id();
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

void ServerInstance::HandleMigrateReply(uint32_t request_id) {
  std::cout << "[Rack " << rack_id_ << "] Get Migrate Reply\n";
  bool exist = request_map_.Modify(request_id, [&](auto &req) {
    try {
      req->set_value(true);
    } catch (const std::future_error &e) {
      std::cerr << "[MIGRATE_REPLY] Future error: " << e.what() << std::endl;
    }
  });
  if (!exist) {
    std::cerr << "[MIGRATE_REPLY] Request not exist id: " << request_id
              << std::endl;
  }
}

template <typename PayloadType>
std::vector<uint8_t> ServerInstance::ConstructPacket(
    std::unique_ptr<PayloadType> payload, uint32_t dst_ip, uint32_t src_ip) {
  if (!payload) {
    throw std::invalid_argument("Payload cannot be null");
  }
  constexpr uint8_t protocol = PacketTraits<PayloadType>::Protocol;
  size_t payload_size = sizeof(PayloadType);
  size_t total_size = ETH_HLEN + IPV4_HDR_LEN + payload_size;
  std::vector<uint8_t> packet(total_size);

  struct ethhdr *eth_hdr = reinterpret_cast<struct ethhdr *>(packet.data());
  memcpy(eth_hdr->h_dest, controller_mac_.data(), ETH_ALEN);
  memcpy(eth_hdr->h_source, src_mac_, ETH_ALEN);
  eth_hdr->h_proto = htons(ETHERTYPE_IP);

  struct iphdr *ip_hdr =
      reinterpret_cast<struct iphdr *>(packet.data() + ETH_HLEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->tos = 0;
  ip_hdr->tot_len = htons(total_size);
  ip_hdr->id = htons(54321);
  ip_hdr->ttl = 64;
  ip_hdr->protocol = protocol;
  ip_hdr->saddr = src_ip;
  ip_hdr->daddr = dst_ip;
  ip_hdr->check = Checksum(reinterpret_cast<uint16_t *>(ip_hdr), IPV4_HDR_LEN);

  std::memcpy(packet.data() + ETH_HLEN + IPV4_HDR_LEN, payload.get(),
              payload_size);

  return packet;
}

bool ServerInstance::SendPacket(const std::vector<uint8_t> &packet) {
  if (packet.size() < IPV4_HDR_LEN) {
    std::cerr << "Packet too small to extract IP header." << std::endl;
    return false;
  }

  const struct ethhdr *eth_hdr =
      reinterpret_cast<const struct ethhdr *>(packet.data());

  struct sockaddr_ll dest_addr = {};
  socklen_t addrlen = sizeof(dest_addr);
  memset(&dest_addr, 0, addrlen);
  dest_addr.sll_family = AF_PACKET;
  dest_addr.sll_ifindex = ifindex_;
  dest_addr.sll_halen = ETH_ALEN;
  memcpy(dest_addr.sll_addr, eth_hdr->h_dest, ETH_ALEN);

  ssize_t bytes_sent = sendto(sockfd_, packet.data(), packet.size(), 0,
                              (struct sockaddr *)&dest_addr, addrlen);
  if (bytes_sent < 0) {
    perror("sendto failed");
    return false;
  }
  return true;
}

void ServerInstance::ReceiveThread() {
  constexpr int BUFFER_SIZE = 2048;
  constexpr int MAX_RETRIES = 5;

  struct sockaddr_ll src_addr = {};
  socklen_t addrlen = sizeof(src_addr);
  int error_count = 0;

  while (!stop_receive_thread_.load(std::memory_order_relaxed)) {
    std::vector<uint8_t> buffer(BUFFER_SIZE);
    ssize_t recvlen =
        recvfrom(sockfd_, buffer.data(), buffer.size(), MSG_DONTWAIT,
                 (struct sockaddr *)&src_addr, &addrlen);
    if (recvlen > 0) {
      error_count = 0;
      struct ethhdr *eth_hdr = reinterpret_cast<struct ethhdr *>(buffer.data());
      if (memcmp(eth_hdr->h_source, src_mac_, ETH_ALEN) == 0) continue;
      if (ntohs(eth_hdr->h_proto) != ETH_P_IP) continue;
      if (recvlen < ETH_HLEN + IPV4_HDR_LEN) {
        std::cerr << "[Recv] Packet too short: " << recvlen << " bytes.\n";
        continue;
      }
      struct iphdr *ip_hdr =
          reinterpret_cast<struct iphdr *>(buffer.data() + ETH_HLEN);
      if (ip_hdr->daddr == server_ip_in_) {
        buffer.resize(recvlen);
        boost::asio::post(worker_pool_,
                          [this, packet = std::move(buffer)]() mutable {
                            ProcessPacket(packet);
                          });
      }
    } else if (recvlen == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
      perror("[Recv] recvfrom error");
      if (++error_count >= MAX_RETRIES) {
        std::cerr << "[Recv] Reached max error count, exiting.\n";
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  std::cout << "[Recv] Thread exiting cleanly.\n";
}

void ServerInstance::ProcessPacket(const std::vector<uint8_t> &packet) {
  const struct iphdr *ip_hdr =
      reinterpret_cast<const struct iphdr *>(packet.data() + ETH_HLEN);
  switch (ip_hdr->protocol) {
    case IP_PROTOCOLS_MIGRATION_INFO:
      HandleMigrationInfo(packet);
      break;
    case IP_PROTOCOLS_ASK:
      HandleAsk(packet);
      break;
    default:
      std::cerr << "[ProcessPacket] Unknown protocol: "
                << static_cast<int>(ip_hdr->protocol) << "\n";
      break;
  }
}

void ServerInstance::HandleMigrationInfo(const std::vector<uint8_t> &packet) {
  const struct MigrationInfo *migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo *>(packet.data() + ETH_HLEN +
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

std::vector<std::pair<std::string, uint>> ServerInstance::HashToIps(
    std::vector<uint> indices, const std::vector<std::string> &ip_list) {
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

inline std::vector<uint8_t> ServerInstance::ConstructMigratePacket(
    uint32_t dst_ip, uint32_t src_ip, uint16_t index, uint32_t migration_id,
    uint8_t dst_rack_id, uint16_t index_size) {
  size_t payload_size = sizeof(struct KVMigrateHeader);
  size_t total_size = ETH_HLEN + IPV4_HDR_LEN + payload_size;
  std::vector<uint8_t> packet(total_size);

  uint8_t s_mac[ETH_ALEN];
  uint8_t d_mac[ETH_ALEN];
  rte_eth_random_addr(s_mac);
  rte_eth_random_addr(d_mac);
  struct ethhdr *eth_hdr = reinterpret_cast<struct ethhdr *>(packet.data());
  memcpy(eth_hdr->h_source, s_mac, ETH_ALEN);
  memcpy(eth_hdr->h_dest, d_mac, ETH_ALEN);
  eth_hdr->h_proto = htons(ETHERTYPE_IP);

  struct iphdr *ip_hdr =
      reinterpret_cast<struct iphdr *>(packet.data() + ETH_HLEN);
  ip_hdr->ihl = 5;
  ip_hdr->version = 4;
  ip_hdr->tos = 0;
  ip_hdr->tot_len = htons(total_size);
  ip_hdr->id = htons(54321);
  ip_hdr->ttl = 64;
  ip_hdr->protocol = IP_PROTOCOLS_NETCACHE;
  ip_hdr->saddr = src_ip;
  ip_hdr->daddr = dst_ip;
  ip_hdr->check = Checksum(reinterpret_cast<uint16_t *>(ip_hdr), IPV4_HDR_LEN);

  struct KVMigrateHeader *kv_migrate_hdr =
      reinterpret_cast<struct KVMigrateHeader *>(packet.data() + ETH_HLEN +
                                                 IPV4_HDR_LEN);
  kv_migrate_hdr->request_id = generate_request_id();
  uint16_t combined = ENCODE_COMBINED(rack_id_, CACHE_MIGRATE, WRITE_REQUEST);
  kv_migrate_hdr->combined = htons(combined);
  kv_migrate_hdr->migration_id = migration_id;
  kv_migrate_hdr->src_rack_id = rack_id_;
  kv_migrate_hdr->dst_rack_id = dst_rack_id;
  kv_migrate_hdr->cache_index = htons(index);
  kv_migrate_hdr->total_keys = htons(index_size);

  return packet;
}

void ServerInstance::StartMigration(const std::vector<uint8_t> &packet) {
  // if (fsm_ != MIGRATION_NO) {
  //   std::cerr << "Migration already in progress or completed.\n";
  //   return;
  // }
  // std::cout << "[Rack " << rack_id_ << "] Get a StartMigration packet\n";

  const struct MigrationInfo *migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo *>(packet.data() + ETH_HLEN +
                                                     IPV4_HDR_LEN);

  const ClusterInfo &dst_cluster =
      (*clusters_info_)[migration_info_hdr->dst_rack_id];

  std::vector<uint> indices;

  indices.reserve(CHUNK_SIZE);

  for (uint i = index_base_; i <= (index_base_ + CHUNK_SIZE); ++i) {
    indices.push_back(i);
  }

  auto index_to_ips = HashToIps(indices, dst_cluster.servers_ip);

  for (const auto &it : index_to_ips) {
    std::vector<uint8_t> c_mpacket = ConstructMigratePacket(
        inet_addr(it.first.c_str()), server_ip_in_, it.second,
        migration_info_hdr->migration_id, migration_info_hdr->dst_rack_id,
        index_to_ips.size());
    auto *packet_data = new std::vector<uint8_t>(std::move(c_mpacket));

    int ret = rte_ring_enqueue(kv_migration_ring_, packet_data);
    if (ret < 0) {
      std::cerr << "Failed to enqueue migration packet to ring: " << ret
                << std::endl;
      continue;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

std::vector<uint> ServerInstance::SampleIndices(size_t sample_size) {
  std::vector<uint> indices(index_limit_ - index_base_);
  std::iota(indices.begin(), indices.end(), index_base_);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::shuffle(indices.begin(), indices.end(), gen);

  if (sample_size > indices.size()) {
    sample_size = indices.size();
  }

  return std::vector<uint>(indices.begin(), indices.begin() + sample_size);
}

void ServerInstance::HandleAsk(const std::vector<uint8_t> &packet) {
  const struct AskPacket *ask =
      reinterpret_cast<const struct AskPacket *>(packet.data() + IPV4_HDR_LEN);
  uint32_t request_id = ask->request_id;
  bool exist = request_map_.Modify(request_id, [&](auto &req) {
    try {
      if (ask->ask == 1) {
        req->set_value(true);
      } else {
        req->set_value(false);
      }
    } catch (const std::future_error &e) {
      std::cerr << "[Ask] Future error: " << e.what() << std::endl;
    }
  });
  if (!exist) {
    std::cerr << "[Ask] Request not exist id: " << request_id << std::endl;
  }
}

bool ServerInstance::SendAsk(uint32_t request_id, uint32_t dst_ip,
                             uint8_t ask /* = 1 */) {
  auto payload = std::make_unique<AskPacket>();
  payload->request_id = request_id;
  payload->ask = ask;

  auto packet = ConstructPacket(std::move(payload), dst_ip, server_ip_in_);
  if (!SendPacket(packet)) {
    std::cerr << "Failed to send AskPacket\n";
    return false;
  }
  return true;
}
