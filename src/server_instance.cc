#include "server_instance.h"

#include <linux/if_packet.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <openssl/sha.h>
#include <pcap.h>
#include <sw/redis++/errors.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <zlib.h>

#include <chrono>
#include <optional>
#include <thread>

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

static inline void SwapIpv4(struct iphdr *ip_hdr) {
  uint32_t tmp_ip = ip_hdr->saddr;
  ip_hdr->saddr = ip_hdr->daddr;
  ip_hdr->daddr = tmp_ip;
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
      controller_mac_(controller_mac),
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
      std::cout << "Server already running." << std::endl;
      return false;
    }

    running_ = true;

    redis_ = std::make_unique<sw::redis::Redis>("tcp://127.0.0.1:6379/" +
                                                std::to_string(db_));
    recv_thread_ = std::thread(&ServerInstance::ReceiveThread, this);
    std::cout << "[Rack " << rack_id_ << "] Server " << server_ip_
              << " running...\n";

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
    std::cout << "[Rack " << rack_id_ << "] Server " << server_ip_
              << " stopped.\n";
  }
}

void ServerInstance::SetKvMigrationRing(
    struct rte_ring *ring, std::shared_ptr<int> kv_migration_event_fd_ptr) {
  kv_migration_ring_ = ring;
  kv_migration_event_fd_ptr_ = kv_migration_event_fd_ptr;
}

void ServerInstance::CacheMigrate(const std::string_view &key,
                                  uint32_t migration_id) {
  std::string migrate_key = std::string(key);
  auto payload = std::make_unique<struct CacheMigrateHeader>();
  uint32_t req_id = generate_request_id();
  payload->request_id = req_id;
  payload->migration_id = migration_id;
  std::memset(payload->key.data(), 0, KEY_LENGTH);
  std::memcpy(payload->key.data(), key.data(), KEY_LENGTH);

  auto packet =
      ConstructPacket(std::move(payload), controller_ip_in_, server_ip_in_);
  auto request = std::make_shared<std::promise<bool>>();
  auto future = request->get_future();

  if (!request_map_.Insert(req_id, std::move(request))) {
    std::cerr << "Duplicate request_id: " << req_id << '\n';
    return;
  }
  RequestCleaner cleaner(request_map_, req_id);

  for (uint16_t attempt = 0; attempt < RETRIES; ++attempt) {
    if (!SendPacket(packet)) {
      std::cerr << "Failed to send CacheMigrate\n";
      continue;
    }

    if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
      try {
        if (future.get())
          return;
        else {
          std::cerr << "CacheMigrate failed for key: " << migrate_key
                    << " on attempt " << attempt + 1 << '\n';
        }
      } catch (const std::exception &e) {
        std::cerr << "Promise future error: " << e.what() << '\n';
      } catch (...) {
        std::cerr << "Unknown promise error\n";
      }
    } else {
      std::cerr << "Timeout waiting for CacheMigrate ACK\n";
    }
    exponentialBackoff(attempt);
  }
  std::cerr << "Max retries exceeded";
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
  memcpy(dest_addr.sll_addr, eth_hdr->h_dest, 6);

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

void ServerInstance::StartMigration(const std::vector<uint8_t> &packet) {
  if (fsm_ != MIGRATION_NO) {
    std::cerr << "Migration already in progress or completed.\n";
    return;
  }
  const struct MigrationInfo *migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo *>(packet.data() + ETH_HLEN +
                                                     IPV4_HDR_LEN);

  const ClusterInfo &dst_cluster =
      (*clusters_info_)[migration_info_hdr->dst_rack_id];

  std::vector<uint> indices = SampleIndices(CACHE_SIZE / 2);

  auto index_to_ips = HashToIps(indices, dst_cluster.servers_ip);

  for (const auto &it : index_to_ips) {
    uint32_t req_id = generate_request_id();
    auto payload = std::make_unique<KVMigrateHeader>();
    payload->request_id = req_id;
    payload->combined = ENCODE_COMBINED(rack_id_, CACHE_MIGRATE, WRITE_REQUEST);

    payload->migration_id = migration_info_hdr->migration_id;
    payload->src_rack_id = migration_info_hdr->src_rack_id;
    payload->dst_rack_id = migration_info_hdr->dst_rack_id;
    payload->cache_index = htons(it.second);
    payload->total_keys = htons(index_to_ips.size());
    payload->is_last_key = (it.second == index_to_ips.end()->second) ? 1 : 0;

    auto packet = ConstructPacket(std::move(payload),
                                  inet_addr(it.first.c_str()), server_ip_in_);

    auto packet_data =
        std::make_shared<std::vector<uint8_t>>(std::move(packet));

    auto request = std::make_shared<std::promise<bool>>();
    auto future = request->get_future();

    if (!request_map_.Insert(req_id, std::move(request))) {
      std::cerr << "Duplicate request_id: " << req_id << '\n';
      break;
    }

    RequestCleaner cleaner(request_map_, req_id);

    bool success = false;
    for (uint16_t attempt = 0; attempt < RETRIES; ++attempt) {
      int ret = rte_ring_enqueue(kv_migration_ring_, packet_data.get());
      if (ret < 0) {
        std::cerr << "Failed to enqueue migration packet to ring: " << ret
                  << std::endl;
        continue;
      }

      std::shared_ptr<int> kv_migration_event_fd =
          kv_migration_event_fd_ptr_.lock();
      if (kv_migration_event_fd) {
        uint64_t val = 1;
        ssize_t bytes_written =
            write(*kv_migration_event_fd, &val, sizeof(val));
        if (bytes_written == -1) {
          std::cerr << "Failed to write to event_fd\n";
        }
      } else {
        std::cerr << "Event fd is no longer valid!\n";
      }

      if (future.wait_for(std::chrono::seconds(2)) ==
          std::future_status::ready) {
        try {
          if (future.get()) {
            success = true;
            break;
          }
        } catch (const std::exception &e) {
          std::cerr << "Promise future error: " << e.what() << '\n';
        } catch (...) {
          std::cerr << "Unknown promise error\n";
        }
      }
      exponentialBackoff(attempt);
    }
    if (!success) std::cerr << "Max retries exceeded";
  }
}

std::vector<uint> ServerInstance::SampleIndices(size_t sample_size) {
  std::vector<uint> indices(index_base_ - index_limit_);
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
