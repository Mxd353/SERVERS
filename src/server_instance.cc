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
      fsm_(this) {
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

void ServerInstance::HotReport(const std::string &key, std::string_view value,
                               int count) {
  std::lock_guard<std::mutex> lock(hot_keys_mutex_);

  if (confirmed_hot_keys_.count(key)) {
    std::cout << "This is a confirmed hot key.\n";
    return;
  }

  if (!pending_hot_keys_.insert(key).second) {
    std::cout << "This is a pending hot key.\n";
    return;
  }

  if (value.size() < VALUE_LENGTH * 4) {
    pending_hot_keys_.erase(key);
    throw std::runtime_error("Value too short for hot key report");
  }

  auto payload = std::make_unique<ReportHotKey>();
  payload->request_id = generate_request_id();
  payload->dev_id = rack_id_;

  std::memset(payload->key_hot.data(), 0, KEY_LENGTH);
  std::memcpy(payload->key_hot.data(), key.data(), KEY_LENGTH);
  std::memcpy(payload->value1.data(), value.data(), VALUE_LENGTH * 4);

  payload->count = count;

  auto packet =
      ConstructPacket(std::move(payload), controller_ip_in_, server_ip_in_);

  if (SendPacket(packet)) {
    std::cout << "[HOT] Report hot key: " << key << " to controller.\n";
  } else {
    std::cerr << "[HOT] Failed to send hot key report for key: " << key << "\n";
    pending_hot_keys_.erase(key);
  }
}

std::shared_ptr<MigrationStateMachine::DestinationMigrationMeta>
ServerInstance::EnsureMigrationMeta(uint32_t migration_id, uint8_t source_rack,
                                    uint16_t total_chunks,
                                    std::vector<std::string> &&initial_keys) {
  auto meta = fsm_.getDestinationMigrationMeta(migration_id);
  if (meta) return meta;

  const auto &active = fsm_.getActiveDestinationMigrations();
  if (active.find(migration_id) == active.end()) {
    fsm_.addDestinationMode(migration_id, source_rack, std::move(initial_keys));
    meta = fsm_.getDestinationMigrationMeta(migration_id);
    if (meta) {
      meta->total_chunks = total_chunks;
    }
  }
  return meta;
}

void ServerInstance::HandleKVMigration(struct KV_Migration *kv_migration_hdr,
                                       uint8_t *data, size_t data_len) {
  uint32_t migration_id = ntohl(kv_migration_hdr->migration_id);
  uint8_t source_rack = kv_migration_hdr->src_rack_id;
  uint16_t chunk_index = ntohs(kv_migration_hdr->chunk_index);
  uint16_t total_chunks = ntohs(kv_migration_hdr->total_chunks);

  std::vector<uint8_t> compressed_data(data, data + data_len);
  std::vector<KVPair> decompressed_pairs;
  try {
    decompressed_pairs = DecompressKvPairs(compressed_data);
  } catch (const std::exception &e) {
    std::cerr << "Decompression failed: " << e.what() << "\n";
    return;
  }

  redis_->mset(decompressed_pairs.begin(), decompressed_pairs.end());

  std::vector<std::string> keys_cache;
  keys_cache.reserve(decompressed_pairs.size());
  for (const auto &kv : decompressed_pairs) {
    keys_cache.emplace_back(kv.first);
  }

  auto meta = EnsureMigrationMeta(migration_id, source_rack, total_chunks,
                                  std::move(keys_cache));
  if (!meta) {
    std::cerr << "[Error] Failed to obtain migration meta\n";
    return;
  }

  if (chunk_index >= meta->total_chunks) {
    std::cerr << "[Warning] Invalid chunk index " << chunk_index << "\n";
    return;
  }
  if (!meta->received_chunks.insert(chunk_index).second) {
    std::cerr << "[Info] Duplicate chunk " << chunk_index << " for migration "
              << migration_id << "\n";
    return;
  }

  if (meta->keys) {
    meta->keys->reserve(meta->keys->size() + decompressed_pairs.size());
    for (auto &kv : decompressed_pairs)
      meta->keys->emplace_back(std::move(kv.first));
  }

  if (kv_migration_hdr->is_last_chunk &&
      meta->received_chunks.size() != meta->total_chunks) {
    std::cerr << "Last chunk arrived but some chunks are still missing.\n";
  }

  if (meta->received_chunks.size() == meta->total_chunks) {
    std::cout << "KV migration " << migration_id
              << " completed successfully.\n";
    fsm_.transitionTo(MigrationStateMachine::State::DONE, migration_id);
  }
}

void ServerInstance::PerWrite(const std::string_view &key) {
  bool find = false;
  std::string pre_key = std::string(key);
  uint8_t id = kv_migration_in_id_.load();
  {
    std::unique_lock lock(kv_cache_mutex_);
    auto it = kv_migration_in_cache_.find(pre_key);
    if (it != kv_migration_in_cache_.end()) {
      kv_migration_in_cache_.erase(pre_key);
      find = true;
    }
  }
  if (find) {
    for (uint16_t attempt = 0; attempt < RETRIES; ++attempt) {
      auto payload = std::make_unique<PreWrite>();
      payload->request_id = generate_request_id();
      payload->migration_id = id;
      std::memset(payload->key.data(), 0, KEY_LENGTH);
      std::memcpy(payload->key.data(), key.data(), KEY_LENGTH);

      auto packet =
          ConstructPacket(std::move(payload), controller_ip_in_, server_ip_in_);
      std::promise<bool> result;
      auto future = result.get_future();
      {
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_[payload->request_id] = std::move(result);
      }

      if (!SendPacket(packet)) {
        std::cerr << "Failed to send PerWrite\n";
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_.erase(payload->request_id);
        continue;
      }

      if (future.wait_for(std::chrono::seconds(2)) ==
          std::future_status::ready) {
        try {
          if (future.get()) return;
        } catch (const std::exception &e) {
          std::cerr << "Promise future error: " << e.what() << '\n';
        }
      } else {
        std::cerr << "Timeout waiting for PerWrite ACK\n";
      }
      {
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_.erase(payload->request_id);
      }
      exponentialBackoff(attempt);
    }
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
    case IP_PROTOCOLS_HOTREPORT:
      HandleHotReply(packet);
      break;
    case IP_PROTOCOLS_MIGRATION_INFO:
      HandleMigrationInfo(packet);
      break;
    case IP_PROTOCOLS_ASK:
      HandleAsk(packet);
      break;
    default:
      break;
  }
}

void ServerInstance::HandleHotReply(const std::vector<uint8_t> &packet) {
  const struct ReportHotKey *hot_reply_hdr =
      reinterpret_cast<const struct ReportHotKey *>(packet.data() + ETH_HLEN +
                                                    IPV4_HDR_LEN);

  std::string key_new(hot_reply_hdr->key_hot.data(), KEY_LENGTH);
  std::string key_replay(hot_reply_hdr->key_replace.data(), KEY_LENGTH);

  std::lock_guard<std::mutex> lock(hot_keys_mutex_);
  if (pending_hot_keys_.erase(key_new)) {
    confirmed_hot_keys_.insert(key_new);
    pending_hot_keys_.erase(key_replay);
    confirmed_hot_keys_.erase(key_replay);
    std::cout << "[HOT] Confirmed hot key: " << key_new
              << ", replaced: " << key_replay << "\n";
  } else {
    std::cout << "[HOT] Received confirmation for unknown key: " << key_new
              << "\n";
  }
}

void ServerInstance::onStateChange(MigrationStateMachine::State new_state,
                                   uint32_t migration_id) {
  switch (new_state) {
    case MigrationStateMachine::State::NO:
      std::cout << "Migration reset for ID: " << migration_id << ".\n";
      break;
    case MigrationStateMachine::State::PREPARING:
      std::cout << "Migration PREPARING for ID: " << migration_id << ".\n";
      SendMigrationReady();
      break;
    case MigrationStateMachine::State::TRANSFERRING:
      std::cout << "Migration TRANSFERRING for ID: " << migration_id << ".\n";
      StartMigration();
      break;
    case MigrationStateMachine::State::DONE:
      std::cout << "Migration DONE for ID: " << migration_id << ".\n";
      break;
  }
}

void ServerInstance::HandleMigrationInfo(const std::vector<uint8_t> &packet) {
  const struct MigrationInfo *migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo *>(packet.data() + ETH_HLEN +
                                                     IPV4_HDR_LEN);
  switch (migration_info_hdr->migration_status) {
    case MIGRATION_PREPARING:
      HandleMigrationReady(packet);
      break;
    case MIGRATION_TRANSFERRING:
      if (SendAsk(migration_info_hdr->request_id, controller_ip_in_))
        if (!fsm_.transitionTo(MigrationStateMachine::State::TRANSFERRING))
          perror("Set TRANSFERRING error");
      break;
    case MIGRATION_DONE:

      break;
    default:
      break;
  }
}

void ServerInstance::HandleMigrationReady(const std::vector<uint8_t> &packet) {
  const struct iphdr *ip_hdr =
      reinterpret_cast<const struct iphdr *>(packet.data() + ETH_HLEN);
  const struct MigrationInfo *migration_info_hdr =
      reinterpret_cast<const struct MigrationInfo *>(packet.data() + ETH_HLEN +
                                                     IPV4_HDR_LEN);
  uint total_keys = 0;
  {
    std::lock_guard<std::mutex> lock(hot_keys_mutex_);
    total_keys = confirmed_hot_keys_.size() / 2;
  }
  if (total_keys == 0) {
    for (uint16_t attempt = 0; attempt < RETRIES; ++attempt) {
      auto payload = std::make_unique<KeyMigrationReady>();
      payload->request_id = generate_request_id();
      payload->migration_id = migration_info_hdr->migration_id;
      payload->migration_status = MIGRATION_NOT_ENOUGH;
      payload->is_final = 1;

      auto packet =
          ConstructPacket(std::move(payload), ip_hdr->saddr, ip_hdr->daddr);
      std::promise<bool> result;
      auto future = result.get_future();
      {
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_[payload->request_id] = std::move(result);
      }

      if (!SendPacket(packet)) {
        std::cerr << "Failed to send KeyMigrationReady\n";
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_.erase(payload->request_id);
        continue;
      }

      if (future.wait_for(std::chrono::seconds(2)) ==
          std::future_status::ready) {
        try {
          if (future.get()) return;
        } catch (const std::exception &e) {
          std::cerr << "Promise future error: " << e.what() << '\n';
        }
      } else {
        std::cerr << "Timeout waiting for KeyMigrationReady ACK\n";
      }
      {
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_.erase(payload->request_id);
      }
      exponentialBackoff(attempt);
    }
  } else {
    std::vector<std::string> selected;
    selected.reserve(confirmed_hot_keys_.size());
    std::hash<std::string> hasher;
    for (auto &key : confirmed_hot_keys_) {
      if ((hasher(key) % 2) == 0) {
        selected.emplace_back(std::move(key));
      }
    }

    if (!fsm_.startSourceMode(ntohl(migration_info_hdr->migration_id),
                              migration_info_hdr->dst_rack_id,
                              std::move(selected))) {
      perror("Set READY with meta error");
    }
  }
}

void ServerInstance::SendMigrationReady() {
  auto meta = fsm_.getSourceMigrationMeta();
  const auto &keys = *meta->keys;
  size_t total_keys = keys.size();
  size_t total_chunks = (total_keys + CHUNK_SIZE - 1) / CHUNK_SIZE;

  for (size_t chunk_index = 0; chunk_index < total_chunks; ++chunk_index) {
    size_t start = chunk_index * CHUNK_SIZE;
    size_t end = std::min(start + CHUNK_SIZE, total_keys);
    size_t current_chunk_size = end - start;

    auto payload = std::make_unique<KeyMigrationReady>();
    payload->request_id = generate_request_id();
    payload->migration_id = htonl(meta->id);
    payload->migration_status = MIGRATION_PREPARING;
    payload->chunk_index = htons(chunk_index);
    payload->total_chunks = htons(total_chunks);
    payload->chunk_size = htons(current_chunk_size);
    payload->is_final = (chunk_index == total_chunks - 1) ? 1 : 0;

    for (size_t i = 0; i < current_chunk_size; ++i) {
      const std::string &key = keys[start + i];
      std::memset(payload->keys[i].data(), 0, KEY_LENGTH);
      std::memcpy(payload->keys[i].data(), key.data(),
                  std::min<size_t>(KEY_LENGTH - 1, key.size()));
    }

    auto packet = ConstructPacket(std::move(payload), controller_ip_in_,
                                  controller_ip_in_);
    std::promise<bool> result;
    auto future = result.get_future();
    {
      std::lock_guard<std::mutex> lock(request_map_mutex_);
      request_map_[payload->request_id] = std::move(result);
    }
    for (uint16_t attempt = 0; attempt < RETRIES; ++attempt) {
      if (!SendPacket(packet)) {
        std::cerr << "Failed to send KeyMigrationReady\n";
        std::lock_guard<std::mutex> lock(request_map_mutex_);
        request_map_.erase(payload->request_id);
        break;
      }
      if (future.wait_for(std::chrono::seconds(2)) ==
          std::future_status::ready) {
        bool result = future.get();
        if (result) break;
      } else {
        std::cerr << "Timeout waiting for KeyMigrationReady ACK\n";
      }
      exponentialBackoff(attempt);
    }
    {
      std::lock_guard<std::mutex> lock(request_map_mutex_);
      request_map_.erase(payload->request_id);
    }
  }
}

void ServerInstance::HandleMigrationStart(uint32_t request_id) {
  if (SendAsk(request_id, controller_ip_in_))
    if (!fsm_.transitionTo(MigrationStateMachine::State::TRANSFERRING))
      perror("Set START error");
}

std::unordered_map<std::string, std::vector<ServerInstance::KVPair>>
ServerInstance::HashKeysToIps(const std::vector<KVPair> &kv_pairs,
                              const std::vector<std::string> &ip_list) {
  std::unordered_map<std::string, std::vector<KVPair>> result;
  if (ip_list.empty()) {
    std::cerr << "Warning: IP 列表为空" << std::endl;
    return result;
  }
  size_t num_ips = ip_list.size();
  unsigned char hash[SHA256_DIGEST_LENGTH];

  for (const auto &kv : kv_pairs) {
    // 计算 SHA256 哈希
    SHA256(reinterpret_cast<const unsigned char *>(kv.first.data()),
           kv.first.size(), hash);

    uint64_t hash_val = 0;
    std::memcpy(&hash_val, hash, sizeof(uint64_t));
    hash_val = be64toh(hash_val);

    const std::string &selected_ip = ip_list[hash_val % num_ips];
    result[selected_ip].emplace_back(kv);
  }

  return result;
}

void ServerInstance::StartMigration() {
  auto meta = fsm_.getSourceMigrationMeta();
  const auto &keys = *meta->keys;
  const ClusterInfo &dst_cluster = (*clusters_info_)[meta->dst_rack];

  std::vector<std::optional<std::string>> values;
  redis_->mget(keys.begin(), keys.end(), std::back_inserter(values));

  std::vector<KVPair> kv_pairs;
  for (size_t i = 0; i < keys.size(); ++i) {
    if (values[i].has_value()) {
      kv_pairs.push_back({keys[i], values[i].value()});
    }
  }

  size_t total_keys = kv_pairs.size();
  size_t total_chunks = (total_keys + CHUNK_SIZE - 1) / CHUNK_SIZE;

  auto keys_to_ip = HashKeysToIps(kv_pairs, dst_cluster.servers_ip);

  for (const auto &it : keys_to_ip) {
    for (size_t chunk_index = 0; chunk_index < total_chunks; ++chunk_index) {
      size_t start = chunk_index * CHUNK_SIZE;
      size_t end = std::min(start + CHUNK_SIZE, total_keys);

      auto payload = std::make_unique<KV_Migration>();
      payload->request_id = generate_request_id();
      payload->dev_id = static_cast<uint8_t>(rack_id_);
      payload->migration_id = htonl(meta->id);
      payload->src_rack_id = static_cast<uint8_t>(rack_id_);
      payload->dst_rack_id = meta->dst_rack;
      payload->chunk_index = htons(chunk_index);
      payload->total_chunks = htons(total_chunks);
      payload->chunk_size = htons(end - start);
      payload->compression = 1;
      payload->is_last_chunk = (chunk_index == total_chunks - 1) ? 1 : 0;

      std::vector<KVPair> chunk_kv_pairs(it.second.begin() + start,
                                         it.second.begin() + end);

      std::vector<uint8_t> raw_data = CompressKvPairs(chunk_kv_pairs);
      auto packet = ConstructPacket(std::move(payload),
                                    inet_addr(it.first.c_str()), server_ip_in_);

      packet.insert(packet.end(), raw_data.begin(), raw_data.end());
      auto packet_data =
          std::make_shared<std::vector<uint8_t>>(std::move(packet));

      int ret = rte_ring_enqueue(kv_migration_ring_, packet_data.get());
      if (ret < 0) {
        std::cerr << "Failed to enqueue migration packet to ring: " << ret
                  << std::endl;
      }

      std::shared_ptr<int> kv_migration_event_fd =
          kv_migration_event_fd_ptr_.lock();
      if (kv_migration_event_fd) {
        uint64_t val = 1;
        ssize_t bytes_written =
            write(*kv_migration_event_fd, &val, sizeof(val));
        if (bytes_written == -1) {
          perror("Failed to write to event_fd");
        } else {
          std::cout << "Written " << bytes_written << " bytes to event_fd"
                    << std::endl;
        }
      } else {
        std::cerr << "Event fd is no longer valid!" << std::endl;
      }
    }
  }
}

std::vector<uint8_t> ServerInstance::CompressKvPairs(
    const std::vector<KVPair> &kv_pairs) {
  const size_t per_kv_size = KEY_LENGTH + VALUE_LENGTH * 4;
  const size_t uncompressed_size = kv_pairs.size() * per_kv_size;
  std::vector<uint8_t> binary_data(uncompressed_size);

  auto *dst = binary_data.data();
  for (const auto &pair : kv_pairs) {
    static_assert(sizeof(pair.first[0]) == 1, "Requires byte-sized elements");
    static_assert(sizeof(pair.second[0]) == 1, "Requires byte-sized elements");

    memcpy(dst, pair.first.data(), KEY_LENGTH);
    memcpy(dst + KEY_LENGTH, pair.second.data(), VALUE_LENGTH);
    dst += per_kv_size;
  }

  uLongf compressed_size = compressBound(uncompressed_size);
  std::vector<uint8_t> compressed;
  compressed.resize(compressed_size);

  int ret =
      compress2(reinterpret_cast<Bytef *>(compressed.data()), &compressed_size,
                binary_data.data(), uncompressed_size, Z_BEST_SPEED);

  if (ret != Z_OK) {
    throw std::runtime_error("zlib compression failed: " + std::to_string(ret));
  }

  compressed.resize(compressed_size);
  compressed.shrink_to_fit();

  return compressed;
}

std::vector<ServerInstance::KVPair> ServerInstance::DecompressKvPairs(
    const std::vector<uint8_t> &compressed_data) {
  uLongf decompressed_size = compressBound(compressed_data.size());
  std::vector<uint8_t> decompressed_data(decompressed_size);

  int ret = uncompress(reinterpret_cast<Bytef *>(decompressed_data.data()),
                       &decompressed_size, compressed_data.data(),
                       compressed_data.size());

  if (ret != Z_OK) {
    throw std::runtime_error("zlib decompression failed: " +
                             std::to_string(ret));
  }

  decompressed_data.resize(decompressed_size);

  std::vector<KVPair> kv_pairs;
  const size_t per_kv_size = KEY_LENGTH + VALUE_LENGTH * 4;
  size_t total_kv_pairs = decompressed_size / per_kv_size;

  auto *data_ptr = decompressed_data.data();
  for (size_t i = 0; i < total_kv_pairs; ++i) {
    std::array<char, KEY_LENGTH> key;
    std::array<char, VALUE_LENGTH> value;

    memcpy(key.data(), data_ptr, KEY_LENGTH);
    memcpy(value.data(), data_ptr + KEY_LENGTH, VALUE_LENGTH);

    kv_pairs.push_back({std::string(key.begin(), key.end()),
                        std::string(value.begin(), value.end())});
    data_ptr += per_kv_size;
  }

  return kv_pairs;
}

void ServerInstance::HandleAsk(const std::vector<uint8_t> &packet) {
  const struct AskPacket *ask =
      reinterpret_cast<const struct AskPacket *>(packet.data() + IPV4_HDR_LEN);
  auto it = request_map_.find(ask->request_id);
  if (it != request_map_.end()) {
    it->second.set_value(true);
    request_map_.erase(it);
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
