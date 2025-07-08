#include "clusters.h"

#include <iostream>

void ServerCluster::InitServers() {
  int db = 0;
  auto shared_clusters =
      std::make_shared<const std::vector<std::vector<std::string>>>(
          clusters_info_);

  auto controller_info =
      std::make_shared<const ControllerInfo>(controller_info_);

  if (!InitSocket()) {
    std::cerr << "Faile to init socket." << std::endl;
    return;
  }

  auto sock_config =
      std::make_shared<const SockConfig>(SockConfig{sockfd_, ifindex_});

  for (size_t rack_idx = 0; rack_idx < clusters_info_.size(); ++rack_idx) {
    for (const auto &ip : clusters_info_[rack_idx]) {
      ServerInstance::ServerInfo server_info = {static_cast<int>(rack_idx), ip,
                                                src_mac_, db++};
      auto server = std::make_shared<ServerInstance>(
          server_info, sock_config, controller_info, shared_clusters);
      rte_be32_t be32_ip = server->GetIp();
      ip_to_server_.emplace(be32_ip, server);
    }
  }
  std::cout << "Start " << ip_to_server_.size() << " servers from "
            << clusters_info_.size() << "racks.\n";
}

ServerCluster::~ServerCluster() { Stop(); }

void ServerCluster::Start(int thread_count) {
  StartReceiveThreads(thread_count);
}

void ServerCluster::Stop() {
  stop_receive_thread_.store(true, std::memory_order_relaxed);

  for (auto &t : receive_threads_) {
    if (t.joinable()) t.join();
  }
  receive_threads_.clear();

  if (sockfd_ >= 0) {
    close(sockfd_);
    sockfd_ = -1;
  }

  worker_pool_.join();
}

bool ServerCluster::InitSocket() {
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

  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  snprintf(ifr.ifr_name, sizeof(ifr.ifr_name), "%s",
           controller_info_.iface.c_str());

  if (ioctl(sockfd_, SIOCGIFINDEX, &ifr) < 0) {
    close(sockfd_);
    throw std::runtime_error("Failed to get interface index");
  }

  ifindex_ = ifr.ifr_ifindex;

  if (ioctl(sockfd_, SIOCGIFHWADDR, &ifr) < 0) {
    close(sockfd_);
    throw std::runtime_error("ioctl(SIOCGIFHWADDR) failed");
  }
  memcpy(src_mac_.data(), ifr.ifr_hwaddr.sa_data, ETH_ALEN);

  return true;
}

void ServerCluster::StartReceiveThreads(int thread_count) {
  for (int i = 0; i < thread_count; ++i) {
    receive_threads_.emplace_back(&ServerCluster::ReceiveThread, this);
  }
}

void ServerCluster::ReceiveThread() {
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
      if (memcmp(eth_hdr->h_source, src_mac_.data(), ETH_ALEN) == 0) continue;
      if (ntohs(eth_hdr->h_proto) != ETH_P_IP) continue;
      if (recvlen < ETH_HLEN + IPV4_HDR_LEN) {
        std::cerr << "[Recv] Packet too short: " << recvlen << " bytes.\n";
        continue;
      }
      struct iphdr *ip_hdr =
          reinterpret_cast<struct iphdr *>(buffer.data() + ETH_HLEN);
      if (!IsValidDstIp(ip_hdr->daddr)) {
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

void ServerCluster::ProcessPacket(const std::vector<uint8_t> &packet) {
  const struct iphdr *ip_hdr =
      reinterpret_cast<const struct iphdr *>(packet.data() + ETH_HLEN);
  if (auto server = GetServerByIp(ip_hdr->daddr)) {
    server->HandlePacket(packet);
  }
}

const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>> &
ServerCluster::GetIpToServerMap() const {
  return ip_to_server_;
}
