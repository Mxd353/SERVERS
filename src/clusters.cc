#include "clusters.h"

#include <iostream>

void ServerCluster::InitServers() {
  int db = 0;
  auto shared_clusters =
      std::make_shared<const std::vector<std::vector<std::string>>>(
          clusters_info_);

  auto controller_info =
      std::make_shared<const ControllerInfo>(controller_info_);

  for (size_t rack_idx = 0; rack_idx < clusters_info_.size(); ++rack_idx) {
    std::vector<std::shared_ptr<ServerInstance>> servers;
    for (const auto& ip : clusters_info_[rack_idx]) {
      auto server = std::make_shared<ServerInstance>(
          ip, rack_idx, db++, controller_info, shared_clusters);
      servers.push_back(server);
    }
    clusters_.push_back(servers);
  }
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

std::vector<std::shared_ptr<ServerInstance>> ServerCluster::StartAll() {
  std::vector<std::shared_ptr<ServerInstance>> successful_servers;
  for (auto& cluster : clusters_) {
    successful_servers.push_back(server);
  }
  std::cout << "CLUTERS: Start " << successful_servers.size() << " servers for "
            << clusters_info_.size() << " racks.\n";
  return successful_servers;
}

void ServerCluster::StopAll() {
  for (auto& server : servers_) {
    server->Stop();
  }
}
