#include "cluster.h"

#include <iostream>

#include "arp_wrapper.h"

void ServerCluster::InitServers() {
  int db = 0;
  auto shared_clusters =
      std::make_shared<const std::vector<ClusterInfo>>(clusters_info_);

  std::string ip = "210.45.71.91";
  std::string controller_mac = get_mac_from_python(ip);
  std::cout << "CLUTERS: MAC for " << ip << " is " << controller_mac << std::endl;

  for (size_t rack_idx = 0; rack_idx < clusters_info_.size(); ++rack_idx) {
    for (const auto& ip : clusters_info_[rack_idx].servers_ip) {
      auto server = std::make_shared<ServerInstance>(
          ip, rack_idx, db++, clusters_info_[rack_idx].iface_to_controller,
          controller_mac, clusters_info_[rack_idx].controller_ip,
          shared_clusters);
      servers_.push_back(server);
    }
  }
}

std::vector<std::shared_ptr<ServerInstance>> ServerCluster::StartAll() {
  std::vector<std::shared_ptr<ServerInstance>> successful_servers;
  for (auto& server : servers_) {
    if (server->Start()) {
      successful_servers.push_back(server);
    }
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
