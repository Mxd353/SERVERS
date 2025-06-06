#include <atomic>
#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>

#include "cluster.h"

using ClusterInfo = ServerCluster::ClusterInfo;

std::atomic<bool> exit_requested(false);
std::unique_ptr<ServerCluster> cluster;

void signalHandler(int signal) {
  std::cout << "\nReceived shutdown signal: " << signal << std::endl;
  exit_requested = true;
  if (cluster) {
    cluster->StopAll();
  }
  std::exit(0);
}

std::vector<ClusterInfo> clusters_info;

void parseClusterInfo(const std::string& filename) {
  std::ifstream infile(filename);
  std::string line;
  ClusterInfo current_cluster;
  bool in_cluster_section = false;
  bool got_controller_info = false;

  while (std::getline(infile, line)) {
    line.erase(0, line.find_first_not_of(" \t\r\n"));
    line.erase(line.find_last_not_of(" \t\r\n") + 1);

    if (line.empty()) continue;

    if (line[0] == '#') {
      if (in_cluster_section) {
        clusters_info.push_back(current_cluster);
        current_cluster = ClusterInfo();
        got_controller_info = false;
      }
      in_cluster_section = true;
      continue;
    }

    std::istringstream iss(line);
    std::string part1, part2;
    iss >> part1 >> part2;

    if (!part2.empty() && !got_controller_info) {
      current_cluster.iface_to_controller = part1;
      current_cluster.controller_ip = part2;
      got_controller_info = true;
    } else {
      current_cluster.servers_ip.push_back(part1);
    }
  }

  if (in_cluster_section) {
    clusters_info.push_back(current_cluster);
  }
}

int main(int argc, char* argv[]) {
  std::string filename = (argc >= 2) ? argv[1] : "conf/server_ips.conf";
  parseClusterInfo(filename);
  if (clusters_info.empty()) {
    std::cerr << "No server IPs loaded. Exiting." << std::endl;
    return 1;
  }

  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  char* progrom_name = argv[0];
  std::string dpdk_conf = "conf/dpdk.conf";

  auto dpdk_hander = std::make_shared<DPDKHandler>();

  std::cout << "Starting server cluster >>" << std::endl;
  cluster = std::make_unique<ServerCluster>(clusters_info);
  auto servers = cluster->StartAll();

  bool success = dpdk_hander->Initialize(dpdk_conf, progrom_name, servers);
  if (!success) {
    std::cout << "dpdk_hander initialize faile >>" << std::endl;
    return 0;
  }

  dpdk_hander->Start();

  while (!exit_requested) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return 0;
}
