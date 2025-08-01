#include <atomic>
#include <csignal>
#include <fstream>
#include <iostream>
#include <sstream>

#include "arp_wrapper.h"
#include "clusters.h"
#include "lib/utils.h"

std::atomic<bool> exit_requested(false);
std::unique_ptr<ServerCluster> clusters;

extern std::atomic<uint64_t> total_latency_us;
extern std::atomic<size_t> completed_request_count;

void signalHandler(int signal) {
  std::cout << "\nReceived shutdown signal: " << signal << std::endl;

  uint64_t completed = completed_request_count.load(std::memory_order_relaxed);
  uint64_t total_latency = total_latency_us.load(std::memory_order_relaxed);

  exit_requested.store(true);
  if (clusters) {
    clusters->Stop();
    std::cout << "All clusters stop" << std::endl;
  }

  double latency_us_per_request =
      completed > 0 ? static_cast<double>(total_latency) / completed : 0.0;

  std::cout << "Total Requests Completed: " << completed << "\n"
            << "Average Latency per Request (ms/op): "
            << latency_us_per_request / 1'000.0 << std::endl;

  std::exit(0);
}

void parseClusterInfo(const std::string& server_conf,
                      const std::string& controller_conf,
                      std::vector<std::vector<std::string>>& racks,
                      ControllerInfo& controller_info) {
  std::ifstream server_file(server_conf);
  std::ifstream controller_file(controller_conf);
  std::string line;
  std::vector<std::string> current_rack;

  while (std::getline(controller_file, line)) {
    line.erase(0, line.find_first_not_of(" \t\r\n"));
    line.erase(line.find_last_not_of(" \t\r\n") + 1);

    if (line.empty() || line[0] == '#') {
      continue;
    }

    std::istringstream iss(line);
    std::string iface, ip;
    if (!(iss >> iface >> ip)) {
      std::cerr << "Invalid line: " << line << std::endl;
      continue;
    }
    std::string ip_real = "210.45.71.91";
    std::string mac = get_mac_from_python(ip_real);
    controller_info = {iface, ip, utils::ParseMac(mac)};

    std::cout << "RUN: to controller: " << iface << " | mac: " << mac
              << " | ip: " << ip << "\n";
  }

  while (std::getline(server_file, line)) {
    line.erase(0, line.find_first_not_of(" \t\r\n"));
    line.erase(line.find_last_not_of(" \t\r\n") + 1);

    if (line.empty()) continue;

    if (line[0] == '#') {
      if (!current_rack.empty()) {
        racks.push_back(current_rack);
        current_rack.clear();
      }
      continue;
    }

    current_rack.push_back(line);
  }

  if (!current_rack.empty()) {
    racks.push_back(current_rack);
  }
}

int main(int argc, char* argv[]) {
  (void)argc;
  std::string server_conf = "conf/server_ips.conf";
  std::string controller_conf = "conf/controller_info.conf";

  std::vector<std::vector<std::string>> racks;
  ControllerInfo controller_info;

  parseClusterInfo(server_conf, controller_conf, racks, controller_info);
  if (racks.empty()) {
    std::cerr << "No server IPs loaded. Exiting." << std::endl;
    return 1;
  }

  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  char* progrom_name = argv[0];
  std::string dpdk_conf = "conf/dpdk.conf";

  auto dpdk_hander = std::make_shared<DPDKHandler>();

  std::cout << "RUN: Starting server cluster >>" << std::endl;
  clusters = std::make_unique<ServerCluster>(racks, controller_info);
  auto servers = clusters->GetIpToServerMap();

  bool success = dpdk_hander->Initialize(dpdk_conf, progrom_name, servers);
  if (!success) {
    std::cout << "dpdk_hander initialize faile >>" << std::endl;
    return 0;
  }

  clusters->Start(1);
  dpdk_hander->Start();

  while (!exit_requested.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  return 0;
}
