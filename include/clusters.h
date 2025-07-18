#pragma once

#include <mutex>

#include "dpdk_handler.h"

class ServerCluster {
 public:
  using ServerMap =
      std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>;

  ServerCluster(const std::vector<std::vector<std::string>> &clusters_info,
                const ControllerInfo &controller_info)
      : clusters_info_(clusters_info), controller_info_(controller_info) {
    InitServers();
  }
  ~ServerCluster();
  void Start(int thread_count);
  void Stop();
  const ServerMap &GetIpToServerMap() const;

 private:
  const std::vector<std::vector<std::string>> &clusters_info_;
  const ControllerInfo &controller_info_;
  std::mutex cluster_mutex_;
  int sockfd_ = -1;
  int ifindex_ = -1;
  std::atomic<bool> stop_receive_thread_{false};
  boost::asio::thread_pool worker_pool_{32};
  std::array<uint8_t, ETH_ALEN> src_mac_;

  std::shared_mutex ip_map_mutex_;
  ServerMap ip_to_server_;
  std::vector<std::thread> receive_threads_;

  void InitServers();
  bool InitSocket();
  void StartReceiveThreads(int thread_count);
  void ReceiveThread(
      std::vector<std::shared_ptr<ServerInstance>> servers_subset);
  inline std::shared_ptr<ServerInstance> GetServerByIp(const rte_be32_t &ip) {
    std::shared_lock lock(ip_map_mutex_);
    auto it = ip_to_server_.find(ip);
    return it != ip_to_server_.end() ? it->second : nullptr;
  };
};
