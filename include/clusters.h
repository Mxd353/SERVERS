#pragma once

#include <mutex>

#include "dpdk_handler.h"

class ServerCluster {
 public:
  ServerCluster(const std::vector<std::vector<std::string>> &clusters_info,
                const ControllerInfo &controller_info)
      : clusters_info_(clusters_info), controller_info_(controller_info) {
    InitServers();
  }
  std::vector<std::shared_ptr<ServerInstance>> StartAll();
  void StopAll();

 private:
  const std::vector<std::vector<std::string>> &clusters_info_;
  const ControllerInfo &controller_info_;
  std::vector<std::vector<std::shared_ptr<ServerInstance>>> clusters_;
  std::mutex cluster_mutex_;
  int sockfd_;

  void InitServers();
  bool InitSocket();
};
