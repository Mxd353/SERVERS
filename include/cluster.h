#pragma once

#include <mutex>

#include "dpdk_handler.h"

class ServerCluster {
 public:
  using ClusterInfo = ServerInstance::ClusterInfo;

  ServerCluster(const std::vector<ClusterInfo> &clusters_info)
      : clusters_info_(clusters_info) {
    InitServers();
  }
  std::vector<std::shared_ptr<ServerInstance>> StartAll();
  void StopAll();

 private:
  std::vector<ClusterInfo> clusters_info_;
  std::vector<std::shared_ptr<ServerInstance>> servers_;
  std::mutex cluster_mutex_;

  void InitServers();
};
