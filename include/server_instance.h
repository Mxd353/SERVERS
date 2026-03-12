#pragma once

#include <net/ethernet.h>
#include <netinet/ip.h>
#include <netinet/udp.h>
#include <rte_ring.h>

#include <atomic>
#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>

#include "lib/c_m_proto.h"
#include "lib/request_map.h"

class ServerInstance {
 public:
  using KVPair = std::pair<std::string, std::string>;
  using ClusterInfo = std::vector<std::vector<std::string>>;

  struct ServerInfo {
    int rack_id;
    std::string ip;
    std::array<uint8_t, ETH_ALEN> mac;
    int db;
  };

  ServerInstance(
      const ServerInfo& server_info,
      std::shared_ptr<const c_m_proto::SockConfig> sock_config,
      std::shared_ptr<const c_m_proto::ControllerInfo> controller_info,
      std::shared_ptr<const ClusterInfo> clusters_info);
  ~ServerInstance();

  [[nodiscard]] uint32_t GetIp() const noexcept { return server_ip_in_; }
  [[nodiscard]] int GetDb() const noexcept { return server_info_.db; }

  void SetKvMigrationRing(struct rte_ring* ring);
  void CacheMigrate(const std::string_view& key, uint32_t migration_id);
  void HandlePacket(const std::vector<uint8_t>& packet);

 private:
  ServerInfo server_info_;
  std::shared_ptr<const c_m_proto::SockConfig> sock_config_;
  std::shared_ptr<const c_m_proto::ControllerInfo> controller_info_;
  std::shared_ptr<const ClusterInfo> clusters_info_;

  uint32_t server_ip_in_;
  uint32_t controller_ip_in_;

  uint index_base_;
  uint index_limit_;

  struct rte_ring* kv_migration_ring_;
  std::weak_ptr<int> kv_migration_event_fd_ptr_;

  std::atomic<bool> running_{false};

  uint8_t fsm_;

  // RequestMap<uint32_t, std::promise<bool>> request_map_;

  template <typename PayloadType>
  auto ConstructPacket(std::unique_ptr<PayloadType> payload, uint32_t dst_ip,
                       uint32_t src_ip) -> std::vector<uint8_t>;
  bool SendPacket(const std::vector<uint8_t>& packet);
  void HandleMigrationInfo(const std::vector<uint8_t>& packet);
  auto HashToIps(const std::vector<uint32_t>& indices,
                 const std::vector<std::string>& ip_list)
      -> std::vector<std::pair<std::string, uint32_t>>;
  inline auto ConstructMigratePacket(uint32_t dst_ip, uint32_t src_ip,
                                     uint16_t index, uint32_t migration_id,
                                     uint8_t dst_rack_id, uint16_t index_size)
      -> std::vector<uint8_t>;
  void StartMigration(const std::vector<uint8_t>& packet);
};
