#pragma once

#include <net/ethernet.h>
#include <netinet/ip.h>
#include <rte_ring.h>
#include <sw/redis++/async_redis++.h>
#include <sw/redis++/redis++.h>

#include <atomic>
#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>

#include "lib/c_m_proto.h"
#include "lib/request_map.h"

constexpr size_t CHUNK_SIZE = 32;

class ServerInstance {
 public:
  typedef std::pair<std::string, std::string> KVPair;

  struct ClusterInfo {
    std::string iface_to_controller;
    std::string controller_ip;
    std::vector<std::string> servers_ip;
  };

  ServerInstance(const std::string& server_ip, int rack_id, int db,
                 const std::string& iface_to_controller,
                 const std::string& controller_mac,
                 const std::string& controller_ip,
                 std::shared_ptr<const std::vector<ClusterInfo>> clusters_info);
  ~ServerInstance();

  bool Start();
  void Stop();

  [[nodiscard]] uint32_t GetIp() const noexcept { return server_ip_in_; }
  [[nodiscard]] int GetDb() const noexcept { return db_; }

  void SetKvMigrationRing(struct rte_ring* ring,
                          std::shared_ptr<int> kv_migration_event_fd_ptr);
  void CacheMigrate(const std::string_view& key, uint32_t migration_id);
  void HandleMigrateReply(uint32_t request_id);

 private:
  uint8_t src_mac_[ETH_ALEN];
  std::string server_ip_;
  int rack_id_;
  int db_;
  std::string iface_to_controller_;
  std::string controller_mac_;
  std::string controller_ip_;

  uint32_t server_ip_in_;
  uint32_t controller_ip_in_;

  std::shared_ptr<const std::vector<ClusterInfo>> clusters_info_;

  uint index_base_;
  uint index_limit_;

  struct rte_ring* kv_migration_ring_;
  std::weak_ptr<int> kv_migration_event_fd_ptr_;

  int sockfd_;
  int ifindex_;
  std::thread recv_thread_;
  std::atomic<bool> stop_receive_thread_{false};
  boost::asio::thread_pool worker_pool_{4};
  std::atomic<bool> running_{false};
  std::unique_ptr<sw::redis::Redis> redis_;

  uint8_t fsm_;

  RequestMap<uint32_t, std::promise<bool>> request_map_;

  template <typename PayloadType>
  std::vector<uint8_t> ConstructPacket(std::unique_ptr<PayloadType> payload,
                                       uint32_t dst_ip, uint32_t src_ip);
  bool SendPacket(const std::vector<uint8_t>& packet);
  void ReceiveThread();
  void ProcessPacket(const std::vector<uint8_t>& packet);
  void HandleMigrationInfo(const std::vector<uint8_t>& packet);
  std::vector<std::pair<std::string, uint>> HashToIps(
      std::vector<uint> indices, const std::vector<std::string>& ip_list);
  void StartMigration(const std::vector<uint8_t>& packet);
  std::vector<uint> SampleIndices(size_t sample_size);
  void HandleAsk(const std::vector<uint8_t>& packet);
  bool SendAsk(uint32_t request_id, uint32_t dst_ip, uint8_t ask = 1);
};
