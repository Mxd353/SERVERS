#pragma once

#include <net/ethernet.h>
#include <netinet/ip.h>
#include <rte_ring.h>
#include <sw/redis++/async_redis++.h>
#include <sw/redis++/redis++.h>

#include <atomic>
#include <boost/asio.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>

#include "migration_state_machine.h"

constexpr uint16_t IPV4_HDR_LEN = sizeof(struct iphdr);

constexpr size_t BATCH_SIZE = 32;

class ServerInstance : public MigrationStateMachine::Observer {
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
  [[nodiscard]] bool GetKVMigrationIn() const noexcept {
    return kv_migration_in_;
  }
  void SetKvMigrationRing(struct rte_ring* ring,
                          std::shared_ptr<int> kv_migration_event_fd_ptr);
  void HotReport(const std::string& key, std::string_view value, int count);
  void HandleKVMigration(struct KV_Migration* kv_migration_hdr, uint8_t* data,
                         size_t data_len);
  void PerWrite(const std::string_view& key);

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

  struct rte_ring* kv_migration_ring_;
  std::weak_ptr<int> kv_migration_event_fd_ptr_;

  int sockfd_;
  int ifindex_;
  std::thread recv_thread_;
  std::atomic<bool> stop_receive_thread_{false};
  boost::asio::thread_pool worker_pool_{4};
  std::atomic<bool> running_{false};
  std::unique_ptr<sw::redis::Redis> redis_;

  MigrationStateMachine fsm_;
  std::atomic<bool> kv_migration_in_{false};
  std::atomic<uint8_t> kv_migration_in_id_{0};
  std::unordered_map<std::string, std::string> kv_migration_in_cache_;
  std::shared_mutex kv_cache_mutex_;

  std::mutex hot_keys_mutex_;
  std::unordered_set<std::string> confirmed_hot_keys_;
  std::unordered_set<std::string> pending_hot_keys_;

  std::mutex request_map_mutex_;
  std::unordered_map<uint32_t, std::promise<bool>> request_map_;

  template <typename PayloadType>
  std::vector<uint8_t> ConstructPacket(std::unique_ptr<PayloadType> payload,
                                       uint32_t dst_ip, uint32_t src_ip);
  bool SendPacket(const std::vector<uint8_t>& packet);
  void ReceiveThread();
  void ProcessPacket(const std::vector<uint8_t>& packet);
  void HandleHotReply(const std::vector<uint8_t>& packet);
  void onStateChange(MigrationStateMachine::State new_state,
                     uint32_t migration_id) override;
  void HandleMigrationInfo(const std::vector<uint8_t>& packet);
  void HandleMigrationReady(const std::vector<uint8_t>& packet);
  void SendMigrationReady();
  void HandleMigrationStart(uint32_t request_id);
  std::unordered_map<std::string, std::vector<KVPair>> HashKeysToIps(
      const std::vector<KVPair>& kv_pairs,
      const std::vector<std::string>& ip_list);
  void StartMigration();
  std::vector<uint8_t> CompressKvPairs(const std::vector<KVPair>& kv_pairs);
  std::vector<KVPair> DecompressKvPairs(
      const std::vector<uint8_t>& compressed_data);
  void HandleAsk(const std::vector<uint8_t>& packet);
  bool SendAsk(uint32_t request_id, uint32_t dst_ip, uint8_t ask = 1);
};
