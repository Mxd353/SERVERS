#pragma once

#include <linux/if_packet.h>
#include <rte_eal.h>
#include <rte_ether.h>
#include <rte_ip4.h>
#include <rte_mbuf.h>

#include <boost/asio.hpp>
#include <boost/redis.hpp>

#include "server_instance.h"

#define RTE_LOGTYPE_RING RTE_LOGTYPE_USER1
#define RTE_LOGTYPE_DB RTE_LOGTYPE_USER2
#define RTE_LOGTYPE_CORE RTE_LOGTYPE_USER3
#define RTE_LOGTYPE_WORKER RTE_LOGTYPE_USER4

#define TX_NUM_MBUFS 16'383
#define RX_NUM_MBUFS 32'767
#define MBUF_CACHE_SIZE 256
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 512
#define BURST_SIZE 32
#define RX_RING_SIZE 2048
#define TX_RING_SIZE 1024
#define SAFETY_FACTOR 1.5

constexpr uint32_t RX_CORE_NUM = 8;
constexpr uint32_t WORKER_CORE_NUM = RX_CORE_NUM;
constexpr uint32_t TX_CORE_NUM = 2;
constexpr uint32_t TOTAL_CORE_NUM = RX_CORE_NUM + WORKER_CORE_NUM + TX_CORE_NUM;
constexpr uint32_t NUM_RACKS = 32;
constexpr uint32_t RING_SIZE = 262144;
constexpr uint32_t RACK_PER_WORKER = NUM_RACKS / WORKER_CORE_NUM;
constexpr uint32_t DB_PER_RACK = 32;
constexpr uint32_t TOTAL_DB_NUM = DB_PER_RACK * NUM_RACKS;
constexpr uint32_t DBS_PER_WORKER = TOTAL_DB_NUM / WORKER_CORE_NUM;

extern std::atomic<uint32_t> next_db_id;

class DPDKHandler {
 public:
  DPDKHandler();
  ~DPDKHandler();

  bool Initialize(
      const std::string& conf, char* program_name,
      const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
          servers);
  void Start();
  void Stop();
  rte_ring* kv_migration_ring;

 private:
  volatile bool initialized_ = false;
  rte_mempool* tx_mbufpool_;
  rte_mempool* rx_mbufpool_;
  rte_mempool* ipc_mempool_;
  int ret_;
  rte_ether_addr s_eth_addr_;
  std::shared_mutex ip_map_mutex_;
  std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>> ip_to_server_;
  std::array<rte_ring*, RX_CORE_NUM> rx_rings_;
  std::array<rte_ring*, TX_CORE_NUM> tx_rings_;

  std::array<uint8_t, NUM_RACKS> worker_table;

  enum class CoreType { RX_CORE, WORKER_CORE, TX_CORE, NONE };

  struct CoreInfo {
    uint lcore_id;
    uint16_t queue_id;
    CoreType type;
    std::array<rte_ring*, RX_CORE_NUM>* rx_rings;
    std::pair<rte_ring*, rte_ring*> work_rings;
    rte_ring* tx_ring;

    CoreInfo(uint l = 0, CoreType t = CoreType::NONE,
             std::array<rte_ring*, RX_CORE_NUM>* rxrs = nullptr,
             std::pair<rte_ring*, rte_ring*> wr = {nullptr, nullptr},
             rte_ring* txr = nullptr)
        : lcore_id(l),
          queue_id(0),
          type(t),
          rx_rings(rxrs),
          work_rings(wr),
          tx_ring(txr) {}
  };

  std::vector<CoreInfo> all_cores_;

  struct CoreArgs {
    const CoreInfo* core_info;
    DPDKHandler* instance;
  };

  struct ipc_req {
    uint8_t db_id;
    rte_mbuf* mbuf;
  };

  std::vector<std::unique_ptr<CoreArgs>> core_args_;

  static inline void SwapMac(rte_ether_hdr* eth_hdr);
  static inline rte_be32_t SwapIpv4(rte_ipv4_hdr* ip_hdr);
  int PortInit();
  inline int LookupWorker(rte_be32_t ip_be, uint8_t& worker_out,
                          uint8_t& db_out);
  void RxLoop(CoreInfo core_info);
  void TxLoop(CoreInfo core_info);
  void DBWorker(CoreInfo core_info);

  inline void BuildIptoServerMap(
      const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
          servers);
  inline void CreatRings();
  inline void InitAndLaunchCores();
  inline void CreatPool();
  static inline int LaunchRxLcore(void* arg);
  static inline int LaunchTxLcore(void* arg);
  static inline int LaunchWorkerLcore(void* arg);
  inline void LaunchThreads();

  inline auto GetServerByIp(rte_be32_t ip) -> std::shared_ptr<ServerInstance> {
    auto it = ip_to_server_.find(ip);
    return (it != ip_to_server_.end()) ? it->second : nullptr;
  }
};
