#pragma once

#include <linux/if_packet.h>
#include <rte_eal.h>
#include <rte_ether.h>
#include <rte_ip4.h>
#include <rte_mbuf.h>

#include "server_instance.h"

#define RING_SIZE 1024
#define RTE_LOGTYPE_RING RTE_LOGTYPE_USER1
#define RTE_LOGTYPE_DB RTE_LOGTYPE_USER2
#define RTE_LOGTYPE_CORE RTE_LOGTYPE_USER3
#define TX_NUM_MBUFS 16'383
#define RX_NUM_MBUFS 32'767
#define MBUF_CACHE_SIZE 256
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 512
#define BURST_SIZE 32
#define RX_RING_SIZE 2048
#define TX_RING_SIZE 1024
#define SAFETY_FACTOR 1.5
#define WORKER_NUM 32
#define RX_CORE_NUM 8
#define TX_CORE_NUM 2

constexpr int SUBNET_BASE = 0;
constexpr int SUBNET_COUNT = 32;
constexpr int HOST_PER_SUBNET = 32;


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
  std::array<rte_ring*, WORKER_NUM> rx_rings_;
  std::array<rte_ring*, TX_CORE_NUM> tx_rings_;

  enum class CoreType { Rx, Tx, NONE };

  struct CoreInfo {
    uint lcore_id;
    uint16_t queue_id;
    CoreType type;
    std::array<rte_ring*, WORKER_NUM>* rx_rings;
    rte_ring* tx_ring;

    CoreInfo(uint l = 0, CoreType t = CoreType::NONE,
             std::array<rte_ring*, WORKER_NUM>* rxrs = nullptr,
             rte_ring* txr = nullptr)
        : lcore_id(l), queue_id(0), type(t), rx_rings(rxrs), tx_ring(txr) {}
  };

  std::vector<CoreInfo> rx_cores_;
  std::vector<CoreInfo> tx_cores_;

  struct CoreArgs {
    CoreInfo core_info;
    DPDKHandler* instance;
  };

  struct ipc_req {
    uint8_t db_id;
    rte_mbuf* mbuf;
  };

  std::vector<std::unique_ptr<CoreArgs>> core_args_;

  static inline void SwapMac(rte_ether_hdr* eth_hdr);
  static inline void SwapIpv4(rte_ipv4_hdr* ip_hdr);
  int PortInit();
  void RxLoop(CoreInfo core_info);
  void TxLoop(CoreInfo core_info);
  void DBWorker(std::pair<uint, uint> port_range, rte_ring* rx_ring);

  inline void BuildIptoServerMap(
      const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
          servers);
  inline void LaunchThreads(
      const std::vector<DPDKHandler::CoreInfo>& rx_cores_,
      const std::vector<DPDKHandler::CoreInfo>& tx_cores_);
  static inline int LaunchRxLcore(void* arg);
  static inline int LaunchTxLcore(void* arg);

  inline std::shared_ptr<ServerInstance> GetServerByIp(const rte_be32_t& ip) {
    auto it = ip_to_server_.find(ip);
    return it != ip_to_server_.end() ? it->second : nullptr;
  };
};
