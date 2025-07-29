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
#define TX_NUM_MBUFS 1'048'575
#define RX_NUM_MBUFS 1'048'575
#define MBUF_CACHE_SIZE 512
#define TX_MBUF_DATA_SIZE 256
#define RX_MBUF_DATA_SIZE 2048
#define BURST_SIZE 32
#define RX_RING_SIZE 8192
#define TX_RING_SIZE 4096

class DPDKHandler {
  using CoreInfo = std::pair<uint, uint16_t>;

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
  int ret_;
  rte_ether_addr s_eth_addr_;
  std::shared_mutex ip_map_mutex_;
  std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>> ip_to_server_;
  std::unordered_map<rte_be32_t, std::shared_ptr<sw::redis::Redis>> ip_to_db_;
  std::vector<CoreInfo> special_cores_;
  std::vector<CoreInfo> normal_cores_;

  struct CoreArgs {
    CoreInfo core_info;
    DPDKHandler* instance;
  };

  std::vector<std::unique_ptr<CoreArgs>> core_args_;

  static inline void SwapMac(rte_ether_hdr* eth_hdr);
  static inline void SwapIpv4(rte_ipv4_hdr* ip_hdr);
  void MainLoop(CoreInfo core_info);
  void SpecialLoop(CoreInfo core_info);
  int PortInit();
  inline void BuildIptoServerMap(
      const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
          servers);

  inline void LaunchThreads(
      const std::vector<DPDKHandler::CoreInfo>& special_cores_,
      const std::vector<DPDKHandler::CoreInfo>& normal_cores_);
  static inline int LaunchNormalLcore(void* arg);
  static inline int LaunchSpeciaLcore(void* arg);

  inline std::shared_ptr<sw::redis::Redis> GetDbByIp(const rte_be32_t& ip) {
    auto it = ip_to_db_.find(ip);
    return it != ip_to_db_.end() ? it->second : nullptr;
  };

  inline std::shared_ptr<ServerInstance> GetServerByIp(const rte_be32_t& ip) {
    auto it = ip_to_server_.find(ip);
    return it != ip_to_server_.end() ? it->second : nullptr;
  };
};
