#pragma once

#include <linux/if_packet.h>
#include <rte_eal.h>
#include <rte_ether.h>
#include <rte_ip4.h>
#include <rte_mbuf.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/redis.hpp>
#include <chrono>

#include "server_instance.h"

#define RTE_LOGTYPE_RING RTE_LOGTYPE_USER1
#define RTE_LOGTYPE_DB RTE_LOGTYPE_USER2
#define RTE_LOGTYPE_CORE RTE_LOGTYPE_USER3
#define RTE_LOGTYPE_WORKER RTE_LOGTYPE_USER4
#define RTE_LOGTYPE_TX RTE_LOGTYPE_USER5
#define RTE_LOGTYPE_RX RTE_LOGTYPE_USER6

#define MBUF_NUM 49'150
#define MBUF_CACHE_SIZE 256
#define MBUF_DATA_SIZE 512
#define BURST_SIZE 32
#define RX_RING_SIZE 2048
#define TX_RING_SIZE 1024
#define SAFETY_FACTOR 1.5
#define DB_BASE_PORT 6380

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
constexpr double REDIS_RATE_LIMIT_QPS = 1000.0;  // 每个 Redis 限速 1000 QPS

extern std::atomic<uint32_t> next_db_id;

// 令牌桶限速器
struct TokenBucket {
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;
  using Duration = Clock::duration;

  std::atomic<double> tokens{REDIS_RATE_LIMIT_QPS};
  std::atomic<uint64_t> last_update_ns;  // 存储纳秒时间戳
  const double max_tokens{REDIS_RATE_LIMIT_QPS};
  const double rate_per_sec{REDIS_RATE_LIMIT_QPS};

  TokenBucket() {
    auto now = Clock::now().time_since_epoch();
    last_update_ns.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count(),
        std::memory_order_relaxed);
  }

  // 尝试消费令牌，返回是否成功
  bool TryConsume(double consume = 1.0) {
    auto now = Clock::now();
    uint64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          now.time_since_epoch())
                          .count();
    auto prev_ns = last_update_ns.load(std::memory_order_relaxed);

    // 计算时间差（秒），添加新令牌
    double elapsed = static_cast<double>(now_ns - prev_ns) / 1e9;
    double current = tokens.load(std::memory_order_relaxed);
    double new_tokens = std::min(current + elapsed * rate_per_sec, max_tokens);

    if (new_tokens >= consume) {
      tokens.store(new_tokens - consume, std::memory_order_relaxed);
      last_update_ns.store(now_ns, std::memory_order_relaxed);
      return true;
    }
    return false;
  }
};

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
  void RequestStop() { stop_requested_.store(true, std::memory_order_relaxed); }
  [[nodiscard]] auto IsStopRequested() const -> bool {
    return stop_requested_.load(std::memory_order_relaxed);
  }
  rte_ring* kv_migration_ring;

 private:
  volatile bool initialized_ = false;
  std::atomic<bool> stop_requested_{false};
  rte_mempool* mbufpool_;
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
    uint16_t db_id;
    rte_be32_t src_ip;
    rte_mbuf* mbuf;
  };

  std::vector<std::unique_ptr<CoreArgs>> core_args_;

  static inline void SwapMac(rte_ether_hdr* eth_hdr);
  static inline rte_be32_t SwapIpv4(rte_ipv4_hdr* ip_hdr);
  int PortInit();
  inline int LookupWorker(rte_be32_t ip_be, uint16_t& worker_out,
                          uint16_t& db_out);
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
