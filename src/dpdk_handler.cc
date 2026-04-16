#include "dpdk_handler.h"

#include <rte_byteorder.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mempool.h>
#include <rte_per_lcore.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>

#include "lib/utils.h"

/**
RX lcore
      ↓
per-rack RTSSC ring
      ↓
Worker lcore
      ↓
TX lcore
*/

// 10.0.<rack>.<host>

namespace redis = boost::redis;

using namespace utils;

[[maybe_unused]] std::atomic<uint64_t> total_latency_us{0};
[[maybe_unused]] std::atomic<size_t> completed_request_count{0};
std::atomic<uint32_t> next_db_id{0};

// std::ofstream outfile("server.output");

DPDKHandler::DPDKHandler() {}

DPDKHandler::~DPDKHandler() {
  std::cout << "[DPDK] DPDKHandler destructor called\n";
  Stop();
}

inline void DPDKHandler::BuildIptoServerMap(
    const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
        servers) {
  uint db_size = 0;
  for (const auto& server : servers) {
    if (!server.second) {
      RTE_LOG(ERR, DB, "Invalid ServerInstance pointer!\n");
      continue;
    }
    std::unique_lock lock(ip_map_mutex_);
    ip_to_server_.emplace(server.first, server.second);
    db_size++;
  }
  RTE_LOG(INFO, DB, "Init %d Redis for servers.\n", db_size);
}

bool DPDKHandler::Initialize(
    const std::string& conf, char* program_name,
    const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>&
        servers) {
  std::vector<std::string> args;
  args.push_back(program_name);
  std::ifstream file(conf);
  std::string token;
  uint16_t nb_ports;

  BuildIptoServerMap(servers);

  while (file >> token) {
    args.push_back(token);
  }

  std::vector<char*> argv;
  for (auto& arg : args) {
    argv.push_back(const_cast<char*>(arg.c_str()));
  }

  ret_ = rte_eal_init(argv.size(), argv.data());
  if (ret_ < 0) {
    std::cerr << "DPDK EAL initialization failed\n";
    rte_exit(EXIT_FAILURE, "Error with EAL initialization\n");
    return false;
  }

  kv_migration_ring =
      rte_ring_create("kv_migration_ring", RING_SIZE, rte_socket_id(),
                      RING_F_MP_RTS_ENQ | RING_F_MC_RTS_DEQ);
  if (kv_migration_ring == nullptr) {
    rte_exit(EXIT_FAILURE, "Cannot create kv_migration_ring\n");
  }

  for (const auto& server : servers) {
    server.second->SetKvMigrationRing(kv_migration_ring);
  }

  nb_ports = rte_eth_dev_count_avail();
  if (nb_ports < 1) rte_exit(EXIT_FAILURE, "Error: need at least one port\n");

  initialized_ = true;
  return initialized_;
}

inline void DPDKHandler::SwapMac(rte_ether_hdr* eth_hdr) {
  rte_ether_addr tmp_mac;
  rte_ether_addr_copy(&eth_hdr->dst_addr, &tmp_mac);
  rte_ether_addr_copy(&eth_hdr->src_addr, &eth_hdr->dst_addr);
  rte_ether_addr_copy(&tmp_mac, &eth_hdr->src_addr);
}

inline rte_be32_t DPDKHandler::SwapIpv4(rte_ipv4_hdr* ip_hdr) {
  rte_be32_t tmp_ip = ip_hdr->dst_addr;
  ip_hdr->dst_addr = ip_hdr->src_addr;
  ip_hdr->src_addr = tmp_ip;
  return tmp_ip;
}

int DPDKHandler::PortInit() {
  uint16_t port = 0;
  uint16_t rx_ring_size = RX_RING_SIZE;
  uint16_t tx_ring_size = TX_RING_SIZE;
  int retval;

  if (!rte_eth_dev_is_valid_port(port)) return -1;

  static struct rte_eth_conf port_conf_default;

  struct rte_eth_rxmode tmp_rxmode;
  memset(&tmp_rxmode, 0, sizeof(rte_eth_rxmode));
  tmp_rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
  tmp_rxmode.mtu = 256;

  rte_eth_rss_conf rss_conf;
  rss_conf.rss_hf = RTE_ETH_RSS_IP | RTE_ETH_RSS_UDP;
  rss_conf.rss_key_len = 0;
  rss_conf.rss_key = NULL;

  port_conf_default.rxmode = tmp_rxmode;
  port_conf_default.rx_adv_conf.rss_conf = rss_conf;

  rte_eth_conf port_conf = port_conf_default;

  rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval != 0) {
    RTE_LOG(ERR, EAL, "Error during getting device (port %u) info: %s\n", port,
            strerror(-retval));
    return retval;
  }

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  if (dev_info.rx_offload_capa & RTE_ETH_RX_OFFLOAD_RSS_HASH)
    port_conf.rxmode.offloads |= RTE_ETH_RX_OFFLOAD_RSS_HASH;

  struct rte_eth_rxconf rx_conf;
  rx_conf = dev_info.default_rxconf;
  rx_conf.offloads = port_conf.rxmode.offloads;

  port_conf.rx_adv_conf.rss_conf.rss_hf &= dev_info.flow_type_rss_offloads;
  if (port_conf.rx_adv_conf.rss_conf.rss_hf !=
      port_conf_default.rx_adv_conf.rss_conf.rss_hf) {
    printf(
        "Port %u modified RSS hash function based on hardware support,"
        "requested:%#" PRIx64 " configured:%#" PRIx64 "\n",
        port, port_conf_default.rx_adv_conf.rss_conf.rss_hf,
        port_conf.rx_adv_conf.rss_conf.rss_hf);
  }

  /* Configure the Ethernet device. */
  uint16_t nb_rx_cores = RX_CORE_NUM;
  uint16_t nb_tx_cores = TX_CORE_NUM;

  retval = rte_eth_dev_configure(port, nb_rx_cores, nb_tx_cores, &port_conf);
  if (retval != 0) return retval;

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &rx_ring_size, &tx_ring_size);
  if (retval != 0) return retval;

  rte_eth_txconf txconf;
  txconf = dev_info.default_txconf;
  txconf.offloads = port_conf_default.txmode.offloads;

  int rx_count = 0;
  for (auto& core : all_cores_) {
    if (core.type == CoreType::RX_CORE) {
      assert(rx_count < nb_rx_cores);

      retval = rte_eth_rx_queue_setup(port, rx_count, rx_ring_size,
                                      rte_eth_dev_socket_id(port), &rx_conf,
                                      mbufpool_);
      if (retval < 0) return retval;
      core.queue_id = rx_count;
      rx_count++;
    }
  }

  if (rx_count != nb_rx_cores) {
    RTE_LOG(ERR, EAL, "Mismatch: Expected %u RX cores, found %u\n", nb_rx_cores,
            rx_count);
    return -1;
  }

  uint16_t tx_count = 0;
  for (auto& core : all_cores_) {
    if (core.type == CoreType::TX_CORE) {
      assert(tx_count < nb_tx_cores);

      retval = rte_eth_tx_queue_setup(port, tx_count, tx_ring_size,
                                      rte_eth_dev_socket_id(port), &txconf);
      if (retval < 0) return retval;
      core.queue_id = tx_count;
      tx_count++;
    }
  }

  if (tx_count != nb_tx_cores) {
    RTE_LOG(ERR, EAL, "Mismatch: Expected %u TX cores, found %u\n", nb_tx_cores,
            tx_count);
    return -1;
  }

  /* Starting Ethernet port. 8< */
  retval = rte_eth_dev_start(port);
  if (retval < 0) return retval;

  struct rte_ether_addr bbdev_port_eth_addr;
  retval = rte_eth_macaddr_get(port, &bbdev_port_eth_addr);
  if (retval < 0) {
    printf("rte_eth_macaddr_get: err=%d\n", retval);
    return -1;
  }
  RTE_LOG(INFO, EAL, "Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n",
          (unsigned)port, RTE_ETHER_ADDR_BYTES(&bbdev_port_eth_addr));

  RTE_LOG(INFO, EAL, "Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n",
          (unsigned)port, RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) return retval;

  return 0;
}

inline int DPDKHandler::LookupWorker(rte_be32_t ip_be, uint16_t& worker_out,
                                     uint16_t& db_out) {
  uint32_t ip = rte_be_to_cpu_32(ip_be);

  uint8_t rack = static_cast<uint8_t>((ip >> 8) & 0xFF);
  uint8_t host = static_cast<uint8_t>(ip & 0xFF);

  if (unlikely(rack >= worker_table.size() || host == 0 || host > DB_PER_RACK))
    return -1;

  worker_out = worker_table[rack];
  db_out = rack * DB_PER_RACK + (host - 1);

  if (unlikely(worker_out >= WORKER_CORE_NUM || db_out >= TOTAL_DB_NUM))
    return -1;

  return 0;
}

void DPDKHandler::RxLoop(CoreInfo core_info) {
  const uint16_t port = 0;

  uint lcore_id = core_info.lcore_id;
  uint16_t queue_id = core_info.queue_id;
  auto rx_rings = core_info.rx_rings;

  if (lcore_id == RTE_MAX_LCORE || lcore_id == (unsigned)LCORE_ID_ANY) {
    rte_exit(EXIT_FAILURE, "Invalid lcore_id=%u\n", lcore_id);
  }
  if (rte_lcore_is_enabled(lcore_id) && lcore_id != rte_get_main_lcore()) {
    if (rte_eth_dev_socket_id(port) >= 0 &&
        rte_eth_dev_socket_id(port) != (int)rte_socket_id())
      printf(
          "[WARNING], port %u is on remote NUMA node to "
          "polling thread.\n\tPerformance will "
          "not be optimal.\n",
          port);

    RTE_LOG(NOTICE, RX, "[core %u] polling queue: %hu\n", lcore_id, queue_id);

    rte_mbuf* rx_pkts[BURST_SIZE];
    while (!stop_requested_.load(std::memory_order_relaxed)) {
      uint16_t nb_rx = rte_eth_rx_burst(port, queue_id, rx_pkts, BURST_SIZE);
      if (unlikely(nb_rx == 0)) {
        rte_pause();
        continue;
      } else {
        for (auto i : range(nb_rx)) {
          if (i + 2 < nb_rx)
            rte_prefetch0(rte_pktmbuf_mtod(rx_pkts[i + 2], void*));

          auto* mbuf = rx_pkts[i];

          rte_ether_hdr* eth_hdr = rte_pktmbuf_mtod(mbuf, rte_ether_hdr*);
          rte_ipv4_hdr* ip_hdr = reinterpret_cast<rte_ipv4_hdr*>(eth_hdr + 1);

          if (unlikely(ip_hdr->next_proto_id != IPPROTO_UDP)) {
            rte_pktmbuf_free(rx_pkts[i]);
            continue;
          }

          rte_udp_hdr* udp_hdr = reinterpret_cast<rte_udp_hdr*>(ip_hdr + 1);
          uint16_t dst_port = rte_be_to_cpu_16(udp_hdr->dst_port);

          if (unlikely(dst_port != UDP_PORT_KV)) {
            rte_pktmbuf_free(rx_pkts[i]);
            continue;
          }

          SwapMac(eth_hdr);
          rte_be32_t ip_be = SwapIpv4(ip_hdr);

          uint16_t worker_id, db_id;
          if (LookupWorker(ip_be, worker_id, db_id) != 0) {
            rte_pktmbuf_free(rx_pkts[i]);
            RTE_LOG(WARNING, RX,
                    "[core %u] Not find worker for [%d.%d.%d.%d]\n", lcore_id,
                    DECODE_IP(ip_be));
            continue;
          }

          ipc_req* req;
          if (rte_mempool_get(ipc_mempool_, (void**)&req) == 0) {
            req->db_id = db_id;
            req->src_ip = ip_be;
            req->mbuf = rx_pkts[i];
            if (unlikely(rte_ring_mp_enqueue((*rx_rings)[worker_id], req) <
                         0)) {
              rte_pktmbuf_free(rx_pkts[i]);
              rte_mempool_put(ipc_mempool_, req);
            }
          } else {
            rte_pktmbuf_free(rx_pkts[i]);
          }
        }
      }
    }
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
}

void DPDKHandler::TxLoop(CoreInfo core_info) {
  using namespace c_m_proto;
  const uint16_t port = 0;

  uint lcore_id = core_info.lcore_id;
  uint16_t queue_id = core_info.queue_id;
  auto* tx_ring = core_info.tx_ring;

  RTE_LOG(NOTICE, TX, "[core %u] polling queue: %hu\n", lcore_id, queue_id);

  rte_mbuf* tx_pkts[BURST_SIZE];
  Packet* mig_packets[BURST_SIZE];

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    // 1. 处理缓存迁移 ring
    uint16_t nb_mig = rte_ring_mc_dequeue_burst(
        kv_migration_ring, reinterpret_cast<void**>(mig_packets), BURST_SIZE,
        nullptr);

    if (likely(nb_mig > 0)) {
      uint16_t mig_sent = 0;
      for (auto i : range(nb_mig)) {
        Packet* packet = mig_packets[i];
        if (unlikely(packet->size() > MBUF_DATA_SIZE)) {
          RTE_LOG(WARNING, TX, "[core %u] Migration packet too large: %zu\n",
                  lcore_id, packet->size());
          delete packet;
          continue;
        }

        rte_mbuf* mbuf = rte_pktmbuf_alloc(mbufpool_);
        if (unlikely(mbuf == nullptr)) {
          RTE_LOG(ERR, TX, "[core %u] Failed to allocate mbuf\n", lcore_id);
          delete packet;
          continue;
        }

        // 复制数据到 mbuf
        void* data = rte_pktmbuf_append(mbuf, packet->size());
        rte_memcpy(data, packet->data(), packet->size());

        tx_pkts[mig_sent++] = mbuf;
        delete packet;  // 释放原始 Packet
      }

      if (likely(mig_sent > 0)) {
        uint16_t nb_sent = rte_eth_tx_burst(port, queue_id, tx_pkts, mig_sent);
        if (unlikely(nb_sent < mig_sent)) {
          for (auto i : range(nb_sent, mig_sent)) {
            rte_pktmbuf_free(tx_pkts[i]);
          }
        }
      }
    }

    // 2. 处理正常 TX ring
    uint16_t nb_pkts = rte_ring_sc_dequeue_burst(
        tx_ring, reinterpret_cast<void**>(tx_pkts), BURST_SIZE, nullptr);

    if (likely(nb_pkts > 0)) {
      uint16_t nb_sent = rte_eth_tx_burst(port, queue_id, tx_pkts, nb_pkts);
      if (unlikely(nb_sent < nb_pkts)) {
        RTE_LOG(ERR, TX, "[core %u] Send fail\n", lcore_id);
        for (auto i : range(nb_sent, nb_pkts)) {
          rte_pktmbuf_free(tx_pkts[i]);
        }
      }
    }

    // 3. 如果两个 ring 都没有数据，短暂等待
    if (unlikely(nb_mig == 0 && nb_pkts == 0)) {
      rte_pause();
    }
  }

  RTE_LOG(NOTICE, TX, "[core %u] exiting\n", lcore_id);
}

void DPDKHandler::DBWorker(CoreInfo core_info) {
  using namespace c_m_proto;
  namespace net = boost::asio;

  uint lcore_id = core_info.lcore_id;
  auto* rx_ring = core_info.work_rings.first;
  auto* tx_ring = core_info.work_rings.second;

  const uint32_t start_db = next_db_id.fetch_add(DBS_PER_WORKER);
  uint32_t end_db = start_db + DBS_PER_WORKER - 1;

  if (start_db >= TOTAL_DB_NUM) {
    RTE_LOG(ERR, WORKER, "[core %u] has no DB to handle.\n", lcore_id);
    return;
  }

  if (end_db >= TOTAL_DB_NUM) end_db = TOTAL_DB_NUM - 1;
  const uint32_t db_count = end_db - start_db + 1;

  std::atomic<uint32_t> inflight{0};

  net::io_context ioc;
  auto work_guard = net::make_work_guard(ioc);

  // 创建 16 个 Redis 连接
  std::array<std::shared_ptr<redis::connection>, REDIS_CONNS_PER_WORKER> conns;
  for (uint32_t i = 0; i < REDIS_CONNS_PER_WORKER; i++) {
    redis::config cfg;
    cfg.unix_socket = REDIS_SOCKET_PATH;
    conns[i] =
        std::make_shared<redis::connection>(ioc, redis::logger::level::err);
    conns[i]->async_run(cfg, net::consign(net::detached, conns[i]));
  }

  std::thread io_thread([&ioc, lcore_id] {
    try {
      ioc.run();
    } catch (const std::exception& e) {
      RTE_LOG(ERR, WORKER, "[core %u] IO context exception: %s\n", lcore_id,
              e.what());
    }
  });

  RTE_LOG(NOTICE, WORKER, "[core %u] handles DBs: %u ~ %u (%u connections)\n",
          lcore_id, start_db, end_db, REDIS_CONNS_PER_WORKER);

  // Pipeline 累积结构
  struct PipelineBatch {
    std::vector<rte_mbuf*> mbufs;
    std::vector<std::string> keys;
    std::chrono::steady_clock::time_point first_arrival;
  };

  std::array<PipelineBatch, REDIS_CONNS_PER_WORKER> batches;

  // 刷新 batch 的 lambda
  auto flush_batch = [&](uint32_t conn_idx) {
    auto& batch = batches[conn_idx];
    if (batch.mbufs.empty()) return;

    auto mbufs =
        std::make_shared<std::vector<rte_mbuf*>>(std::move(batch.mbufs));
    auto keys =
        std::make_shared<std::vector<std::string>>(std::move(batch.keys));

    batch.mbufs.clear();
    batch.keys.clear();
    batch.first_arrival = {};

    inflight.fetch_add(mbufs->size(), std::memory_order_relaxed);

    // 使用 push_range 构建 MGET 请求
    auto db_req = std::make_shared<redis::request>();
    db_req->push_range("MGET", keys->begin(), keys->end());

    auto resp = std::make_shared<
        redis::response<std::vector<std::optional<std::string>>>>();

    conns[conn_idx]->async_exec(
        *db_req, *resp,
        [mbufs, keys, resp, tx_ring, &inflight, lcore_id](auto ec, auto) {
          inflight.fetch_sub(mbufs->size(), std::memory_order_relaxed);

          if (ec) {
            RTE_LOG(ERR, DB, "[core %u] Redis MGET error: %s\n", lcore_id,
                    ec.message().c_str());
            for (auto* m : *mbufs) rte_pktmbuf_free(m);
            return;
          }

          auto& results = std::get<0>(*resp).value();

          for (size_t i = 0; i < mbufs->size() && i < results.size(); i++) {
            auto* m = (*mbufs)[i];
            auto& val = results[i];

            if (!val.has_value() || val->empty()) {
              rte_pktmbuf_free(m);
              continue;
            }

            auto* kv_h =
                rte_pktmbuf_mtod_offset(m, KVRequest*, KV_HEADER_OFFSET);
            rte_memcpy(kv_h->value1.data(), val->data(),
                       std::min(val->size(), static_cast<size_t>(VALUE_LENGTH * 4)));

            uint16_t ret = rte_ring_mp_enqueue(tx_ring, m);
            if (ret != 0) rte_pktmbuf_free(m);
          }

          for (size_t i = results.size(); i < mbufs->size(); i++) {
            rte_pktmbuf_free((*mbufs)[i]);
          }
        });
  };

  auto last_log_time = std::chrono::steady_clock::now();

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    struct ipc_req* reqs[BURST_SIZE];
    const uint16_t nb_rx =
        rte_ring_sc_dequeue_burst(rx_ring, (void**)reqs, BURST_SIZE, nullptr);

    auto now = std::chrono::steady_clock::now();

    // 每秒打印 inflight 状态
    if (now - last_log_time >= std::chrono::seconds(1) &&
        inflight.load(std::memory_order_relaxed) > 0) {
      RTE_LOG(NOTICE, WORKER, "[core %u] inflight=%u\n", lcore_id,
              inflight.load(std::memory_order_relaxed));
      last_log_time = now;
    }

    // 检查是否需要刷新 batches
    for (uint32_t i = 0; i < REDIS_CONNS_PER_WORKER; i++) {
      auto& batch = batches[i];
      bool should_flush =
          batch.mbufs.size() >= BURST_SIZE ||
          (batch.mbufs.size() > 0 &&
           now - batch.first_arrival >= PIPELINE_FLUSH_INTERVAL);

      if (should_flush) {
        flush_batch(i);
      }
    }

    if (nb_rx > 0) {
      for (auto i : range(nb_rx)) {
        auto* mbuf = reqs[i]->mbuf;
        const uint32_t db_id = reqs[i]->db_id;
        const auto src_ip = reqs[i]->src_ip;
        rte_mempool_put(ipc_mempool_, reqs[i]);

        if (inflight.load(std::memory_order_relaxed) >=
            MAX_INFLIGHT_PER_WORKER) {
          rte_pktmbuf_free(mbuf);
          continue;
        }

        const uint32_t db_idx = db_id - start_db;
        if (unlikely(db_idx >= db_count)) {
          RTE_LOG(WARNING, DB, "[core %u] Invalid DB ID: %u\n", lcore_id,
                  db_id);
          rte_pktmbuf_free(mbuf);
          continue;
        }

        auto* kv_hdr =
            rte_pktmbuf_mtod_offset(mbuf, KVRequest*, KV_HEADER_OFFSET);
        uint8_t is_req = GET_IS_REQ(kv_hdr->combined);
        std::string_view key(kv_hdr->key.data(), KEY_LENGTH);

        // 映射到连接: db_id % 16
        const uint32_t conn_idx = db_id % REDIS_CONNS_PER_WORKER;

        // 构建 key 前缀: db{db_id}:{key}
        std::string prefixed_key =
            "db" + std::to_string(db_id) + ":" + std::string(key);

        if (GET_OP(kv_hdr->combined) == WRITE_REQUEST) {
          std::string_view value(kv_hdr->value1.data(), VALUE_LENGTH * 4);

          if (is_req == CACHE_MIGRATE) {
            auto* kv_migrate =
                rte_pktmbuf_mtod_offset(mbuf, KVMigrate*, KV_HEADER_OFFSET);
            auto server_instance = GetServerByIp(src_ip);
            if (server_instance) {
              server_instance->CacheMigrate(key, kv_migrate->migration_id);
            }
            rte_pktmbuf_free(mbuf);

            auto db_req = std::make_shared<redis::request>();
            db_req->push("SET", prefixed_key, value);

            inflight.fetch_add(1, std::memory_order_relaxed);
            conns[conn_idx]->async_exec(
                *db_req, redis::ignore,
                [db_req, &inflight, lcore_id](auto ec, auto) {
                  inflight.fetch_sub(1, std::memory_order_relaxed);
                  if (ec) {
                    RTE_LOG(ERR, DB, "[core %u] Redis WRITE error: %s\n",
                            lcore_id, ec.message().c_str());
                  }
                });
            continue;
          }

          // 普通 WRITE：单独发送（不进 pipeline）
          kv_hdr->combined = (kv_hdr->combined & 0x0F) | (SERVER_REPLY << 4);

          auto db_req = std::make_shared<redis::request>();
          db_req->push("SET", prefixed_key, value);

          inflight.fetch_add(1, std::memory_order_relaxed);
          conns[conn_idx]->async_exec(
              *db_req, redis::ignore,
              [mbuf, db_req, tx_ring, &inflight, lcore_id](auto ec, auto) {
                inflight.fetch_sub(1, std::memory_order_relaxed);
                if (ec) {
                  RTE_LOG(ERR, DB, "[core %u] Redis WRITE error: %s\n",
                          lcore_id, ec.message().c_str());
                  rte_pktmbuf_free(mbuf);
                  return;
                }

                uint16_t ret = rte_ring_mp_enqueue(tx_ring, mbuf);
                if (ret != 0) rte_pktmbuf_free(mbuf);
              });

        } else if (GET_OP(kv_hdr->combined) == READ_REQUEST) {
          // READ：累积到 batch，等 MGET 批量发送
          kv_hdr->combined = (kv_hdr->combined & 0x0F) | (SERVER_REPLY << 4);

          auto& batch = batches[conn_idx];
          if (batch.mbufs.empty()) {
            batch.first_arrival = now;
          }

          batch.mbufs.push_back(mbuf);
          batch.keys.push_back(prefixed_key);

          // 达到批量大小，立即刷新
          if (batch.mbufs.size() >= BURST_SIZE) {
            flush_batch(conn_idx);
          }
        }
      }
    }

    if (nb_rx == 0) {
      rte_pause();
    }
  }

  // 退出前刷新所有 pending batches
  for (uint32_t i = 0; i < REDIS_CONNS_PER_WORKER; i++) {
    if (!batches[i].mbufs.empty()) {
      flush_batch(i);
    }
  }

  work_guard.reset();
  ioc.stop();
  if (io_thread.joinable()) io_thread.join();
}

inline void DPDKHandler::CreatRings() {
  for (auto i : range(RX_CORE_NUM)) {
    char ring_name[32];
    snprintf(ring_name, sizeof(ring_name), "rx_ring_%d", i);
    rx_rings_[i] = rte_ring_create(ring_name, RING_SIZE, rte_socket_id(),
                                   RING_F_MP_RTS_ENQ | RING_F_SC_DEQ);
    if (rx_rings_[i] == nullptr) {
      rte_exit(EXIT_FAILURE, "Failed to create ring %s\n", ring_name);
    }
  }

  for (auto i : range(TX_CORE_NUM)) {
    char ring_name[32];
    snprintf(ring_name, sizeof(ring_name), "tx_ring_%d", i);
    tx_rings_[i] = rte_ring_create(ring_name, RING_SIZE, rte_socket_id(),
                                   RING_F_MP_RTS_ENQ | RING_F_SC_DEQ);
    if (tx_rings_[i] == nullptr) {
      rte_exit(EXIT_FAILURE, "Failed to create ring %s\n", ring_name);
    }
  }
}

inline void DPDKHandler::InitAndLaunchCores() {
  all_cores_.reserve(TOTAL_CORE_NUM);

  uint core_counter = 0;
  uint lcore_id;

  printf("lcore_count = %u\n", rte_lcore_count());

  RTE_LCORE_FOREACH_WORKER(lcore_id) {
    if (core_counter < RX_CORE_NUM) {
      all_cores_.emplace_back(lcore_id, CoreType::RX_CORE, &rx_rings_);
    } else if (core_counter < RX_CORE_NUM + WORKER_CORE_NUM) {
      uint q_idx = core_counter - RX_CORE_NUM;
      uint tx_idx = (TX_CORE_NUM > 0) ? (q_idx % TX_CORE_NUM) : 0;

      std::pair<rte_ring*, rte_ring*> pair = {rx_rings_[q_idx],
                                              tx_rings_[tx_idx]};
      all_cores_.emplace_back(lcore_id, CoreType::WORKER_CORE, nullptr, pair);
    } else {
      uint q_idx = core_counter - (RX_CORE_NUM + WORKER_CORE_NUM);
      if (q_idx >= TX_CORE_NUM) break;

      all_cores_.emplace_back(lcore_id, CoreType::TX_CORE, nullptr,
                              std::make_pair(nullptr, nullptr),
                              tx_rings_[q_idx]);
    }
    core_counter++;
    if (core_counter >= TOTAL_CORE_NUM) break;
  }
  printf("worker_count = %u\n", core_counter);
}

inline void DPDKHandler::CreatPool() {
  uint nb_ports = rte_eth_dev_count_avail();

  mbufpool_ =
      rte_pktmbuf_pool_create("MBUF_POOL", MBUF_NUM * nb_ports, MBUF_CACHE_SIZE,
                              0, MBUF_DATA_SIZE, rte_socket_id());
  if (mbufpool_ == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  ipc_mempool_ = rte_mempool_create("IPC_REQ_POOL", 8191, sizeof(ipc_req), 0, 0,
                                    NULL, NULL, NULL, NULL, rte_socket_id(), 0);
  if (ipc_mempool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create ipc mbuf pool\n");
}

inline int DPDKHandler::LaunchRxLcore(void* arg) {
  CoreArgs* args = static_cast<CoreArgs*>(arg);
  args->instance->RxLoop(*args->core_info);
  return 0;
}

inline int DPDKHandler::LaunchTxLcore(void* arg) {
  CoreArgs* args = static_cast<CoreArgs*>(arg);
  (void)args;
  args->instance->TxLoop(*args->core_info);
  return 0;
}

inline int DPDKHandler::LaunchWorkerLcore(void* arg) {
  CoreArgs* args = static_cast<CoreArgs*>(arg);
  (void)args;
  args->instance->DBWorker(*args->core_info);
  return 0;
}

inline void DPDKHandler::LaunchThreads() {
  core_args_.reserve(TOTAL_CORE_NUM);

  for (const auto& lcore : all_cores_) {
    auto args = std::make_unique<CoreArgs>();
    args->instance = this;
    args->core_info = &lcore;
    auto type = args->core_info->type;

    core_args_.push_back(std::move(args));

    int ret = -1;
    const char* core_name = nullptr;

    switch (type) {
      case CoreType::RX_CORE: {
        ret = rte_eal_remote_launch(LaunchRxLcore, core_args_.back().get(),
                                    lcore.lcore_id);
        core_name = "rx";
        break;
      }

      case CoreType::WORKER_CORE: {
        ret = rte_eal_remote_launch(LaunchWorkerLcore, core_args_.back().get(),
                                    lcore.lcore_id);
        core_name = "worker";
        break;
      }

      case CoreType::TX_CORE: {
        ret = rte_eal_remote_launch(LaunchTxLcore, core_args_.back().get(),
                                    lcore.lcore_id);
        core_name = "tx";
        break;
      }

      default:
        std::cerr << "Unknown core type on lcore " << lcore.lcore_id
                  << std::endl;
        continue;
    }
    if (ret < 0) {
      std::cerr << "Failed to launch " << core_name << " thread on core "
                << lcore.lcore_id << ", error: " << rte_strerror(-ret)
                << std::endl;
      return;
    }
  }
  std::cout << "Successfully launched all RX/TX and Worker threads."
            << std::endl;
}

void DPDKHandler::Start() {
  uint16_t port = 0;

  for (auto rack : range(NUM_RACKS)) {
    worker_table[rack] = rack / RACK_PER_WORKER;
  }

  CreatRings();

  InitAndLaunchCores();

  CreatPool();

  // set mac address
  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &s_eth_addr_);

  // port init
  if (PortInit() != 0)
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);

  // start threads
  LaunchThreads();

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    monitor_mempool(mbufpool_);
    monitor_mempool(ipc_mempool_);

    // 监控 ring 使用情况
    std::cout << "[RX_QUEUE]: ";
    for (auto i : range(RX_CORE_NUM))
      std::cout << rte_eth_rx_queue_count(port, i) << " | ";
    std::cout << std::endl;

    std::cout << "[RX_RING]: ";
    for (auto i : range(RX_CORE_NUM))
      std::cout << rte_ring_count(rx_rings_[i]) << " | ";
    std::cout << std::endl;

    std::cout << "[TX_RING]: ";
    for (auto i : range(TX_CORE_NUM))
      std::cout << rte_ring_count(tx_rings_[i]) << " | ";
    std::cout << std::endl;

    // 分段 sleep，每 100ms 检查一次停止请求
    for (int i = 0; i < 20 && !stop_requested_.load(std::memory_order_relaxed); i++) {
      rte_delay_us_sleep(100'000);
    }
  }

  rte_eal_mp_wait_lcore();
}

void DPDKHandler::Stop() {
  if (initialized_ && !stop_requested_.load(std::memory_order_relaxed)) {
    std::cout << "[DPDK] Stopping...\n";
    stop_requested_.store(true, std::memory_order_relaxed);

    // Wait for all lcores to finish
    rte_eal_mp_wait_lcore();

    std::cout << "[DPDK] All lcores stopped\n";

    uint16_t port = 0;
    rte_eth_dev_stop(port);
    rte_eth_dev_close(port);

    rte_ring_free(kv_migration_ring);

    rte_eal_cleanup();
    std::cout << "[DPDK] Cleanup completed\n";
  }
}
