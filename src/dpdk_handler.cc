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

std::ofstream outfile("server.output");

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
                                      rx_mbufpool_);
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
        if (unlikely(packet->size() > TX_MBUF_DATA_SIZE)) {
          RTE_LOG(WARNING, TX, "[core %u] Migration packet too large: %zu\n",
                  lcore_id, packet->size());
          delete packet;
          continue;
        }

        rte_mbuf* mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
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

  net::io_context ioc;

  std::vector<std::shared_ptr<redis::connection>> conns;
  conns.reserve(db_count);

  for (auto i : range(db_count)) {
    redis::config cfg;
    std::string db_socket = std::to_string(i + start_db);

    cfg.unix_socket = "/tmp/redis." + db_socket;

    auto conn =
        std::make_shared<redis::connection>(ioc, redis::logger::level::err);

    conn->async_run(cfg, net::consign(net::detached, conn));
    conns.push_back(conn);
  }

  std::thread io_thread([&ioc, lcore_id] {
    try {
      ioc.run();
    } catch (const std::exception& e) {
      RTE_LOG(ERR, WORKER, "[core %u] IO context exception: %s\n", lcore_id,
              e.what());
    }
  });

  RTE_LOG(NOTICE, WORKER, "[core %u] handles DBs: %u ~ %u\n", lcore_id,
          start_db, end_db);

  struct DBPipeline {
    redis::request read_req;
    redis::request write_req;

    std::vector<rte_mbuf*> read_mbufs;
    std::vector<rte_mbuf*> write_mbufs;

    std::chrono::steady_clock::time_point last_read_flush;
    std::chrono::steady_clock::time_point last_write_flush;
  };

  const uint32_t MAX_INFLIGHT_WINDOW = 128;

  std::vector<std::atomic<uint32_t>> db_inflight(db_count);
  for (auto& x : db_inflight) x.store(0);

  // 记录当前有数据积压的 DB 索引
  std::vector<uint32_t> active_dbs;
  active_dbs.reserve(db_count);
  std::vector<bool> is_db_active(db_count, false);

  std::vector<DBPipeline> pipelines(db_count);
  auto base_time = std::chrono::steady_clock::now();

  // 为每个 DB 创建 TokenBucket 限速器
  std::vector<std::unique_ptr<TokenBucket>> rate_limiters;
  rate_limiters.reserve(db_count);
  for (auto i : range(db_count)) {
    rate_limiters.emplace_back(std::make_unique<TokenBucket>());
    auto& p = pipelines[i];
    p.read_mbufs.reserve(BURST_SIZE);
    p.write_mbufs.reserve(BURST_SIZE);
    auto stagger = std::chrono::microseconds(i * 10);
    p.last_write_flush = base_time + stagger;
    p.last_read_flush = base_time + stagger;
  }

  // Lambda to flush write pipeline
  auto flush_write_pipeline = [&](uint32_t db_idx, DBPipeline& pipeline) {
    uint32_t batch_size = pipeline.write_req.get_commands();
    if (batch_size == 0) return;

    auto conn = conns[db_idx];
    auto req_copy = std::move(pipeline.write_req);
    auto mbufs_copy = std::move(pipeline.write_mbufs);

    pipeline.write_req = redis::request{};
    pipeline.write_mbufs.clear();
    pipeline.last_write_flush = std::chrono::steady_clock::now();

    db_inflight[db_idx].fetch_add(batch_size, std::memory_order_relaxed);

    conn->async_exec(
        req_copy, redis::ignore,
        [mbufs = std::move(mbufs_copy), tx_ring,
         &inflight = db_inflight[db_idx], lcore_id](auto ec, auto) {
          inflight.fetch_sub(mbufs.size(), std::memory_order_relaxed);

          if (ec) {
            RTE_LOG(ERR, DB, "[core %u] Redis WRITE error: %s\n", lcore_id,
                    ec.message().c_str());
            for (auto* m : mbufs) rte_pktmbuf_free(m);
            return;
          }

          RTE_LOG(ERR, DB, "[core %u] Redis WRITE success\n", lcore_id);

          uint16_t ret = rte_ring_mp_enqueue_bulk(
              tx_ring, reinterpret_cast<void* const*>(mbufs.data()),
              mbufs.size(), nullptr);
          if (ret < mbufs.size()) {
            for (auto i : range(ret, mbufs.size())) {
              rte_pktmbuf_free(mbufs[i]);
            }
          }
        });
  };

  // Lambda to flush read pipeline
  auto flush_read_pipeline = [&](uint32_t db_idx, DBPipeline& pipeline) {
    uint32_t batch_size = pipeline.read_mbufs.size();
    if (batch_size == 0) return;

    auto conn = conns[db_idx];
    auto req_copy = std::move(pipeline.read_req);
    auto mbufs_copy = std::move(pipeline.read_mbufs);

    pipeline.read_req = redis::request{};
    pipeline.read_mbufs.clear();
    pipeline.last_read_flush = std::chrono::steady_clock::now();

    db_inflight[db_idx].fetch_add(batch_size, std::memory_order_relaxed);

    auto resps = std::make_shared<redis::response<std::vector<std::string>>>();

    conn->async_exec(
        std::move(req_copy), *resps,
        [resps, tx_ring, mbufs = std::move(mbufs_copy),
         &inflight = db_inflight[db_idx], lcore_id](auto ec, auto) mutable {
          inflight.fetch_sub(mbufs.size(), std::memory_order_relaxed);

          if (ec) {
            RTE_LOG(ERR, DB, "[core %u] Redis READ error: %s\n", lcore_id,
                    ec.message().c_str());
            for (auto* m : mbufs) rte_pktmbuf_free(m);
            return;
          }

          RTE_LOG(ERR, DB, "[core %u] Redis READ success\n", lcore_id);

          auto& vec = std::get<0>(*resps).value();

          for (size_t i = 0; i < vec.size() && i < mbufs.size(); ++i) {
            auto* kv_h =
                rte_pktmbuf_mtod_offset(mbufs[i], KVRequest*, KV_HEADER_OFFSET);

            const std::string& val = vec[i];
            if (!val.empty()) {
              rte_memcpy(
                  kv_h->value1.data(), val.data(),
                  std::min(val.size(), static_cast<size_t>(VALUE_LENGTH * 4)));
            }
          }

          uint16_t ret = rte_ring_mp_enqueue_bulk(
              tx_ring, reinterpret_cast<void* const*>(mbufs.data()),
              mbufs.size(), nullptr);
          if (ret < mbufs.size()) {
            for (auto i : range(ret, mbufs.size())) rte_pktmbuf_free(mbufs[i]);
          }
        });
  };

  while (!stop_requested_.load(std::memory_order_relaxed)) {
    struct ipc_req* reqs[BURST_SIZE];
    const uint16_t nb_rx =
        rte_ring_sc_dequeue_bulk(rx_ring, (void**)reqs, BURST_SIZE, nullptr);

    auto current_time = std::chrono::steady_clock::now();

    if (nb_rx > 0) {
      for (auto i : range(nb_rx)) {
        auto& req = reqs[i];

        const uint32_t db_idx = req->db_id - start_db;
        if (unlikely(db_idx >= db_count)) {
          RTE_LOG(WARNING, DB, "[core %u] Invalid DB ID: %u\n", lcore_id,
                  req->db_id);
          rte_pktmbuf_free(req->mbuf);
          continue;
        }

        if (unlikely(db_inflight[db_idx].load(std::memory_order_relaxed) >=
                     MAX_INFLIGHT_WINDOW)) {
          // 该 DB 已过载，执行丢包，保护 Worker 和其他 DB
          rte_pktmbuf_free(req->mbuf);
          rte_mempool_put(ipc_mempool_, req);
          continue;
        }

        // // 限速检查：超过 1000 QPS 直接丢弃请求
        // if (!rate_limiters[db_idx]->TryConsume()) {
        //   rte_pktmbuf_free(req->mbuf);
        //   rte_mempool_put(ipc_mempool_, req);
        //   continue;
        // }

        auto& pipeline = pipelines[db_idx];
        if (!is_db_active[db_idx]) {
          is_db_active[db_idx] = true;
          active_dbs.push_back(db_idx);
        }

        auto* kv_hdr =
            rte_pktmbuf_mtod_offset(req->mbuf, KVRequest*, KV_HEADER_OFFSET);
        uint8_t is_req = GET_IS_REQ(kv_hdr->combined);
        std::string_view key(kv_hdr->key.data(), KEY_LENGTH);

        if (GET_OP(kv_hdr->combined) == WRITE_REQUEST) {
          std::string_view value(kv_hdr->value1.data(), VALUE_LENGTH * 4);
          pipeline.write_req.push("SET", key, value);

          if (is_req == CACHE_MIGRATE) {
            auto* kv_migrate = rte_pktmbuf_mtod_offset(req->mbuf, KVMigrate*,
                                                       KV_HEADER_OFFSET);
            auto server_instance = GetServerByIp(req->src_ip);
            if (server_instance) {
              server_instance->CacheMigrate(key, kv_migrate->migration_id);
            }
            rte_pktmbuf_free(req->mbuf);

            flush_write_pipeline(db_idx, pipeline);
            continue;
          }

          kv_hdr->combined = (kv_hdr->combined & 0x0F) | (SERVER_REPLY << 4);
          pipeline.write_mbufs.push_back(req->mbuf);

          if (pipeline.write_req.get_commands() >= BURST_SIZE) {
            flush_write_pipeline(db_idx, pipeline);
          }
        } else {
          kv_hdr->combined = (kv_hdr->combined & 0x0F) | (SERVER_REPLY << 4);
          pipeline.read_req.push("GET", key);
          pipeline.read_mbufs.push_back(req->mbuf);

          if (pipeline.read_mbufs.size() >= BURST_SIZE) {
            flush_read_pipeline(db_idx, pipeline);
          }
        }
      }
    }  // end of for (auto i : range(nb_rx))

    static auto last_sched = std::chrono::steady_clock::now();

    static auto sched_interval = std::chrono::microseconds(50);
    static uint32_t consecutive_busy = 0;

    if (nb_rx > BURST_SIZE * 0.8) {
      consecutive_busy++;
      if (consecutive_busy > 10) {
        sched_interval =
            std::max(sched_interval / 2, std::chrono::microseconds(10));
        consecutive_busy = 0;
      }
    } else {
      sched_interval =
          std::min(sched_interval * 2, std::chrono::microseconds(500));
    }

    if (unlikely((current_time - last_sched) >= sched_interval)) {
      last_sched = current_time;

      std::vector<uint32_t> next_active_dbs;
      next_active_dbs.reserve(active_dbs.size());

      for (uint32_t db_idx : active_dbs) {
        auto& pipeline = pipelines[db_idx];

        if (db_inflight[db_idx].load(std::memory_order_relaxed) >=
            MAX_INFLIGHT_WINDOW) {
          next_active_dbs.push_back(db_idx);
          continue;
        }

        bool still_active = false;

        if (pipeline.write_req.get_commands() > 0) {
          if ((current_time - pipeline.last_write_flush) >=
              std::chrono::milliseconds(1)) {
            flush_write_pipeline(db_idx, pipeline);
          } else {
            still_active = true;
          }
        }

        if (!pipeline.read_mbufs.empty()) {
          if ((current_time - pipeline.last_read_flush) >=
              std::chrono::milliseconds(1)) {
            flush_read_pipeline(db_idx, pipeline);
          } else {
            still_active = true;
          }
        }

        if (still_active) {
          next_active_dbs.push_back(db_idx);
        } else {
          is_db_active[db_idx] = false;
        }
      }

      active_dbs = std::move(next_active_dbs);
    }

    if (nb_rx == 0) {
      rte_pause();
    }
  }
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

  tx_mbufpool_ = rte_pktmbuf_pool_create(
      "TX_MBUF_POOL", TX_NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      TX_MBUF_DATA_SIZE, rte_socket_id());
  if (tx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create tx mbuf pool\n");

  rx_mbufpool_ = rte_pktmbuf_pool_create(
      "RX_MBUF_POOL", RX_NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      RX_MBUF_DATA_SIZE, rte_socket_id());
  if (rx_mbufpool_ == NULL)
    rte_exit(EXIT_FAILURE, "Cannot create rx mbuf pool\n");

  ipc_mempool_ = rte_mempool_create("ipc_req_pool", 8191, sizeof(ipc_req), 0, 0,
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
    monitor_mempool(rx_mbufpool_);
    monitor_mempool(tx_mbufpool_);

    for (auto i : range(RX_CORE_NUM))
      std::cout << rte_eth_rx_queue_count(port, i) << " | ";
    std::cout << std::endl;

    rte_delay_us_sleep(1000'000);
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
