#include "dpdk_handler.h"

#include <rte_byteorder.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_mempool.h>
#include <rte_per_lcore.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/redis/connection.hpp>
#include <boost/redis/src.hpp>
#include <cassert>
#include <fstream>
#include <iostream>

#include "lib/sync_connection.hpp"
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

std::atomic<uint64_t> total_latency_us{0};
std::atomic<size_t> completed_request_count{0};

std::ofstream outfile("server.output");

inline void redis_test(redis::sync_connection& conn) {
  try {
    redis::request req;
    redis::response<std::string> resp;

    req.push("PING");

    conn.exec(req, resp);

    auto reply = std::get<0>(resp).value();
    if (reply != "PONG") {
      std::cerr << "[redis_test] Unexpected ping reply: " << reply << std::endl;
      std::exit(1);
    }
  } catch (const std::exception& e) {
    std::cerr << "[redis_test] Redis exception: " << e.what() << std::endl;
    std::exit(1);
  } catch (...) {
    std::cerr << "[redis_test] Unknown exception occurred." << std::endl;
    std::exit(1);
  }
}

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
                      RING_F_SC_DEQ | RING_F_MP_RTS_ENQ);
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
  tmp_rxmode.mtu = TX_MBUF_DATA_SIZE;

  rte_eth_rss_conf rss_conf;
  rss_conf.rss_hf = RTE_ETH_RSS_IP;

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
                                      rte_eth_dev_socket_id(port), NULL,
                                      rx_mbufpool_);
      if (retval < 0) return retval;
      core.queue_id = rx_count;
      rx_count++;
    }
    if (rx_count != nb_rx_cores) {
      RTE_LOG(ERR, USER1, "Mismatch: Expected %u RX cores, found %u\n",
              nb_rx_cores, rx_count);
      return -1;
    }
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
    RTE_LOG(ERR, USER1, "Mismatch: Expected %u TX cores, found %u\n",
            nb_tx_cores, tx_count);
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

inline void InitLookupTables() {
  for (auto rack = 0; rack < NUM_RACKS; ++rack) {
    worker_table[rack] = rack / RACK_PER_WORKER;
  }
}

inline int LookupWorker(rte_be32_t ip_be, uint8_t& worker_out,
                        uint8_t& db_out) {
  uint32_t ip = rte_be_to_cpu_32(ip_be);

  uint8_t rack = static_cast<uint8_t>((ip >> 8) & 0xFF);  // subnet
  uint8_t host = static_cast<uint8_t>(ip & 0xFF);         // host

  if (unlikely(rack >= worker_table.size() || host == 0 || host > DB_PER_RACK))
    return -1;

  worker_out = worker_table[rack];
  db_out = rack * DB_PER_RACK + (host - 1);

  if (unlikely(worker_out == 0xFF || db_out >= TOTAL_DB_NUM)) return -1;

  return 0;
}

void DPDKHandler::RxLoop(CoreInfo core_info) {
  const uint16_t port = 0;

  thread_local uint lcore_id = core_info.lcore_id;
  thread_local uint16_t queue_id = core_info.queue_id;
  thread_local auto rx_rings = core_info.rx_rings;

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

    RTE_LOG(NOTICE, CORE, "[Rx Core %u] polling queue: %hu\n", lcore_id,
            queue_id);

    rte_mbuf* rx_pkts[BURST_SIZE];
    while (true) {
      uint16_t nb_rx = rte_eth_rx_burst(port, queue_id, rx_pkts, BURST_SIZE);
      if (unlikely(nb_rx <= 0)) {
        rte_pause();
        continue;
      } else {
        for (uint16_t i = 0; i < nb_rx; i++) {
          if (i + 1 < nb_rx)
            rte_prefetch0(rte_pktmbuf_mtod(rx_pkts[i + 1], void*));

          rte_ether_hdr* eth_hdr = rte_pktmbuf_mtod(rx_pkts[i], rte_ether_hdr*);
          rte_ipv4_hdr* ip_hdr = reinterpret_cast<rte_ipv4_hdr*>(eth_hdr + 1);
          if (unlikely(ip_hdr->next_proto_id != IPPROTO_UDP)) {
            rte_pktmbuf_free(rx_pkts[i]);
            continue;
          }
          rte_udp_hdr* udp_hdr = reinterpret_cast<rte_udp_hdr*>(ip_hdr + 1);

          // uint16_t src_port = rte_be_to_cpu_16(udp_hdr->src_port);
          uint16_t dst_port = rte_be_to_cpu_16(udp_hdr->dst_port);

          if (unlikely(dst_port != UDP_PORT_KV)) {
            rte_pktmbuf_free(rx_pkts[i]);
            continue;
          }

          SwapMac(eth_hdr);
          rte_be32_t ip_be = SwapIpv4(ip_hdr);

          uint8_t worker_id, db_id;
          if (LookupWorker(ip_be, worker_id, db_id) == 0) {
            rte_pktmbuf_free(rx_pkts[i]);
            RTE_LOG(WARNING, DB,
                    "Not find worker for [%d.%d.%d.%d] in core: %u\n",
                    DECODE_IP(ip_be), lcore_id);
            continue;
          }

          ipc_req* req;
          if (rte_mempool_get(ipc_mempool_, (void**)&req) == 0) {
            req->db_id = db_id;
            req->mbuf = rx_pkts[i];
            if (unlikely(rte_ring_enqueue((*rx_rings)[worker_id], req) < 0)) {
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
  const uint16_t port = 0;

  thread_local uint lcore_id = core_info.lcore_id;
  thread_local uint16_t queue_id = core_info.queue_id;
  thread_local auto* tx_ring = core_info.tx_ring;

  RTE_LOG(NOTICE, CORE, "[TX Core %u] polling queue: %hu\n", lcore_id,
          queue_id);

  rte_mbuf* tx_pkts[BURST_SIZE];

  while (true) {
    uint16_t nb_pkts = rte_ring_dequeue_burst(
        tx_ring, reinterpret_cast<void**>(tx_pkts), BURST_SIZE, nullptr);

    if (unlikely(nb_pkts <= 0)) {
      rte_pause();
      continue;
    } else {
      uint16_t nb_sent = rte_eth_tx_burst(port, queue_id, tx_pkts, nb_pkts);

      if (unlikely(nb_sent < nb_pkts)) {
        for (uint16_t i = nb_sent; i < nb_pkts; i++) {
          rte_pktmbuf_free(tx_pkts[i]);
        }
      }
    }

    RTE_LOG(NOTICE, CORE, "[TX Core %u] exiting\n", lcore_id);
    return;
    // std::vector<uint8_t> *packet_data = nullptr;
    // if (rte_ring_dequeue(tx_ring, (void **)&packet_data) == 0) {
    //   if (packet_data) {
    //     rte_mbuf *mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
    //     if (!mbuf) {
    //       std::cerr << "Failed to allocate mbuf" << std::endl;
    //       delete packet_data;
    //       continue;
    //     }
    //     char *mbuf_data = rte_pktmbuf_mtod(mbuf, char *);
    //     rte_memcpy(mbuf_data, packet_data->data(), packet_data->size());

    //     rte_pktmbuf_data_len(mbuf) = packet_data->size();
    //     rte_pktmbuf_pkt_len(mbuf) = rte_pktmbuf_data_len(mbuf);
    //     delete packet_data;
    //     int ret = rte_eth_tx_burst(0 /*port id*/, queue_id, &mbuf, 1);
    //     if (ret < 1) {
    //       RTE_LOG(ERR, CORE, "Send error: %s (errno=%d)\n",
    //       rte_strerror(-ret),
    //               -ret);
    //       rte_pktmbuf_free(mbuf);
    //       continue;
    //     }
    //   }
    // } else {
    //   rte_delay_us_sleep(10);
    // }
  }
}

void DPDKHandler::DBWorker(CoreInfo core_info) {
  using namespace c_m_proto;
  namespace net = boost::asio;

  uint lcore_id = core_info.lcore_id;
  auto* rx_ring = core_info.work_rings.first;
  auto* tx_ring = core_info.work_rings.second;

  uint32_t start_db = next_db_id.fetch_add(DBS_PER_WORKER);
  uint32_t end_db = start_db + DBS_PER_WORKER - 1;

  if (start_db >= TOTAL_DB_NUM) {
    RTE_LOG(ERR, WORKER, "[Worker core %u] has no DB to handle.\n", lcore_id);
    return;
  }

  if (end_db >= TOTAL_DB_NUM) end_db = TOTAL_DB_NUM - 1;

  RTE_LOG(NOTICE, WORKER, "[Worker core %u] handles DBs: %u ~ %u\n", lcore_id,
          start_db, end_db);

  auto ioc = std::make_shared<net::io_context>();

  std::vector<std::shared_ptr<redis::connection>> conns(DBS_PER_WORKER);
  std::vector<DBPipeline> pipelines(DBS_PER_WORKER);

  for (auto i = 0; i <= DBS_PER_WORKER; ++i) {
    redis::config cfg;
    cfg.addr.host = "127.0.0.1";
    cfg.addr.port = std::to_string(i + start_db);

    auto conn = std::make_shared<redis::connection>(ioc->get_executor());
    conn->async_run(cfg, {}, net::consign(net::detached, conn));
    conns[i] = conn;
  }

  std::thread io_thread([ioc] { ioc->run(); });

  ipc_req* req = nullptr;
  while (true) {
    if (rte_ring_dequeue(rx_ring, (void**)&req) == 0) {
      std::size_t index = req->db_id - start_db;

      auto conn = conns[index];
      auto pipeline_ptr = &pipelines[index];

      net::post(*ioc, [conn, pipeline_ptr, tx_ring, req_copy = *req]() {
        auto& pipeline = *pipeline_ptr;

        rte_pktmbuf_refcnt_update(req_copy.mbuf, 1);

        KVRequest* kv_header = rte_pktmbuf_mtod_offset(
            req_copy.mbuf, KVRequest*, KV_HEADER_OFFSET);
        kv_header->combined |= 0x1000;  // SERVER_REPLY << 12

        std::string_view key{kv_header->key.data(), KEY_LENGTH};

        if (GET_OP(kv_header->combined) == WRITE_REQUEST) {
          std::string_view value{kv_header->value1.data(), VALUE_LENGTH * 4};
          pipeline.req.push("SET", key, value);
        } else {
          pipeline.req.push("GET", key);
        }

        pipeline.mbufs.push_back(req_copy.mbuf);

        if (pipeline.req.get_commands() >= BURST_SIZE) {
          auto pipe_reqs = std::move(pipeline.req);
          auto mbufs = std::move(pipeline.mbufs);

          pipeline.req = redis::request{};
          pipeline.mbufs = {};

          auto resps = std::make_shared<redis::generic_response>();

          conn->async_exec(
              pipe_reqs, *resps,
              [resps, mbufs = std::move(mbufs), tx_ring](auto ec,
                                                         auto) mutable {
                if (ec) {
                  for (auto m : mbufs) rte_pktmbuf_free(m);
                  return;
                }
                auto& vec = resps->value();
                for (size_t i = 0; i < vec.size() && i < mbufs.size(); ++i) {
                  KVRequest* kv_h = rte_pktmbuf_mtod_offset(
                      mbufs[i], KVRequest*, KV_HEADER_OFFSET);
                  if (GET_OP(kv_h->combined) == READ_REQUEST) {
                    const std::string& val = vec[i].value;

                    if (!val.empty()) {
                      rte_memcpy(kv_h->value1.data(), val.data(),
                                 VALUE_LENGTH * 4);
                    }
                  }
                }
                int ret = rte_ring_enqueue_bulk(
                    *tx_ring, reinterpret_cast<void* const*>(mbufs.data()),
                    mbufs.size(), nullptr);
                if (ret < 0) {
                  for (auto m : mbufs) rte_pktmbuf_free(m);
                }
              });
        }
      });
    } else {
      rte_pause();
    }
  }
  io_thread.join();
}

inline void DPDKHandler::CreatRings() {
  for (int i = 0; i < RX_CORE_NUM; i++) {
    char ring_name[32];
    snprintf(ring_name, sizeof(ring_name), "rx_ring_%d", i);
    rx_rings_[i] = rte_ring_create(ring_name, RING_SIZE, rte_socket_id(),
                                   RING_F_MP_RTS_ENQ | RING_F_SC_DEQ);
    if (rx_rings_[i] == nullptr) {
      rte_exit(EXIT_FAILURE, "Failed to create ring %s\n", ring_name);
    }
  }

  for (int i = 0; i < TX_CORE_NUM; i++) {
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

  RTE_LCORE_FOREACH_WORKER(lcore_id) {
    if (core_counter < RX_CORE_NUM) {
      all_cores_.emplace_back(lcore_id, CoreType::RX_CORE, &rx_rings_);
    } else if (core_counter < RX_CORE_NUM + WORKER_CORE_NUM) {
      uint q_idx = core_counter - RX_CORE_NUM;
      uint tx_idx = (TX_CORE_NUM > 0) ? (q_idx % TX_CORE_NUM) : 0;
      if (q_idx >= RX_CORE_NUM) break;

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
        int ret = rte_eal_remote_launch(LaunchRxLcore, core_args_.back().get(),
                                        lcore.lcore_id);
        core_name = "rx";
        break;
      }

      case CoreType::WORKER_CORE: {
        int ret = rte_eal_remote_launch(
            LaunchWorkerLcore, core_args_.back().get(), lcore.lcore_id);
        core_name = "worker";
        break;
      }

      case CoreType::TX_CORE: {
        int ret = rte_eal_remote_launch(LaunchTxLcore, core_args_.back().get(),
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

  InitLookupTables();

  InitAndLaunchCores();

  CreatPool();

  // set mac address
  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &s_eth_addr_);

  // port init
  if (PortInit() != 0)
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);

  // start threads
  LaunchThreads();

  while (true) {
    utils::monitor_mempool(rx_mbufpool_);
    utils::monitor_mempool(tx_mbufpool_);

    for (size_t i = 0; i < RX_CORE_NUM; ++i)
      std::cout << rte_eth_rx_queue_count(port, i) << " | ";
    std::cout << std::endl;

    rte_delay_us_sleep(1000'000);
  }

  rte_eal_mp_wait_lcore();
}

void DPDKHandler::Stop() {
  if (initialized_) {
    std::cout << "[DPDK] Cleanup triggered\n";
    initialized_ = false;

    uint16_t port = 0;
    rte_eth_dev_stop(port);
    rte_eth_dev_close(port);

    rte_ring_free(kv_migration_ring);

    rte_eal_cleanup();
    std::cout << "Bye..." << std::endl;
  }
}
