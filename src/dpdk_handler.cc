#include "dpdk_handler.h"

#include <rte_byteorder.h>
#include <rte_debug.h>
#include <rte_ethdev.h>
#include <rte_lcore.h>
#include <rte_log.h>
#include <rte_per_lcore.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "lib/utils.h"

std::atomic<uint64_t> total_latency_us{0};
std::atomic<size_t> completed_request_count{0};

std::ofstream outfile("server.output");

#include <rte_mempool.h>

inline void redis_test(std::shared_ptr<sw::redis::Redis> redis) {
  try {
    std::string reply = redis->ping();
    if (reply != "PONG") {
      std::cerr << "[redis_test] Unexpected ping reply: " << reply << std::endl;
      std::exit(1);
    }
  } catch (const sw::redis::ReplyError &e) {
    std::cerr << "[redis_test] Redis ReplyError: " << e.what() << std::endl;
    std::exit(1);
  } catch (const std::exception &e) {
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
    const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>
        &servers) {
  uint db_size = 0;
  for (const auto &server : servers) {
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

inline void DPDKHandler::BuildIptoDBMap(
    std::unordered_map<rte_be32_t, std::shared_ptr<sw::redis::Redis>>
        &ip_to_db) {
  uint db_size = 0;
  for (const auto &server : ip_to_server_) {
    if (!server.second) {
      RTE_LOG(ERR, DB, "Invalid ServerInstance pointer!\n");
      continue;
    }
    int db_index = server.second->GetDb();
    auto redis = std::make_shared<sw::redis::Redis>(
        "tcp://127.0.0.1:6379/" + std::to_string(db_index) +
        "?keep_alive=true&socket_timeout=50ms&connect_timeout=1s");

    redis_test(redis);
    ip_to_db.emplace(server.first, redis);
    db_size++;
  }
}

bool DPDKHandler::Initialize(
    const std::string &conf, char *program_name,
    const std::unordered_map<rte_be32_t, std::shared_ptr<ServerInstance>>
        &servers) {
  std::vector<std::string> args;
  args.push_back(program_name);
  std::ifstream file(conf);
  std::string token;
  uint16_t nb_ports;

  BuildIptoServerMap(servers);

  while (file >> token) {
    args.push_back(token);
  }

  std::vector<char *> argv;
  for (auto &arg : args) {
    argv.push_back(const_cast<char *>(arg.c_str()));
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

  for (const auto &server : servers) {
    server.second->SetKvMigrationRing(kv_migration_ring);
  }

  nb_ports = rte_eth_dev_count_avail();
  if (nb_ports < 1) rte_exit(EXIT_FAILURE, "Error: need at least one port\n");

  initialized_ = true;
  return initialized_;
}

inline void DPDKHandler::SwapMac(rte_ether_hdr *eth_hdr) {
  rte_ether_addr tmp_mac;
  rte_ether_addr_copy(&eth_hdr->src_addr, &tmp_mac);
  rte_ether_addr_copy(&eth_hdr->dst_addr, &eth_hdr->src_addr);
  rte_ether_addr_copy(&tmp_mac, &eth_hdr->dst_addr);
}

inline void DPDKHandler::SwapIpv4(rte_ipv4_hdr *ip_hdr) {
  uint32_t tmp_ip = ip_hdr->src_addr;
  ip_hdr->src_addr = ip_hdr->dst_addr;
  ip_hdr->dst_addr = tmp_ip;
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
  rss_conf.rss_hf = RTE_ETH_RSS_IP | RTE_ETH_RSS_TCP | RTE_ETH_RSS_UDP;

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
  uint16_t nb_normal_cores = normal_cores_.size();
  uint16_t nb_special_cores = special_cores_.size();
  retval = rte_eth_dev_configure(
      port, nb_normal_cores, nb_normal_cores + nb_special_cores, &port_conf);
  if (retval != 0) return retval;

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &rx_ring_size, &tx_ring_size);
  if (retval != 0) return retval;

  rte_eth_txconf txconf;
  txconf = dev_info.default_txconf;
  txconf.offloads = port_conf_default.txmode.offloads;

  uint16_t queue_id = 0;
  for (uint i = 0; i < nb_normal_cores; i++) {
    retval =
        rte_eth_rx_queue_setup(port, queue_id, rx_ring_size,
                               rte_eth_dev_socket_id(port), NULL, rx_mbufpool_);
    if (retval < 0) return retval;

    retval = rte_eth_tx_queue_setup(port, queue_id, tx_ring_size,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) return retval;
    normal_cores_[i].second = queue_id++;
  }

  for (uint i = 0; i < nb_special_cores; i++) {
    retval = rte_eth_tx_queue_setup(port, queue_id, tx_ring_size,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) return retval;
    special_cores_[i].second = queue_id++;
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

void DPDKHandler::MainLoop(CoreInfo core_info) {
  uint16_t port = 0;

  uint lcore_id = core_info.first;
  uint16_t queue_id = core_info.second;

  std::unordered_map<rte_be32_t, std::shared_ptr<sw::redis::Redis>> ip_to_db;

  BuildIptoDBMap(ip_to_db);

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

    RTE_LOG(NOTICE, CORE, "[Normal] %u polling queue: %hu\n", lcore_id,
            queue_id);

    rte_mbuf *bufs[BURST_SIZE];
    while (true) {
      uint16_t nb_rx = 0;
      nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      if (unlikely(nb_rx <= 0)) {
        rte_delay_us(10);
        continue;
      } else {
        for (uint16_t i = 0; i < nb_rx; i++) {
          // uint64_t start_us = utils::get_now_micros();

          struct rte_mbuf *resp_buf = rte_pktmbuf_clone(bufs[i], tx_mbufpool_);
          rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(resp_buf, rte_ether_hdr *);

          rte_ipv4_hdr *ip_hdr = reinterpret_cast<rte_ipv4_hdr *>(eth_hdr + 1);
          rte_prefetch0(ip_hdr + 1);

          SwapMac(eth_hdr);

          rte_be32_t dst_addr = ip_hdr->dst_addr;

          ip_hdr->dst_addr = ip_hdr->src_addr;
          ip_hdr->src_addr = dst_addr;

          KVHeader *kv_header = reinterpret_cast<KVHeader *>(ip_hdr + 1);
          // std::cout << completed_request_count << std::endl;
          kv_header->combined |= 0x1000;  // SERVER_REPLY << 12

          auto it = ip_to_db.find(dst_addr);
          if (it != ip_to_db.end()) {
            uint8_t op = GET_OP(kv_header->combined);
            uint8_t is_req = GET_IS_REQ(kv_header->combined);

            auto value_ptr = kv_header->value1.data();
            std::string_view key{kv_header->key.data(), KEY_LENGTH};

            if (op == WRITE_REQUEST) {
              std::string_view value{value_ptr, VALUE_LENGTH * 4};
              it->second->set(key, value);

              if (is_req == WRITE_MIRROR || is_req == CACHE_MIGRATE) {
                KVMigrateHeader *kv_migration_header =
                    (KVMigrateHeader *)(kv_header);

                if (auto server = GetServerByIp(dst_addr))
                  server->CacheMigrate(key, kv_migration_header->migration_id);
                rte_pktmbuf_free(bufs[i]);
                continue;

              } else if (is_req == MIGRATE_REPLY) {
                // if (auto server = GetServerByIp(dst_addr))
                //   server->HandleMigrateReply(kv_header->request_id);
              }
            } else {
              if (auto val = it->second->get(key)) {
                rte_memcpy(value_ptr, val->data(), VALUE_LENGTH * 4);
              } else {
                RTE_LOG(WARNING, DB, "[%d.%d.%d.%d] Not find key: %.*s\n",
                        DECODE_IP(dst_addr), KEY_LENGTH, kv_header->key.data());
              }
            }
          } else {
            RTE_LOG(WARNING, DB,
                    "[%d.%d.%d.%d] Not find db on lcore: %u, nb_rx: %d\n ",
                    DECODE_IP(dst_addr), rte_lcore_id(), nb_rx);
          }

          uint16_t nb_tx = rte_eth_tx_burst(port, queue_id, &resp_buf, 1);
          if (unlikely(nb_tx != 1))
            RTE_LOG(WARNING, DB, "Send error on lcore: %u\n", rte_lcore_id());
          rte_pktmbuf_free(resp_buf);
        }
        rte_pktmbuf_free_bulk(bufs, nb_rx);
      }
    }
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
}

void DPDKHandler::SpecialLoop(CoreInfo core_info) {
  uint lcore_id = core_info.first;
  uint16_t queue_id = core_info.second;

  RTE_LOG(NOTICE, CORE, "[Special] %u polling queue: %hu\n", lcore_id,
          queue_id);

  while (true) {
    std::vector<uint8_t> *packet_data = nullptr;
    if (rte_ring_dequeue(kv_migration_ring, (void **)&packet_data) == 0) {
      if (packet_data) {
        rte_mbuf *mbuf = rte_pktmbuf_alloc(tx_mbufpool_);
        if (!mbuf) {
          std::cerr << "Failed to allocate mbuf" << std::endl;
          delete packet_data;
          continue;
        }
        char *mbuf_data = rte_pktmbuf_mtod(mbuf, char *);
        rte_memcpy(mbuf_data, packet_data->data(), packet_data->size());

        rte_pktmbuf_data_len(mbuf) = packet_data->size();
        rte_pktmbuf_pkt_len(mbuf) = rte_pktmbuf_data_len(mbuf);
        delete packet_data;
        int ret = rte_eth_tx_burst(0 /*port id*/, queue_id, &mbuf, 1);
        if (ret < 1) {
          RTE_LOG(ERR, CORE, "Send error: %s (errno=%d)\n", rte_strerror(-ret),
                  -ret);
          rte_pktmbuf_free(mbuf);
          continue;
        }
      }
    } else {
      rte_pause();
    }
  }
}

inline int DPDKHandler::LaunchNormalLcore(void *arg) {
  CoreArgs *args = static_cast<CoreArgs *>(arg);
  args->instance->MainLoop(args->core_info);
  return 0;
}

inline int DPDKHandler::LaunchSpeciaLcore(void *arg) {
  CoreArgs *args = static_cast<CoreArgs *>(arg);
  args->instance->SpecialLoop(args->core_info);
  return 0;
}

inline void DPDKHandler::LaunchThreads(
    const std::vector<DPDKHandler::CoreInfo> &special_cores_,
    const std::vector<DPDKHandler::CoreInfo> &normal_cores_) {
  for (auto &lcore : special_cores_) {
    uint core_id = lcore.first;
    uint16_t queue_id = lcore.second;

    auto args = std::make_unique<CoreArgs>();
    args->instance = this;
    args->core_info = std::make_pair(core_id, queue_id);

    core_args_.push_back(std::move(args));
    int ret = rte_eal_remote_launch(LaunchSpeciaLcore, core_args_.back().get(),
                                    core_id);
    if (ret < 0) {
      std::cerr << "Failed to launch special thread on core " << core_id
                << ", error: " << rte_strerror(-ret) << std::endl;
      return;
    }
  }

  for (auto &lcore : normal_cores_) {
    uint core_id = lcore.first;
    uint16_t queue_id = lcore.second;

    auto args = std::make_unique<CoreArgs>();
    args->instance = this;
    args->core_info = std::make_pair(core_id, queue_id);

    core_args_.push_back(std::move(args));
    int ret = rte_eal_remote_launch(LaunchNormalLcore, core_args_.back().get(),
                                    core_id);
    if (ret < 0) {
      std::cerr << "Failed to launch normal thread on core " << core_id
                << ", error: " << rte_strerror(-ret) << std::endl;
      return;
    }
  }
}

void DPDKHandler::Start() {
  uint16_t port = 0;
  uint nb_ports = rte_eth_dev_count_avail();

  uint count = 0;
  uint lcore_id;

  RTE_LCORE_FOREACH_WORKER(lcore_id) {
    if (count++ < 1) {
      special_cores_.push_back(std::make_pair(lcore_id, 0));
    } else {
      normal_cores_.push_back(std::make_pair(lcore_id, 0));
    }
  }

  tx_mbufpool_ = rte_pktmbuf_pool_create(
      "TX_MBUF_POOL", TX_NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      TX_MBUF_DATA_SIZE, rte_socket_id());
  if (tx_mbufpool_ == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  // uint32_t pool_size =
  //     (RX_RING_SIZE * 2 * normal_cores_.size()) * SAFETY_FACTOR;
  rx_mbufpool_ = rte_pktmbuf_pool_create(
      "RX_MBUF_POOL", RX_NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      RX_MBUF_DATA_SIZE, rte_socket_id());
  if (rx_mbufpool_ == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &s_eth_addr_);

  if (PortInit() != 0)
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);

  LaunchThreads(special_cores_, normal_cores_);
  while (true) {
    utils::monitor_mempool(rx_mbufpool_);
    utils::monitor_mempool(tx_mbufpool_);
    for (size_t i = 0; i < normal_cores_.size(); ++i)
      std::cout << rte_eth_rx_queue_count(port, i) << " | ";
    std::cout << std::endl;
    rte_delay_ms(1000);
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
