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

struct CustomPacket {
  rte_ether_hdr eth_hdr;
  rte_ipv4_hdr ip_hdr;
  KVHeader kv_header;
};

void ReverseRTE_IPV4(uint32_t ip, std::string &result) {
  uint8_t a = (ip >> 24) & 0xFF;
  uint8_t b = (ip >> 16) & 0xFF;
  uint8_t c = (ip >> 8) & 0xFF;
  uint8_t d = ip & 0xFF;

  result = std::to_string(d) + "." + std::to_string(c) + "." +
           std::to_string(b) + "." + std::to_string(a);
}

const uint32_t custom_packet_len = sizeof(CustomPacket);

std::ofstream outfile("server.output");

DPDKHandler::DPDKHandler() {}

DPDKHandler::~DPDKHandler() {
  if (initialized_) {
    initialized_ = false;

    if (kv_migration_event_fd_ptr_ && *kv_migration_event_fd_ptr_ != -1) {
      close(*kv_migration_event_fd_ptr_);
    }
    if (epoll_fd_ != -1) {
      close(epoll_fd_);
    }

    uint16_t port = 0;
    rte_eth_dev_stop(port);
    rte_eth_dev_close(port);

    rte_ring_free(kv_migration_ring);

    rte_eal_cleanup();
    printf("Bye...\n");
  }
}

inline void DPDKHandler::BuildIptoServerMap(
    const std::vector<std::shared_ptr<ServerInstance>> &servers) {
  for (const auto &server : servers) {
    if (!server) {
      RTE_LOG(ERR, DB, "Invalid ServerInstance pointer!\n");
      continue;
    }
    std::unique_lock lock(ip_map_mutex_);
    rte_be32_t be32_ip = server->GetIp();
    auto redis = std::make_shared<sw::redis::Redis>(
        "tcp://127.0.0.1:6379/" + std::to_string(server->GetDb()));
    RTE_LOG(INFO, DB, "Init Redis index: %d \n", server->GetDb());
    ip_to_server_.emplace(be32_ip, std::make_pair(server, redis));
  }
}

bool DPDKHandler::Initialize(
    const std::string &conf, char *program_name,
    const std::vector<std::shared_ptr<ServerInstance>> &servers) {
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

  EventInit();

  for (const auto &server : servers) {
    server->SetKvMigrationRing(kv_migration_ring, kv_migration_event_fd_ptr_);
  }

  nb_ports = rte_eth_dev_count_avail();
  if (nb_ports < 1) rte_exit(EXIT_FAILURE, "Error: need at least one port\n");

  initialized_ = true;
  return initialized_;
}

void DPDKHandler::EventInit() {
  kv_migration_event_fd_ptr_ = std::make_shared<int>(-1);
  *kv_migration_event_fd_ptr_ = eventfd(0, EFD_NONBLOCK);

  if (*kv_migration_event_fd_ptr_ == -1) {
    perror("eventfd");
    exit(EXIT_FAILURE);
  }

  epoll_fd_ = epoll_create1(0);

  if (epoll_fd_ == -1) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  epoll_event ev1;
  ev1.events = EPOLLIN;
  ev1.data.fd = *kv_migration_event_fd_ptr_;
  if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, *kv_migration_event_fd_ptr_, &ev1) ==
      -1) {
    perror("epoll_ctl kv_migration_event_fd_");
    exit(EXIT_FAILURE);
  }
}

inline void DPDKHandler::SwapMac(struct rte_ether_hdr *eth_hdr) {
  struct rte_ether_addr tmp_mac;
  rte_ether_addr_copy(&eth_hdr->src_addr, &tmp_mac);
  rte_ether_addr_copy(&eth_hdr->dst_addr, &eth_hdr->src_addr);
  rte_ether_addr_copy(&tmp_mac, &eth_hdr->dst_addr);
}

inline void DPDKHandler::SwapIpv4(struct rte_ipv4_hdr *ip_hdr) {
  uint32_t tmp_ip = ip_hdr->src_addr;
  ip_hdr->src_addr = ip_hdr->dst_addr;
  ip_hdr->dst_addr = tmp_ip;
}

int DPDKHandler::PortInit() {
  uint16_t port = 0;
  uint16_t nb_rxd = RX_RING_SIZE;
  uint16_t nb_txd = TX_RING_SIZE;
  int retval;

  if (mbuf_pool_ == NULL) {
    RTE_LOG(ERR, EAL,
            "mbuf_pool is NULL, please create a mempool before calling "
            "PortInit.\n");
    return -1;
  }

  struct rte_eth_conf port_conf;
  if (!rte_eth_dev_is_valid_port(port)) return -1;
  memset(&port_conf, 0, sizeof(struct rte_eth_conf));

  struct rte_eth_dev_info dev_info;
  retval = rte_eth_dev_info_get(port, &dev_info);
  if (retval != 0) {
    RTE_LOG(ERR, EAL, "Error during getting device (port %u) info: %s\n", port,
            strerror(-retval));
    return retval;
  }

  if (dev_info.flow_type_rss_offloads & RTE_ETH_RSS_IP) {
    port_conf.rxmode.mq_mode = RTE_ETH_MQ_RX_RSS;
    port_conf.rx_adv_conf.rss_conf.rss_key = nullptr;
    port_conf.rx_adv_conf.rss_conf.rss_hf = RTE_ETH_RSS_IP;
    RTE_LOG(NOTICE, EAL, "RSS enabled: RTE_ETH_RSS_IP\n");
  }

  if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
    port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

  /* Configure the Ethernet device. */
  uint16_t nb_normal_cores = normal_cores_.size();
  uint16_t nb_special_cores = special_cores_.size();
  uint16_t nb_cores = nb_normal_cores + nb_special_cores;
  retval = rte_eth_dev_configure(port, nb_normal_cores, nb_cores, &port_conf);
  if (retval != 0) return retval;

  retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd, &nb_txd);
  if (retval != 0) return retval;

  struct rte_eth_txconf txconf;
  txconf = dev_info.default_txconf;
  txconf.offloads = port_conf.txmode.offloads;

  uint16_t queue_id = 0;
  for (uint i = 0; i < nb_normal_cores; i++) {
    retval = rte_eth_rx_queue_setup(
        port, queue_id, nb_rxd, rte_eth_dev_socket_id(port), NULL, mbuf_pool_);
    if (retval < 0) return retval;

    retval = rte_eth_tx_queue_setup(port, queue_id, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) return retval;
    normal_cores_[i].second = queue_id++;
  }

  for (uint i = 0; i < nb_special_cores; i++) {
    retval = rte_eth_tx_queue_setup(port, queue_id, nb_txd,
                                    rte_eth_dev_socket_id(port), &txconf);
    if (retval < 0) return retval;
    special_cores_[i].second = queue_id++;
  }

  /* Starting Ethernet port. 8< */
  retval = rte_eth_dev_start(port);
  if (retval < 0) return retval;

  RTE_LOG(INFO, EAL, "Port %u MAC: " RTE_ETHER_ADDR_PRT_FMT "\n",
          (unsigned)port, RTE_ETHER_ADDR_BYTES(&s_eth_addr_));

  retval = rte_eth_promiscuous_enable(port);
  if (retval != 0) return retval;

  return 0;
}

void DPDKHandler::ProcessReceivedPacket(struct rte_mbuf *mbuf, uint16_t port,
                                        uint16_t queue_id) {
  struct rte_ether_hdr *eth_hdr =
      rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
  if (eth_hdr->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4)) {
    rte_pktmbuf_free(mbuf);
    return;
  }

  struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
  rte_prefetch0(ip_hdr);

  if (ip_hdr->next_proto_id == IP_PROTOCOLS_NETCACHE) {
    SwapMac(eth_hdr);
    rte_be32_t dst_addr = ip_hdr->dst_addr;
    if (auto db = GetDbByIp(dst_addr)) {
      struct KVHeader *kv_header = (struct KVHeader *)(ip_hdr + 1);
      rte_prefetch0(kv_header);

      uint8_t op = GET_OP(kv_header->combined);

      uint8_t is_req = GET_IS_REQ(kv_header->combined);
      if (is_req == CLIENT_REQUEST) kv_header->combined |= 0x1000; // SERVER_REPLY << 12

      auto value_ptr = kv_header->value1.data();
      std::string_view key{kv_header->key.data(), KEY_LENGTH};

      if (op == WRITE_REQUEST) {
        db->set(key, std::string_view(value_ptr, VALUE_LENGTH * 4));

        if (is_req == WRITE_MIRROR || is_req == CACHE_MIGRATE) {
          RTE_LOG(INFO, DB, "Receive a %s packet\n",
                  is_req == WRITE_MIRROR ? "WRITE_MIRROR" : "CACHE_MIGRATE");
          struct KVMigrateHeader *kv_migration_header =
              (struct KVMigrateHeader *)(kv_header);
          if (auto server = GetServerByIp(dst_addr))
            server->CacheMigrate(
                key, rte_be_to_cpu_32(kv_migration_header->migration_id));
          if (is_req == WRITE_MIRROR) {
            rte_pktmbuf_free(mbuf);
            return;
          } else if (is_req == CACHE_MIGRATE) {
            kv_header->combined |= 0x6000; // MIGRATE_REPLY << 12
          }
        }
      } else if (op == READ_REQUEST) {
        if (auto val = db->get(key)) {
          memcpy(value_ptr, val->data(), VALUE_LENGTH * 4);
        } else {
          RTE_LOG(WARNING, DB, "[%d.%d.%d.%d] Not find key: %.*s\n",
                  DECODE_IP(dst_addr), KEY_LENGTH, kv_header->key.data());
        }
      }
    } else {
      RTE_LOG(WARNING, DB, "[%d.%d.%d.%d] Not find db on lcore: %u\n",
              DECODE_IP(dst_addr), rte_lcore_id());
    }

    ip_hdr->dst_addr = ip_hdr->src_addr;
    ip_hdr->src_addr = dst_addr;

    const uint16_t nb_tx = rte_eth_tx_burst(port, queue_id, &mbuf, 1);
    if (nb_tx < 1) {
      rte_pktmbuf_free(mbuf);
    }
  }
}

void DPDKHandler::MainLoop(CoreInfo core_info) {
  uint16_t port = 0;

  uint lcore_id = core_info.first;
  uint16_t queue_id = core_info.second;

  if (lcore_id == RTE_MAX_LCORE || lcore_id == (unsigned)LCORE_ID_ANY) {
    printf("Invalid lcore_id=%u\n", lcore_id);
    rte_exit(EXIT_FAILURE, "Invalid lcore_id=%u\n", lcore_id);
    return;
  }
  if (rte_lcore_is_enabled(lcore_id) && lcore_id != rte_get_main_lcore()) {
    if (rte_eth_dev_socket_id(port) >= 0 &&
        rte_eth_dev_socket_id(port) != (int)rte_socket_id())
      printf(
          "[WARNING], port %u is on remote NUMA node to "
          "polling thread.\n\tPerformance will "
          "not be optimal.\n",
          port);

    RTE_LOG(NOTICE, WORKER,
            "[Normal] Core: %u polling queue: %hu on socket %u\n", lcore_id,
            queue_id, rte_lcore_to_socket_id(lcore_id));
    struct rte_mbuf *bufs[BURST_SIZE];
    while (true) {
      const uint16_t nb_rx = rte_eth_rx_burst(port, queue_id, bufs, BURST_SIZE);

      if (unlikely(nb_rx == 0)) continue;

      if (nb_rx > 0) {
        for (uint16_t i = 0; i < nb_rx; i++) {
          if (bufs[i] != NULL) {
            ProcessReceivedPacket(bufs[i], port, queue_id);
          }
        }
      }
    }
  } else {
    printf("Skip main lcore %u\n", lcore_id);
  }
}

void DPDKHandler::SpecialLoop(CoreInfo core_info) {
  uint lcore_id = core_info.first;
  uint16_t queue_id = core_info.second;

  RTE_LOG(NOTICE, WORKER, "[Special] Core: %u polling queue: %hu\n", lcore_id,
          queue_id);

  const int timeout_ms = -1;
  epoll_event event;
  while (true) {
    int nfds = epoll_wait(epoll_fd_, &event, 1, timeout_ms);
    if (nfds == -1) {
      if (errno == EINTR) continue;
      perror("epoll_wait");
      break;
    }

    if (nfds == 1) {
      if (event.data.fd != *kv_migration_event_fd_ptr_) continue;
      uint64_t val;
      read(*kv_migration_event_fd_ptr_, &val, sizeof(val));

      std::shared_ptr<std::vector<uint8_t>> packet_data(nullptr);
      while (rte_ring_dequeue(kv_migration_ring, (void **)&packet_data) == 0) {
        if (packet_data) {
          struct rte_mbuf *mbuf = rte_pktmbuf_alloc(mbuf_pool_);
          if (!mbuf) {
            std::cerr << "Failed to allocate mbuf" << std::endl;
            return;
          }

          char *mbuf_data = rte_pktmbuf_mtod(mbuf, char *);
          memcpy(mbuf_data, packet_data->data(), packet_data->size());

          rte_pktmbuf_data_len(mbuf) = packet_data->size();
          rte_pktmbuf_pkt_len(mbuf) = rte_pktmbuf_data_len(mbuf);

          int ret = rte_eth_tx_burst(0 /*port id*/, queue_id, &mbuf, 1);
          if (ret < 1) {
            RTE_LOG(ERR, WORKER,
                    "Failed to send packet on core %u, queue %hu: %s\n",
                    lcore_id, queue_id, rte_strerror(-ret));
            rte_pktmbuf_free(mbuf);
            return;
          }
          packet_data.reset();
        }
      }
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

  mbuf_pool_ = rte_pktmbuf_pool_create(
      "MBUF_POOL", NUM_MBUFS * nb_ports, MBUF_CACHE_SIZE, 0,
      RTE_MBUF_DEFAULT_BUF_SIZE, rte_socket_id());
  if (mbuf_pool_ == NULL) rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");

  uint count = 0;
  uint lcore_id;

  RTE_LCORE_FOREACH_WORKER(lcore_id) {
    if (count++ < 1) {
      special_cores_.push_back(std::make_pair(lcore_id, 0));
    } else {
      normal_cores_.push_back(std::make_pair(lcore_id, 0));
    }
  }

  rte_ether_unformat_addr("aa:bb:cc:dd:ee:ff", &s_eth_addr_);

  if (PortInit() != 0)
    rte_exit(EXIT_FAILURE, "Cannot init port %" PRIu16 "\n", port);

  LaunchThreads(special_cores_, normal_cores_);

  rte_eal_mp_wait_lcore();
}
