#pragma once

#include <rte_ethdev.h>
#include <rte_ip4.h>

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#define ENCODE_DEV_INFO(dev_id, dev_type) \
  (((dev_id & 0x1F) << 3) | (dev_type & 0x07))

#define GET_DEV_ID(dev_info) static_cast<uint8_t>(((dev_info) >> 3) & 0x1F)
#define GET_DEV_TYPE(dev_info) static_cast<uint8_t>((dev_info) & 0x07)

#define ENCODE_COMBINED(is_req, op) \
  (((is_req & 0x0F) << 4) | ((op & 0x03) << 2) | (0x00 & 0x03))

#define GET_IS_REQ(combined) static_cast<uint8_t>(((combined) >> 4) & 0x0F)
#define GET_OP(combined) static_cast<uint8_t>(((combined) >> 2) & 0x03)
#define GET_HOT_QUERY(combined) static_cast<uint8_t>((combined) & 0x03)

#define DECODE_IP(ip) \
  ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (ip >> 24) & 0xFF

#define UDP_PORT_KV 50000
#define UDP_PORT_MI 50001
#define UDP_PORT_CM 50002

namespace c_m_proto {

using Packet = std::vector<uint8_t>;

constexpr size_t CACHE_SIZE = 310;
constexpr size_t CHUNK_SIZE = 128;

constexpr size_t KEY_LENGTH = 16;
constexpr size_t VALUE_LENGTH = 4;

constexpr uint16_t RETRIES = 3;

// DevType
constexpr uint8_t DEV_SPINE = 0;
constexpr uint8_t DEV_LEAF = 1;
constexpr uint8_t DEV_CLIENT = 2;
constexpr uint8_t DEV_UNKNOWN = 3;

// op
constexpr uint8_t READ_REQUEST = 0;
constexpr uint8_t WRITE_REQUEST = 1;
constexpr uint8_t NO_REQUEST = 0xFF;

// is_req
constexpr uint8_t CLIENT_REQUEST = 0;
constexpr uint8_t SERVER_REPLY = 1;
constexpr uint8_t CACHE_REJECT = 2;
constexpr uint8_t CACHE_REPLY = 3;
constexpr uint8_t CACHE_MIGRATE = 4;
constexpr uint8_t MIGRATE_REPLY = 6;
constexpr uint8_t WRITE_MIRROR = 5;

// migration_status
constexpr uint8_t MIGRATION_NO = 0;
constexpr uint8_t MIGRATION_START = 1;
constexpr uint8_t MIGRATION_DONE = 2;
constexpr uint8_t MIGRATION_TRANSFER_DONE = 6;

struct ControllerInfo {
  std::string iface;
  std::string ip;
  std::array<uint8_t, RTE_ETHER_ADDR_LEN> mac;
};

struct SockConfig {
  int sockfd;
  int ifindex;
};

#pragma pack(push, 1)

struct KVHeader {
  uint8_t dev_info;
  uint32_t request_id = 0;
  uint8_t combined;
};

struct KVRequest : KVHeader {
  std::array<char, KEY_LENGTH> key{};
  std::array<char, VALUE_LENGTH> value1{};
  std::array<char, VALUE_LENGTH> value2{};
  std::array<char, VALUE_LENGTH> value3{};
  std::array<char, VALUE_LENGTH> value4{};
};

struct KVMigrate : KVRequest {
  uint32_t migration_id = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
  uint16_t cache_index = 0;
  uint16_t total_keys = 0;
};

struct MigrationInfo {
  uint32_t request_id = 0;
  uint32_t migration_id = 0;
  uint8_t migration_status = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
};

struct CacheMigrate {
  uint32_t request_id = 0;
  uint32_t migration_id = 0;
  std::array<char, KEY_LENGTH> key{};
};

#pragma pack(pop)

constexpr uint16_t IPV4_HDR_LEN = sizeof(rte_ipv4_hdr);
constexpr uint16_t UDP_HDR_LEN = sizeof(rte_udp_hdr);
constexpr uint16_t KV_HDR_LEN = sizeof(KVRequest);
constexpr uint16_t KV_MIGRATE_HDR_LEN = sizeof(KVMigrate);
constexpr uint16_t KV_HEADER_OFFSET =
    RTE_ETHER_HDR_LEN + IPV4_HDR_LEN + UDP_HDR_LEN;

const uint16_t TOTAL_LEN =
    RTE_ETHER_HDR_LEN + IPV4_HDR_LEN + UDP_HDR_LEN + KV_HDR_LEN;

template <typename PayloadType>
struct PacketTraits {
  static constexpr uint8_t Protocol = IPPROTO_UDP;
  static constexpr uint16_t SrcPort = UDP_PORT_KV;
  static constexpr uint16_t DstPort = UDP_PORT_KV;
};

template <>
struct PacketTraits<KVRequest> {
  static constexpr uint8_t Protocol = IPPROTO_UDP;
  static constexpr uint16_t SrcPort = UDP_PORT_KV;
  static constexpr uint16_t DstPort = UDP_PORT_KV;
};

template <>
struct PacketTraits<MigrationInfo> {
  static constexpr uint8_t Protocol = IPPROTO_UDP;
  static constexpr uint16_t SrcPort = UDP_PORT_MI;
  static constexpr uint16_t DstPort = UDP_PORT_MI;
};

template <>
struct PacketTraits<CacheMigrate> {
  static constexpr uint8_t Protocol = IPPROTO_UDP;
  static constexpr uint16_t SrcPort = UDP_PORT_CM;
  static constexpr uint16_t DstPort = UDP_PORT_CM;
};
}  // namespace c_m_proto
