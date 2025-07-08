#pragma once

#include <rte_ethdev.h>
#include <rte_ip4.h>

#include <array>
#include <cstdint>
#include <vector>

#define ENCODE_COMBINED(dev_id, is_req, op)                               \
  (((dev_id & 0xFF) << 8) | ((is_req & 0x0F) << 4) | ((op & 0x03) << 2) | \
   (0x00 & 0x03))
#define DECODE_IP(ip) \
  ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (ip >> 24) & 0xFF

#define GET_DEV_ID(combined) static_cast<uint8_t>((combined) & 0xFF)
#define GET_IS_REQ(combined) static_cast<uint8_t>(((combined) >> 12) & 0x0F)
#define GET_OP(combined) static_cast<uint8_t>(((combined) >> 10) & 0x03)
#define GET_HOT_QUERY(combined) static_cast<uint8_t>(((combined) >> 8) & 0x03)

#define IP_PROTOCOLS_NETCACHE 0x54
#define IP_PROTOCOLS_MIGRATION_INFO 0x57
#define IP_PROTOCOLS_CACHE_MIGRATE 0x60
#define IP_PROTOCOLS_ASK 0x61

#define KEY_LENGTH 16
#define VALUE_LENGTH 4

constexpr size_t CACHE_SIZE = 256;
constexpr size_t CHUNK_SIZE = 128;
constexpr uint16_t RETRIES = 3;

// op
constexpr uint8_t READ_REQUEST = 0;
constexpr uint8_t WRITE_REQUEST = 1;

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
  std::array<uint8_t, ETH_ALEN> mac;
};

struct SockConfig {
  int sockfd;
  int ifindex;
};

#pragma pack(push, 1)
struct BaseHeader {
  uint32_t request_id = 0;
};

struct KVHeader : public BaseHeader {
  uint16_t
      combined;  // dev_id(8 bit) | is_req(4 bit) | op(2 bit) | hot_query(2 bit)
  uint32_t count = 0;
  std::array<char, KEY_LENGTH> key{};
  std::array<char, VALUE_LENGTH> value1{};
  std::array<char, VALUE_LENGTH> value2{};
  std::array<char, VALUE_LENGTH> value3{};
  std::array<char, VALUE_LENGTH> value4{};
};

struct KVMigrateHeader : public KVHeader {  // pick up from KVHeader
  uint32_t migration_id = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
  uint16_t cache_index = 0;
  uint16_t total_keys = 0;
};

struct MigrationInfo : public BaseHeader {
  uint32_t migration_id = 0;
  uint8_t migration_status = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
};

struct CacheMigrateHeader : public BaseHeader {
  uint32_t migration_id = 0;
  std::array<char, KEY_LENGTH> key{};
};

struct AskPacket : public BaseHeader {
  uint8_t ask = 0;
};
#pragma pack(pop)

constexpr uint16_t IPV4_HDR_LEN = sizeof(struct rte_ipv4_hdr);
constexpr uint16_t C_M_HDR_LEN = sizeof(struct KVHeader);

const uint16_t TOTAL_LEN = RTE_ETHER_HDR_LEN + IPV4_HDR_LEN + C_M_HDR_LEN;
template <typename T>
struct PacketTraits;

template <typename T>
struct PacketTraits {
  static_assert(sizeof(T) == 0,
                "PacketTraits<T> is not specialized for this type");
};

template <>
struct PacketTraits<KVHeader> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_NETCACHE;
};

template <>
struct PacketTraits<KVMigrateHeader> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_NETCACHE;
};

template <>
struct PacketTraits<MigrationInfo> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_MIGRATION_INFO;
};

template <>
struct PacketTraits<CacheMigrateHeader> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_CACHE_MIGRATE;
};

template <>
struct PacketTraits<AskPacket> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_ASK;
};
