#pragma once

#include <rte_ethdev.h>
#include <rte_ip4.h>

#include <array>
#include <cstdint>
#include <vector>

#define ENCODE_COMBINED(dev_id, op)                                     \
  (((dev_id & 0xFF) << 8) | ((0x00 & 0x0F) << 4) | ((op & 0x03) << 2) | \
   (0x00 & 0x03))
#define DECODE_IP(ip) \
  ip & 0xFF, (ip >> 8) & 0xFF, (ip >> 16) & 0xFF, (ip >> 24) & 0xFF

#define GET_DEV_ID(combined) static_cast<uint8_t>((combined) & 0xFF)
#define GET_IS_REQ(combined) static_cast<uint8_t>(((combined) >> 12) & 0x0F)
#define GET_OP(combined) static_cast<uint8_t>(((combined) >> 10) & 0x03)
#define GET_HOT_QUERY(combined) static_cast<uint8_t>(((combined) >> 8) & 0x03)

#define IP_PROTOCOLS_NETCACHE 0x54
#define IP_PROTOCOLS_HOTREPORT 0x55
#define IP_PROTOCOLS_MIGRATION_INFO 0x57
#define IP_PROTOCOLS_MIGRATION_READY 0x58
#define IP_PROTOCOLS_KV_MIGRATION 0x59
#define IP_PROTOCOLS_PRE_WRITE 0x60
#define IP_PROTOCOLS_ASK 0x61

#define KEY_LENGTH 16
#define VALUE_LENGTH 4

constexpr uint16_t RETRIES = 3;

// op
constexpr uint8_t READ_REQUEST = 0;
constexpr uint8_t WRITE_REQUEST = 1;

// is_req
constexpr uint8_t CLIENT_REQUEST = 0;
constexpr uint8_t SERVER_REPLY = 1;
constexpr uint8_t CACHE_REJECT = 2;
constexpr uint8_t CACHE_REPLY = 3;
constexpr uint8_t WRITE_MIGRATION = 5;

// migration_status
constexpr uint8_t MIGRATION_NO = 0;
constexpr uint8_t MIGRATION_PREPARING = 3;
constexpr uint8_t MIGRATION_TRANSFERRING = 1;
constexpr uint8_t MIGRATION_DONE = 2;
constexpr uint8_t MIGRATION_NOT_ENOUGH = 5;

#pragma pack(push, 1)
struct BaseHeader {
  uint32_t request_id = 0;
};

struct KVHeader : public BaseHeader {
  uint16_t
      combined;  // dev_id(8 bit) | is_req(4 bit) | op(2 bit) | hot_query(2 bit)
  uint32_t count;
  std::array<char, KEY_LENGTH> key{};
  std::array<char, VALUE_LENGTH> value1{};
  std::array<char, VALUE_LENGTH> value2{};
  std::array<char, VALUE_LENGTH> value3{};
  std::array<char, VALUE_LENGTH> value4{};
};

struct ReportHotKey : public BaseHeader {
  uint8_t dev_id = 0;
  std::array<char, KEY_LENGTH> key_hot{};
  std::array<char, VALUE_LENGTH> value1{};
  std::array<char, VALUE_LENGTH> value2{};
  std::array<char, VALUE_LENGTH> value3{};
  std::array<char, VALUE_LENGTH> value4{};
  std::array<char, KEY_LENGTH> key_replace{};
  uint32_t count = 0;
};

struct MigrationInfo : public BaseHeader {
  uint32_t migration_id = 0;
  uint8_t migration_status = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
};

struct KeyMigrationReady : public BaseHeader {
  uint32_t migration_id = 0;
  uint8_t migration_status = 0;
  uint16_t chunk_index = 0;
  uint16_t total_chunks = 0;
  uint16_t chunk_size = 0;
  std::vector<std::array<char, KEY_LENGTH>> keys;
  uint8_t is_final = 0;

  KeyMigrationReady(size_t key_count = 0)
      : keys(key_count, std::array<char, KEY_LENGTH>{}) {}
};

struct KV_Migration : public BaseHeader {
  uint8_t dev_id = 0;
  uint32_t migration_id = 0;
  uint8_t src_rack_id = 0;
  uint8_t dst_rack_id = 0;
  uint16_t chunk_index = 0;
  uint16_t total_chunks = 0;
  uint16_t chunk_size = 0;
  uint8_t compression = 1;
  uint8_t is_last_chunk = 0;
};

struct PreWrite : public BaseHeader {
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
struct PacketTraits<ReportHotKey> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_HOTREPORT;
};

template <>
struct PacketTraits<MigrationInfo> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_MIGRATION_INFO;
};

template <>
struct PacketTraits<KeyMigrationReady> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_MIGRATION_READY;
};

template <>
struct PacketTraits<KV_Migration> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_KV_MIGRATION;
};

template <>
struct PacketTraits<PreWrite> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_PRE_WRITE;
};

template <>
struct PacketTraits<AskPacket> {
  static constexpr uint8_t Protocol = IP_PROTOCOLS_ASK;
};
