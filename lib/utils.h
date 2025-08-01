#pragma once

#include <linux/if_ether.h>
#include <netinet/ip.h>
#include <rte_byteorder.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <climits>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>

namespace utils {

inline void monitor_mempool(struct rte_mempool *mp) {
  unsigned avail = rte_mempool_avail_count(mp);
  unsigned in_use = rte_mempool_in_use_count(mp);
  double use_percent = (double)in_use * 100.0 / (double)mp->size;

  RTE_LOG(NOTICE, MEMPOOL, "Status: Available=%u, In_use=%u (%.1f%%)\n", avail,
          in_use, use_percent);
}

inline uint64_t get_now_micros() {
  using Clock = std::chrono::high_resolution_clock;
  return std::chrono::duration_cast<std::chrono::microseconds>(
             Clock::now().time_since_epoch())
      .count();
}

inline void PrintHexData(const void *data, size_t size) {
  unsigned char *byte_data = (unsigned char *)data;
  for (size_t i = 0; i < size; ++i) {
    printf("%02x ", byte_data[i]);
    if ((i + 1) % 16 == 0) {
      printf("  ");
      for (size_t j = i - 15; j <= i; ++j) {
        printf("%c", (byte_data[j] >= 32 && byte_data[j] <= 126) ? byte_data[j]
                                                                 : '.');
      }
      printf("\n");
    }
  }
  if (size % 16 != 0) {
    size_t remaining = size % 16;
    for (size_t i = 0; i < (16 - remaining); ++i) {
      printf("   ");
    }
    printf("  ");
    for (size_t i = size - remaining; i < size; ++i) {
      printf("%c",
             (byte_data[i] >= 32 && byte_data[i] <= 126) ? byte_data[i] : '.');
    }
    printf("\n");
  }
  printf("\n");
}

inline void ReverseRTE_IPV4(rte_be32_t ip, std::string &result) {
  uint8_t a = (ip >> 24) & 0xFF;
  uint8_t b = (ip >> 16) & 0xFF;
  uint8_t c = (ip >> 8) & 0xFF;
  uint8_t d = ip & 0xFF;

  result = std::to_string(d) + "." + std::to_string(c) + "." +
           std::to_string(b) + "." + std::to_string(a);
}

inline std::string ReverseRTE_IPV4(rte_be32_t ip) {
  std::string result;
  ReverseRTE_IPV4(ip, result);
  return result;
}

static inline void SwapIpv4(struct iphdr *ip_hdr) {
  uint32_t tmp_ip = ip_hdr->saddr;
  ip_hdr->saddr = ip_hdr->daddr;
  ip_hdr->daddr = tmp_ip;
}

inline void ParseMac(const std::string &mac_str,
                     std::array<uint8_t, ETH_ALEN> &mac_bytes) {
  std::istringstream iss(mac_str);
  std::string byte_str;
  int i = 0;
  while (std::getline(iss, byte_str, ':') && i < 6) {
    mac_bytes[i++] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
  }
}

inline std::array<uint8_t, ETH_ALEN> ParseMac(const std::string &mac_str) {
  std::array<uint8_t, ETH_ALEN> mac_bytes{};
  ParseMac(mac_str, mac_bytes);
  return mac_bytes;
}

static inline void exponentialBackoff(int attempt) {
  int wait_ms = std::min(500 * (1 << (attempt - 1)), 4000);
  if (attempt == 0) wait_ms = 0;

  std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
}

static inline uint32_t generate_request_id() {
  static std::atomic<uint32_t> counter{0};
  static std::random_device rd;
  static std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis(0, UINT32_MAX);

  return htonl(dis(gen) ^ counter.fetch_add(1, std::memory_order_relaxed));
}

static inline std::vector<uint> SampleIndices(uint base, uint limit,
                                              size_t sample_size) {
  std::vector<uint> indices(limit - base);
  indices.reserve(sample_size);
  std::iota(indices.begin(), indices.end(), base);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::shuffle(indices.begin(), indices.end(), gen);

  if (sample_size > indices.size()) {
    sample_size = indices.size();
  }

  return std::vector<uint>(indices.begin(), indices.begin() + sample_size);
}

}  // namespace utils
