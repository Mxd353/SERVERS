#pragma once

#include <netinet/ip.h>
#include <linux/if_ether.h>
#include <rte_byteorder.h>

#include <array>
#include <iostream>
#include <sstream>
#include <string>

namespace utils {
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

inline void parse_mac(const std::string &mac_str,
                      std::array<uint8_t, ETH_ALEN> &mac_bytes) {
  std::istringstream iss(mac_str);
  std::string byte_str;
  int i = 0;
  while (std::getline(iss, byte_str, ':') && i < 6) {
    mac_bytes[i++] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
  }
}

inline std::array<uint8_t, ETH_ALEN> parse_mac(const std::string &mac_str) {
  std::array<uint8_t, ETH_ALEN> mac_bytes{};
  parse_mac(mac_str, mac_bytes);
  return mac_bytes;
}

}  // namespace utils
