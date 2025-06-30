#pragma once

#include <rte_byteorder.h>

#include <iostream>

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

inline std::string ReverseRTE_IPV4(rte_be32_t ip) {
  uint8_t a = (ip >> 24) & 0xFF;
  uint8_t b = (ip >> 16) & 0xFF;
  uint8_t c = (ip >> 8) & 0xFF;
  uint8_t d = ip & 0xFF;

  std::string result = std::to_string(d) + "." + std::to_string(c) + "." +
                       std::to_string(b) + "." + std::to_string(a);
  return result;
}

inline void ReverseRTE_IPV4(rte_be32_t ip, std::string &result) {
  uint8_t a = (ip >> 24) & 0xFF;
  uint8_t b = (ip >> 16) & 0xFF;
  uint8_t c = (ip >> 8) & 0xFF;
  uint8_t d = ip & 0xFF;

  result = std::to_string(d) + "." + std::to_string(c) + "." +
           std::to_string(b) + "." + std::to_string(a);
}

}  // namespace utils
