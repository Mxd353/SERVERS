# AGENTS.md - CacheMigration SERVERS

## Project Overview

C++17 server cluster application using DPDK for high-performance network packet processing and Boost.Redis for cache migration. CMake-based build system targeting Linux.

## Build Commands

```bash
# Configure and build
mkdir -p build && cd build
cmake ..
make -j$(nproc)

# Clean build
rm -rf build && mkdir build && cd build && cmake .. && make -j$(nproc)

# Build with custom paths
cmake .. -DBOOST_ROOT=/home/cc/lib/boost -DTBB_ROOT=/path/to/tbb

# Build with optimizations
cmake .. -DENABLE_OPTIMIZATION=ON

# Run (requires sudo for DPDK)
./run.sh

# Run with debugging (gdb)
sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" gdb ./build/run

# Run with perf profiling
sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" perf record -F 99 -g -- ./build/run
```

## Dependencies

- GCC 11+ (custom install at `$HOME/gcc/`)
- Boost 1.90.0+ (at `$HOME/lib/boost/`)
- DPDK 21.0+ (at `$HOME/lib/dpdk/`)
- TBB (Intel Threading Building Blocks)
- OpenSSL (required by Boost.Asio SSL)
- Python3 (for ARP module)

## Testing

No test framework currently configured. Manual testing via:
```bash
./run.sh  # Executes the server cluster
```

To add unit tests, consider integrating GoogleTest or Catch2 with CMake.

## Code Style Guidelines

### Header Files
- Use `#pragma once` for header guards (NOT `#ifndef`/`#define`/`#endif`)
- Group includes in order:
  1. C system headers (`<linux/if_packet.h>`, `<netinet/ip.h>`)
  2. C++ standard headers (`<atomic>`, `<memory>`, `<string>`)
  3. Third-party headers (`<boost/asio.hpp>`, `<rte_eal.h>`)
  4. Project headers (`"server_instance.h"`, `"lib/utils.h"`)
- Use `inline` for header-only utility functions

### Naming Conventions
- **Classes/Structs**: PascalCase (`ServerCluster`, `DPDKHandler`, `ControllerInfo`)
- **Functions/Methods**: PascalCase (`HandlePacket`, `InitServers`, `GetIp`)
- **Member variables**: snake_case with trailing underscore (`server_info_`, `sockfd_`)
- **Local variables**: snake_case (`rack_idx`, `sock_config`)
- **Constants/Macros**: UPPER_SNAKE_CASE (`RX_NUM_MBUFS`, `UDP_PORT_KV`)
- **`constexpr` variables**: UPPER_SNAKE_CASE or mixed case accepted
- **Namespaces**: snake_case (`c_m_proto`, `utils`)
- **Type aliases**: PascalCase with trailing `_t` avoided (`using ServerMap = ...`)

### Types and Modern C++
- Use `auto` for return type syntax: `auto func() -> ReturnType`
- Use `[[nodiscard]]` for methods returning important values
- Prefer `std::make_unique`/`std::make_shared` over raw `new`
- Use `std::atomic` for thread-safe primitives with explicit memory orders
- Use `std::shared_mutex` for reader-writer locks
- Use `std::array` over C arrays for fixed-size collections

### Error Handling
- Use `throw std::runtime_error(...)` for critical failures
- Use `std::cerr` for non-fatal error logging
- Use `perror(...)` for system call errors
- Return `bool` for success/failure when continuing is possible

### Formatting
- 2-space indentation
- Opening brace on same line for functions/classes
- Trailing return type syntax: `auto func() -> type`
- Use `(void)param` to explicitly mark unused parameters

### Imports Example
```cpp
// C system headers
#include <linux/if_packet.h>
#include <netinet/ip.h>

// C++ standard headers
#include <atomic>
#include <memory>
#include <string>

// Third-party headers
#include <boost/asio.hpp>
#include <rte_eal.h>

// Project headers
#include "server_instance.h"
#include "lib/utils.h"
```

### Macros
- Use `constexpr` over `#define` where possible
- Use `#define` only for compile-time bit manipulation macros

## Architecture

- `src/run.cc` - Entry point, signal handling, cluster initialization
- `src/clusters.cc` - Server cluster management and packet receiving
- `src/dpdk_handler.cc` - DPDK initialization and packet processing
- `src/server_instance.cc` - Individual server instance logic
- `src/boost_redis.cc` - Boost.Redis integration
- `include/` - Public headers
- `lib/` - Utilities and protocol definitions
- `lib/arp/` - ARP handling with Python integration
- `conf/` - Configuration files

## Common Patterns

```cpp
// Thread-safe member access
auto GetServerByIp(rte_be32_t ip) -> std::shared_ptr<ServerInstance> {
  std::shared_lock lock(ip_map_mutex_);
  auto it = ip_to_server_.find(ip);
  return it != ip_to_server_.end() ? it->second : nullptr;
}

// Memory order for atomic operations
stop_receive_thread_.store(true, std::memory_order_relaxed);
auto val = counter.load(std::memory_order_relaxed);

// Post work to thread pool
boost::asio::post(worker_pool_, [server, packet]() mutable {
  server->HandlePacket(packet);
});

// Range-based for with index
for (auto rack_idx : utils::range(clusters_info_.size())) { ... }

// Make shared with const data
auto shared_clusters =
    std::make_shared<const std::vector<std::vector<std::string>>>(
        clusters_info_);
```

## Troubleshooting

**Link errors with OpenSSL**: Boost.Asio SSL requires OpenSSL. Ensure OpenSSL is installed and CMake finds it.

**DPDK not found**: Set `PKG_CONFIG_PATH` to include DPDK's pkgconfig directory:
```bash
export PKG_CONFIG_PATH=/home/cc/lib/dpdk/lib/x86_64-linux-gnu/pkgconfig:$PKG_CONFIG_PATH
```
