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

# Run (requires sudo for DPDK)
./run.sh

# Run with debugging (gdb)
sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" gdb ./build/run

# Run with perf profiling
sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" perf record -F 99 -g -- ./build/run
```

## Dependencies

- GCC 11+ (custom install at `$HOME/gcc/`)
- Boost (at `$HOME/lib/boost/`)
- DPDK (at `$HOME/lib/dpdk/`)
- TBB (Intel Threading Building Blocks)
- OpenSSL
- Python3

## Testing

No test framework currently configured. Manual testing via:
```bash
./run.sh  # Executes the server cluster
```

## Code Style Guidelines

### Header Files
- Use `#pragma once` for header guards (NOT `#ifndef`/`#define`/`#endif`)
- Group includes: C system → C++ standard → third-party → project headers
- Use `inline` for header-only utility functions

### Naming Conventions
- **Classes**: PascalCase (`ServerCluster`, `DPDKHandler`)
- **Functions/Methods**: PascalCase (`HandlePacket`, `InitServers`)
- **Member variables**: snake_case with trailing underscore (`server_info_`, `sockfd_`)
- **Local variables**: snake_case (`rack_idx`, `sock_config`)
- **Constants/Macros**: UPPER_SNAKE_CASE (`RX_NUM_MBUFS`, `UDP_PORT_KV`)
- **`constexpr` variables**: UPPER_SNAKE_CASE or mixed case accepted
- **Namespaces**: snake_case (`c_m_proto`, `utils`)

### Types and Modern C++
- Use `auto` for return type syntax: `auto func() -> ReturnType`
- Use `[[nodiscard]]` for methods returning important values
- Prefer `std::make_unique`/`std::make_shared` over raw `new`
- Use `std::atomic` for thread-safe primitives
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
```
