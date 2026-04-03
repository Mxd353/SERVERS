cmake_minimum_required(VERSION 4.0)

# =============================================================================
# 项目配置
# =============================================================================
project(ClusterRun
    VERSION 1.0.0
    DESCRIPTION "Server Cluster with DPDK and Redis Cache Migration"
    LANGUAGES CXX
)

enable_language(C)

# 允许从命令行指定 C++ 标准
cmake_policy(SET CMP0167 NEW)
cmake_policy(SET CMP0144 NEW)  # find_package uses upper-case <PACKAGENAME>_ROOT variables

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# =============================================================================
# 用户可配置选项
# =============================================================================
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(ENABLE_OPTIMIZATION "Enable O3 optimization" OFF)
option(ENABLE_DEBUG "Enable debug symbols" ON)
option(ENABLE_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)
option(USE_CUSTOM_GCC "Use custom GCC from HOME_DIR/gcc" OFF)

# 依赖库路径配置 (可通过 -D 参数覆盖)
set(BOOST_ROOT "$ENV{HOME}/lib/boost" CACHE PATH "Boost installation prefix")
set(OPENSSL_ROOT "" CACHE PATH "OpenSSL installation prefix (optional)")
set(TBB_ROOT "${CMAKE_PREFIX_PATH}/tbb" CACHE PATH "TBB installation prefix")
set(DPDK_ROOT "$ENV{HOME}/lib/dpdk" CACHE PATH "DPDK installation prefix")
set(GCC_CUSTOM_ROOT "$ENV{HOME}/gcc" CACHE PATH "Custom GCC installation prefix")

# =============================================================================
# 编译器配置
# =============================================================================
if(USE_CUSTOM_GCC)
    if(EXISTS "${GCC_CUSTOM_ROOT}/bin/gcc" AND EXISTS "${GCC_CUSTOM_ROOT}/bin/g++")
        set(CMAKE_C_COMPILER "${GCC_CUSTOM_ROOT}/bin/gcc")
        set(CMAKE_CXX_COMPILER "${GCC_CUSTOM_ROOT}/bin/g++")
        message(STATUS "Using custom GCC from: ${GCC_CUSTOM_ROOT}")
    else()
        message(FATAL_ERROR "Custom GCC not found at ${GCC_CUSTOM_ROOT}")
    endif()
endif()

message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "CXX Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "GCC C Compiler version: ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "GCC C++ Compiler version: ${CMAKE_CXX_COMPILER_VERSION}")

# =============================================================================
# 编译选项配置
# =============================================================================
add_definitions(-DRTE_USE_C11_MEM_MODEL)

set(COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
)

if(ENABLE_WARNINGS_AS_ERRORS)
    list(APPEND COMMON_COMPILE_OPTIONS -Werror)
endif()

if(ENABLE_OPTIMIZATION)
    list(APPEND COMMON_COMPILE_OPTIONS
        -O3
        -march=native
        -funroll-loops
        -fomit-frame-pointer
    )
    message(STATUS "Optimization: O3 enabled")
else()
    list(APPEND COMMON_COMPILE_OPTIONS -O2)
    message(STATUS "Optimization: O2 (default)")
endif()

if(ENABLE_DEBUG)
    list(APPEND COMMON_COMPILE_OPTIONS -g)
    message(STATUS "Debug symbols: enabled")
endif()

# =============================================================================
# ARP 子模块
# =============================================================================
add_subdirectory(lib/arp)
add_compile_definitions(ARP_MODULE_PATH="${ARP_MODULE_PATH}")

# =============================================================================
# 查找依赖库
# =============================================================================

# Python3
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
message(STATUS "Python3: ${Python3_VERSION}")
message(STATUS "Python3 include: ${Python3_INCLUDE_DIRS}")

# Threads
find_package(Threads REQUIRED)
message(STATUS "Threads: ${CMAKE_THREAD_LIBS_INIT}")

# OpenSSL (Boost.Asio SSL 需要)
if(EXISTS "${OPENSSL_ROOT}")
    set(OpenSSL_ROOT ${OPENSSL_ROOT})
    message(STATUS "Using custom OpenSSL from: ${OPENSSL_ROOT}")
endif()
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL version: ${OPENSSL_VERSION}")

# Boost
if(EXISTS "${BOOST_ROOT}")
    set(Boost_ROOT ${BOOST_ROOT})
    set(Boost_NO_SYSTEM_PATHS ON)
    message(STATUS "Using custom Boost from: ${BOOST_ROOT}")
endif()
find_package(Boost 1.90.0 REQUIRED)
message(STATUS "Boost version: ${Boost_VERSION}")
message(STATUS "Boost include: ${Boost_INCLUDE_DIRS}")

# TBB
if(EXISTS "${TBB_ROOT}")
    list(APPEND CMAKE_PREFIX_PATH ${TBB_ROOT})
    message(STATUS "Using custom TBB from: ${TBB_ROOT}")
endif()
find_package(TBB REQUIRED)
message(STATUS "TBB version: ${TBB_VERSION}")

# DPDK (通过 pkg-config)
find_package(PkgConfig REQUIRED)
if(EXISTS "${DPDK_ROOT}/lib/x86_64-linux-gnu/pkgconfig")
    set(ENV{PKG_CONFIG_PATH} "${DPDK_ROOT}/lib/x86_64-linux-gnu/pkgconfig:$ENV{PKG_CONFIG_PATH}")
    message(STATUS "Using custom DPDK from: ${DPDK_ROOT}")
endif()
pkg_check_modules(DPDK REQUIRED libdpdk>=21.0)
message(STATUS "DPDK version: ${DPDK_VERSION}")
message(STATUS "DPDK include: ${DPDK_INCLUDE_DIRS}")

# =============================================================================
# 源文件配置
# =============================================================================
set(SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
file(GLOB_RECURSE SOURCE_FILES "${SOURCE_DIR}/*.cc")
message(STATUS "Source files: ${SOURCE_FILES}")

set(TARGET_NAME run)

add_executable(${TARGET_NAME} ${SOURCE_FILES})

# =============================================================================
# 编译定义
# =============================================================================
target_compile_definitions(${TARGET_NAME}
    PRIVATE
        BOOST_REDIS_SEPARATE_COMPILATION
        $<$<CONFIG:Debug>:RTE_LIBRTE_ETHDEV_DEBUG>
)

# =============================================================================
# 头文件目录
# =============================================================================
target_include_directories(${TARGET_NAME}
    PUBLIC
        ${Boost_INCLUDE_DIRS}
        ${DPDK_INCLUDE_DIRS}
        ${Python3_INCLUDE_DIRS}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# =============================================================================
# 链接库
# =============================================================================
target_link_directories(${TARGET_NAME}
    PUBLIC
        ${DPDK_LIBRARY_DIRS}
)

target_link_libraries(${TARGET_NAME}
    PUBLIC
        Boost::headers
        OpenSSL::SSL
        OpenSSL::Crypto
        TBB::tbb
        Threads::Threads
        Python3::Python
        ${DPDK_LIBRARIES}
)

# =============================================================================
# 编译选项
# =============================================================================
target_compile_options(${TARGET_NAME}
    PUBLIC
        ${COMMON_COMPILE_OPTIONS}
        ${DPDK_CFLAGS_OTHER}
)

# 依赖关系
add_dependencies(${TARGET_NAME} arp_py)

# =============================================================================
# 配置文件复制
# =============================================================================
file(GLOB_RECURSE CONF_FILES "${CMAKE_SOURCE_DIR}/conf/*")
foreach(conf_file ${CONF_FILES})
    get_filename_component(conf_name ${conf_file} NAME)
    configure_file(${conf_file} ${CMAKE_CURRENT_BINARY_DIR}/conf/${conf_name} COPYONLY)
endforeach()

# =============================================================================
# 安装配置
# =============================================================================
include(GNUInstallDirs)

install(TARGETS ${TARGET_NAME}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/lib/
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/include/
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/conf/
    DESTINATION ${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}/conf
)

# =============================================================================
# 摘要输出
# =============================================================================
message(STATUS "")
message(STATUS "=== ${PROJECT_NAME} Configuration Summary ===")
message(STATUS "  Version:          ${PROJECT_VERSION}")
message(STATUS "  Build type:       ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard:     ${CMAKE_CXX_STANDARD}")
message(STATUS "  Install prefix:   ${CMAKE_INSTALL_PREFIX}")
message(STATUS "  Boost:            ${Boost_VERSION}")
message(STATUS "  OpenSSL:          ${OPENSSL_VERSION}")
message(STATUS "  TBB:              ${TBB_VERSION}")
message(STATUS "  DPDK:             ${DPDK_VERSION}")
message(STATUS "  Python3:          ${Python3_VERSION}")
message(STATUS "===========================================")
message(STATUS "")
