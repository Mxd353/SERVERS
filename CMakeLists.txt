cmake_minimum_required(VERSION 4.0)

project(ClusterRun
    VERSION 1.0
    DESCRIPTION "Cluter Run"
    LANGUAGES CXX
)

add_subdirectory(lib/arp)

set(HOME_DIR $ENV{HOME})
message(STATUS "HOME_DIR = ${HOME_DIR}")

set(CMAKE_C_COMPILER "${HOME_DIR}/gcc/bin/gcc")
if(EXISTS "${HOME_DIR}/gcc/bin/g++")
    set(CMAKE_CXX_COMPILER "${HOME_DIR}/gcc/bin/g++")
else()
    message(FATAL_ERROR "Custom g++ not found at ${HOME_DIR}/gcc/bin/g++")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

message(STATUS "Using C++ standard: ${CMAKE_CXX_STANDARD}")

add_definitions(-DRTE_USE_C11_MEM_MODEL)

add_compile_options(
    -g
    -Wall
    -Wextra
    -Werror
)

file(GLOB SOURCE_FILES
    "src/*.cc"
)
message(STATUS "Found source files: ${SOURCE_FILES}")

set(TARGET_NAME run)

add_executable(${TARGET_NAME}
    ${SOURCE_FILES}
)

target_compile_definitions(${TARGET_NAME}
    PRIVATE
        RTE_LIBRTE_ETHDEV_DEBUG
)

include_directories(
    include
)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
if(Python3_FOUND)
    message(STATUS "Found Python3")
    message(STATUS "Python3 include dirs: ${Python3_INCLUDE_DIRS}")
    message(STATUS "Python3 libraries: ${Python3_LIBRARIES}")
else()
    message(FATAL_ERROR "Python3 not found")
endif()

cmake_policy(SET CMP0167 NEW)

find_package(Threads REQUIRED)
if(Threads_FOUND)
    message(STATUS "Threads library: ${CMAKE_THREAD_LIBS_INIT}")
endif()

set(BOOST_ROOT "${HOME_DIR}/lib/boost")
set(Boost_NO_SYSTEM_PATHS ON)
find_package(Boost REQUIRED COMPONENTS system)
if(Boost_FOUND)
    message(STATUS "Boost found: ${Boost_VERSION}")
    message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
endif()

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

set(DPDK_PATH ${HOME_DIR}/lib/dpdk)
set(DPDK_LIB_DIR "${DPDK_PATH}/lib/x86_64-linux-gnu")
set(ENV{PKG_CONFIG_PATH} "${DPDK_LIB_DIR}/pkgconfig:$ENV{PKG_CONFIG_PATH}")
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED libdpdk)
if(DPDK_FOUND)
    message(STATUS "DPDK found: ${DPDK_VERSION}")
    message(STATUS "DPDK include dir: ${DPDK_INCLUDE_DIRS}")
    message(STATUS "DPDK library dir: ${DPDK_LIBRARY_DIRS}")
endif()

set(LIBUV_PATH ${HOME_DIR}/lib/libuv)
message(STATUS "LIBUV_PATH: ${LIBUV_PATH}")
find_library(LIBUV_LIB libuv uv uv1
    PATHS ${LIBUV_PATH}/lib
    NO_DEFAULT_PATH
)
if(NOT LIBUV_LIB)
    message(FATAL_ERROR "Could not find libuv library in: ${LIBUV_PATH}/lib")
endif()
message(STATUS "Using LIBUV include dir: ${LIBUV_PATH}")
message(STATUS "Using libuv library: ${LIBUV_LIB}")

find_path(HIREDIS_HEADER hiredis)
find_library(HIREDIS_LIB hiredis)
if(NOT HIREDIS_LIB)
    message(FATAL_ERROR "Could not find hiredis library")
endif()
message(STATUS "Found hiredis lib: ${HIREDIS_LIB}")

set(REDIS_PLUS_PLUS_PATH ${HOME_DIR}/lib/redis++)
find_library(REDIS_PLUS_PLUS_LIB redis++ PATHS ${REDIS_PLUS_PLUS_PATH}/lib NO_DEFAULT_PATH)
if(NOT REDIS_PLUS_PLUS_LIB)
    message(FATAL_ERROR "Could not find redis++ library in: ${REDIS_PLUS_PLUS_PATH}/lib")
endif()
message(STATUS "Found redis++ lib: ${REDIS_PLUS_PLUS_LIB}")

target_include_directories(${TARGET_NAME}
    PUBLIC
        ${BOOST_ROOT}/include
        ${LIBUV_PATH}/include
        ${DPDK_PATH}/include
        ${HIREDIS_HEADER}
        ${REDIS_PLUS_PLUS_PATH}/include
        include
)

target_link_directories(${TARGET_NAME}
    PUBLIC
        ${DPDK_LIB_DIR}
)

target_link_libraries(${TARGET_NAME}
    PUBLIC
        Boost::system
        Threads::Threads
        OpenSSL::Crypto
        ZLIB::ZLIB
        Python3::Python
        ${LIBUV_LIB}
        ${HIREDIS_LIB}
        ${REDIS_PLUS_PLUS_LIB}
        ${DPDK_LIBRARIES}
)

add_dependencies(${TARGET_NAME} arp_py)

target_compile_options(${TARGET_NAME}
    PUBLIC
        ${DPDK_CFLAGS_OTHER}
)

file(GLOB_RECURSE CONF_FILES "${CMAKE_SOURCE_DIR}/conf/*")
foreach(conf_file ${CONF_FILES})
    get_filename_component(conf_name ${conf_file} NAME)
    configure_file(${conf_file} ${CMAKE_CURRENT_BINARY_DIR}/conf/${conf_name} COPYONLY)
endforeach()

install(TARGETS ${TARGET_NAME} DESTINATION bin)
install(DIRECTORY lib/ DESTINATION lib)

set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY CLEAN_NO_CUSTOM 1)
