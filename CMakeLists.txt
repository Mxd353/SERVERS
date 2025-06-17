cmake_minimum_required(VERSION 4.0)

project(ClusterRun
    VERSION 1.0
    DESCRIPTION "Cluter Run"
    LANGUAGES CXX
)

add_subdirectory(lib/arp)

set(HOME_DIR $ENV{HOME})
message(STATUS "HOME_DIR = ${HOME_DIR}")

if(EXISTS "${HOME_DIR}/gcc/bin/gcc" AND EXISTS "${HOME_DIR}/gcc/bin/g++")
    set(CMAKE_C_COMPILER "${HOME_DIR}/gcc/bin/gcc")
    set(CMAKE_CXX_COMPILER "${HOME_DIR}/gcc/bin/g++")
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

if(EXISTS "${HOME_DIR}/lib/boost")
    set(BOOST_ROOT "${HOME_DIR}/lib/boost")
    set(Boost_NO_SYSTEM_PATHS ON)
endif()
find_package(Boost REQUIRED COMPONENTS system)
if(Boost_FOUND)
    message(STATUS "Boost found: ${Boost_VERSION}")
    message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
else()
    message(FATAL_ERROR "Boost not found!")
endif()

if(EXISTS "${HOME_DIR}/lib/tbb")
    set(CMAKE_PREFIX_PATH "${HOME_DIR}/lib/tbb" ${CMAKE_PREFIX_PATH})
endif()
find_package(TBB REQUIRED)
if(TBB_FOUND)
    message(STATUS "TBB found: ${TBB_VERSION}")

else()
    message(FATAL_ERROR "TBB not found!")
endif()

find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

if(EXISTS "${HOME_DIR}/lib/dpdk")
    set(DPDK_PATH "${HOME_DIR}/lib/dpdk")
    set(DPDK_LIB_DIR "${DPDK_PATH}/lib/x86_64-linux-gnu")
    set(ENV{PKG_CONFIG_PATH} "${DPDK_LIB_DIR}/pkgconfig:$ENV{PKG_CONFIG_PATH}")
endif()
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED libdpdk)
include_directories(${DPDK_INCLUDE_DIRS})
link_directories(${DPDK_LIBRARY_DIRS})
if(DPDK_FOUND)
    message(STATUS "DPDK_INCLUDE_DIRS: ${DPDK_INCLUDE_DIRS}")
    message(STATUS "DPDK_LIBRARY_DIRS: ${DPDK_LIBRARY_DIRS}")
    message(STATUS "DPDK_LIBRARIES: ${DPDK_LIBRARIES}")
else()
    message(FATAL_ERROR "DPDK not found. Make sure pkg-config can find libdpdk.pc.")
endif()

if(EXISTS "${HOME_DIR}/lib/libuv")
    set(LIBUV_PATH ${HOME_DIR}/lib/libuv)
    message(STATUS "LIBUV_PATH: ${LIBUV_PATH}")

    find_library(LIBUV_LIB NAMES uv uv1 libuv
        PATHS ${LIBUV_PATH}/lib
        NO_DEFAULT_PATH
    )
    find_path(LIBUV_INCLUDE_DIR uv.h
        PATHS ${LIBUV_PATH}/include
        NO_DEFAULT_PATH
    )
else()
    message(STATUS "Using system libuv library")

    find_library(LIBUV_LIB NAMES uv uv1 libuv)
    find_path(LIBUV_INCLUDE_DIR uv.h)

    if(NOT LIBUV_LIB OR NOT LIBUV_INCLUDE_DIR)
        message(FATAL_ERROR "Could not find system libuv. Please install libuv or set LIBUV_PATH.")
    endif()
endif()

message(STATUS "Using LIBUV include dir: ${LIBUV_INCLUDE_DIR}")
message(STATUS "Using libuv library: ${LIBUV_LIB}")

find_path(HIREDIS_HEADER hiredis)
find_library(HIREDIS_LIB hiredis)
if(NOT HIREDIS_LIB)
    message(FATAL_ERROR "Could not find hiredis library")
else()
    message(STATUS "Found hiredis lib: ${HIREDIS_LIB}")
endif()


if(EXISTS "${HOME_DIR}/lib/redis++")
    set(REDIS_PLUS_PLUS_PATH ${HOME_DIR}/lib/redis++)
    find_path(REDIS_PLUS_PLUS_HEADER sw
        PATHS ${REDIS_PLUS_PLUS_PATH}/include
        NO_DEFAULT_PATH
    )
    find_library(REDIS_PLUS_PLUS_LIB redis++ PATHS ${REDIS_PLUS_PLUS_PATH}/lib NO_DEFAULT_PATH)
else()
    find_path(REDIS_PLUS_PLUS_HEADER sw)
    find_library(REDIS_PLUS_PLUS_LIB redis++)
endif()

if(NOT REDIS_PLUS_PLUS_HEADER OR NOT REDIS_PLUS_PLUS_LIB)
    message(FATAL_ERROR "Could not find Redis++ library or headers")
else()
    message(STATUS "Using redis++ include dir: ${REDIS_PLUS_PLUS_HEADER}")
    message(STATUS "Using redis++ lib: ${REDIS_PLUS_PLUS_LIB}")
endif()

target_include_directories(${TARGET_NAME}
    PUBLIC
        ${Boost_INCLUDE_DIRS}
        ${LIBUV_INCLUDE_DIR}
        ${DPDK_INCLUDE_DIRS}
        ${HIREDIS_HEADER}
        ${REDIS_PLUS_PLUS_HEADER}
        include
        ${CMAKE_SOURCE_DIR}
)

target_link_directories(${TARGET_NAME}
    PUBLIC
        ${DPDK_LIB_DIR}
)

target_link_libraries(${TARGET_NAME}
    PUBLIC
        Boost::system
        TBB::tbb
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
