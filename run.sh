#!/bin/bash

export GCC_HOME="$HOME/gcc"
export BOOST_HOME="$HOME/lib/boost"
export RTE_SDK="$HOME/lib/dpdk"


if [[ ! -d "$GCC_HOME/lib64" ]]; then
    echo "[ERROR] GCC 库路径不存在: $GCC_HOME/lib64"
    exit 1
fi

if [[ ! -d "$BOOST_HOME/lib" ]]; then
    echo "[ERROR] Boost 库路径不存在: $BOOST_HOME/lib"
    exit 1
fi

export LD_LIBRARY_PATH="$GCC_HOME/lib64:$BOOST_HOME/lib:$RTE_SDK/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH"

echo "[INFO] 使用的 LD_LIBRARY_PATH:"
echo "       $LD_LIBRARY_PATH"

if [[ ! -f "./build/run" ]]; then
    echo "[ERROR] 找不到可执行文件 ./build/run"
    exit 1
fi

export DISPLAY=:0

sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" ./build/run
# sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" gdb ./build/run
# sudo env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" perf record -F 99 -g -- ./build/run
