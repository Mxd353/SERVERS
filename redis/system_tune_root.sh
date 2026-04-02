#!/bin/bash
# System tuning for 1024 Redis instances (run with sudo)

echo "=== System Tuning for Redis ==="

# 1. Memory overcommit (REQUIRED for Redis)
echo "1. Setting vm.overcommit_memory=1..."
sysctl vm.overcommit_memory=1
echo "   vm.overcommit_memory = $(sysctl -n vm.overcommit_memory)"

# 2. Unix socket queue size
echo ""
echo "2. Setting Unix socket queue size..."
sysctl net.unix.max_dgram_qlen=4096
echo "   net.unix.max_dgram_qlen = $(sysctl -n net.unix.max_dgram_qlen)"

# 3. TCP settings
echo ""
echo "3. TCP performance settings..."
sysctl net.core.somaxconn=65535
echo "   net.core.somaxconn = $(sysctl -n net.core.somaxconn)"

# 4. Transparent Huge Pages (disable for Redis)
echo ""
echo "4. Disabling Transparent Huge Pages..."
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo "   THP disabled"
fi

echo ""
echo "=== Tuning Complete ==="
