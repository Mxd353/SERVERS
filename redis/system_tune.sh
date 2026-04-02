#!/bin/bash
# System tuning for 1024 Redis instances

echo "=== System Tuning for Redis ==="

# 1. Memory overcommit (required for Redis)
echo "1. Setting vm.overcommit_memory=1..."
sudo sysctl vm.overcommit_memory=1
echo "   vm.overcommit_memory = $(sysctl -n vm.overcommit_memory)"

# 2. Increase file descriptor limits
echo ""
echo "2. Setting ulimit -n..."
# Current limit
echo "   Current limit: $(ulimit -n)"

# Set limit for current session
ulimit -n 2097152
echo "   New limit: $(ulimit -n)"

# 3. Unix socket queue size
echo ""
echo "3. Setting Unix socket queue size..."
sudo sysctl net.unix.max_dgram_qlen=4096
echo "   net.unix.max_dgram_qlen = $(sysctl -n net.unix.max_dgram_qlen)"

# 4. TCP settings (even though we use Unix Socket, for completeness)
echo ""
echo "4. TCP performance settings..."
sudo sysctl net.core.somaxconn=65535
sudo sysctl net.ipv4.tcp_max_syn_backlog=65535
echo "   net.core.somaxconn = $(sysctl -n net.core.somaxconn)"

# 5. Transparent Huge Pages (disable for Redis)
echo ""
echo "5. Disabling Transparent Huge Pages..."
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
    echo "   THP status: $(cat /sys/kernel/mm/transparent_hugepage/enabled | grep -o '\[never\]' || echo 'warning: not disabled')"
else
    echo "   THP not available (may be in VM)"
fi

echo ""
echo "=== Tuning Complete ==="
echo ""
echo "To make changes permanent, add to /etc/sysctl.conf:"
echo "  vm.overcommit_memory = 1"
echo "  net.unix.max_dgram_qlen = 4096"
echo "  net.core.somaxconn = 65535"
echo "  net.ipv4.tcp_max_syn_backlog = 65535"
echo ""
echo "And to /etc/security/limits.conf:"
echo "  * soft nofile 2097152"
echo "  * hard nofile 2097152"
