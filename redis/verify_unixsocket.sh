#!/bin/bash
# Verify Unix Socket configuration

echo "=== Redis Unix Socket Verification ==="
echo ""

# Check if Redis instances are running
echo "1. Checking running Redis instances..."
redis_pids=$(pgrep -c redis-server)
echo "   Found $redis_pids redis-server processes"
echo ""

# Check Unix Socket files
echo "2. Checking Unix Socket files in /tmp..."
socket_count=$(ls -1 /tmp/redis.* 2>/dev/null | wc -l)
echo "   Found $socket_count Unix Socket files"
if [ $socket_count -gt 0 ]; then
    echo "   First 5 sockets:"
    ls -1 /tmp/redis.* 2>/dev/null | head -5
fi
echo ""

# Check socket permissions
echo "3. Checking socket permissions..."
if [ -S /tmp/redis.0 ]; then
    perm=$(stat -c "%a %n" /tmp/redis.0 2>/dev/null)
    echo "   redis.0: $perm"
    if [[ $perm == 700* ]]; then
        echo "   ✓ Correct permission (700)"
    else
        echo "   ⚠ Permission should be 700"
    fi
fi
echo ""

# Test connectivity via Unix Socket
echo "4. Testing Unix Socket connectivity..."
if [ -S /tmp/redis.0 ]; then
    result=$(redis-cli -s /tmp/redis.0 PING 2>&1)
    if [ "$result" == "PONG" ]; then
        echo "   ✓ redis.0 responds to PING"
    else
        echo "   ✗ redis.0 connection failed: $result"
    fi
else
    echo "   ⚠ redis.0 socket not found"
fi
echo ""

# Check kernel parameters
echo "5. Kernel parameters for Unix Socket..."
echo "   net.unix.max_dgram_qlen = $(sysctl -n net.unix.max_dgram_qlen 2>/dev/null || echo 'N/A')"
echo "   fs.file-max = $(sysctl -n fs.file-max 2>/dev/null || echo 'N/A')"
echo ""

echo "=== Verification Complete ==="
