#!/bin/bash
# Quick test for Redis Unix Socket benchmark

echo "=== Quick Redis Benchmark Test ==="
echo ""

# Test single instance
echo "Testing instance 0..."
redis-benchmark -s /tmp/redis.0 -t set,get -n 1000 -c 4 -d 16 -q --threads 1 2>&1 | tail -5

echo ""
echo "Testing instance 100..."
redis-benchmark -s /tmp/redis.100 -t set,get -n 1000 -c 4 -d 16 -q --threads 1 2>&1 | tail -5

echo ""
echo "Testing instance 500..."
redis-benchmark -s /tmp/redis.500 -t set,get -n 1000 -c 4 -d 16 -q --threads 1 2>&1 | tail -5

echo ""
echo "Testing instance 1023..."
redis-benchmark -s /tmp/redis.1023 -t set,get -n 1000 -c 4 -d 16 -q --threads 1 2>&1 | tail -5

echo ""
echo "=== Test Complete ==="
