#!/usr/bin/env python3
"""
Redis benchmark runner for testing multiple Redis instances via Unix Socket.
"""

import subprocess
import re
import time
from pathlib import Path
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
INSTANCE_COUNT = 1024
SOCKET_DIR = Path('/tmp')
TOTAL_REQUESTS = 1000
CONCURRENCY = 10
DATA_SIZE = 16

# 使用脚本所在目录下的 benchmark_logs 目录
SCRIPT_DIR = Path(__file__).parent
TMP_DIR = SCRIPT_DIR / 'benchmark_logs'


def setup_logging():
    """Setup log directory and clean old logs."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    # Clean old log files
    for f in TMP_DIR.iterdir():
        if f.name.endswith('.log'):
            f.unlink()


def run_benchmark(instance_id: int) -> Tuple[int, bool]:
    """Run benchmark for a single Redis instance via Unix Socket."""
    log_file = TMP_DIR / f'redis_{instance_id}.log'
    socket_path = SOCKET_DIR / f'redis.{instance_id}'

    # Check if socket exists
    if not socket_path.exists():
        print(f'❌ Socket not found: {socket_path}')
        return instance_id, False

    cmd = [
        'redis-benchmark',
        '-s', str(socket_path),
        '-t', 'set,get',
        '-n', str(TOTAL_REQUESTS),
        '-c', str(CONCURRENCY),
        '-d', str(DATA_SIZE),
        '-q',
        '--threads', '1'  # Use single thread to reduce load
    ]

    try:
        with open(log_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, timeout=60)
            if result.returncode != 0:
                stderr = result.stderr.decode('utf-8', errors='ignore')[:200]
                print(f'❌ Benchmark failed for instance {instance_id} (exit code: {result.returncode}): {stderr}')
                return instance_id, False
        return instance_id, True
    except subprocess.TimeoutExpired:
        print(f'❌ Timeout benchmarking instance {instance_id}')
        return instance_id, False
    except Exception as e:
        print(f'❌ Error benchmarking instance {instance_id}: {e}')
        return instance_id, False


def parse_log_file(log_path: Path) -> Tuple[float, float]:
    """Parse benchmark log file and extract QPS values."""
    set_qps = 0.0
    get_qps = 0.0

    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Parse SET QPS - look for "SET: X.XX requests per second"
        set_match = re.search(r'SET:\s+([\d.]+)\s+requests per second', content)
        if set_match:
            set_qps = float(set_match.group(1))

        # Parse GET QPS - look for "GET: X.XX requests per second"
        get_match = re.search(r'GET:\s+([\d.]+)\s+requests per second', content)
        if get_match:
            get_qps = float(get_match.group(1))

    except (IOError, ValueError) as e:
        print(f'⚠️  Error parsing {log_path}: {e}')

    return set_qps, get_qps


def main():
    """Main entry point."""
    setup_logging()

    print(f'Launching benchmarks for {INSTANCE_COUNT} Redis instances via Unix Socket...')
    print(f'Socket path pattern: {SOCKET_DIR}/redis.<id>')
    print(f'Configuration: {TOTAL_REQUESTS} requests, {CONCURRENCY} concurrency, {DATA_SIZE} bytes')
    start_time = time.time()

    # Check available sockets
    available_sockets = list(SOCKET_DIR.glob('redis.*'))
    print(f'Found {len(available_sockets)} Redis sockets')
    
    if len(available_sockets) < INSTANCE_COUNT:
        print(f'⚠️  Warning: Only {len(available_sockets)} sockets found, expected {INSTANCE_COUNT}')

    # Run benchmarks in batches to avoid overwhelming the system
    completed = 0
    failed = 0
    valid_instances = 0
    total_set_qps = 0.0
    total_get_qps = 0.0
    
    MAX_PARALLEL = 16  # Limit parallel benchmarks
    BATCH_SIZE = 64    # Process in batches with delay between
    BATCH_DELAY = 0.5  # Seconds between batches
    
    # Process in batches
    for batch_start in range(0, INSTANCE_COUNT, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, INSTANCE_COUNT)
        print(f'\nProcessing batch {batch_start}-{batch_end-1}...')
        
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
            future_to_id = {
                executor.submit(run_benchmark, i): i
                for i in range(batch_start, batch_end)
            }

            for future in as_completed(future_to_id):
                instance_id, success = future.result()
                completed += 1
                if not success:
                    failed += 1
                
                # Print progress every 10 instances
                if completed % 10 == 0 or completed == INSTANCE_COUNT:
                    print(f'[{completed:4}/{INSTANCE_COUNT}] {failed} failed', end='\r')
        
        # Delay between batches to let system recover
        if batch_end < INSTANCE_COUNT:
            time.sleep(BATCH_DELAY)
    
    print()  # New line after progress
    end_time = time.time()
    print(f'Benchmarking completed in {end_time - start_time:.1f} seconds.')
    
    # Parse results
    summary_log_path = TMP_DIR / 'summary.log'
    total_set_ops = 0
    total_get_ops = 0
    valid_instances = 0
    total_set_qps = 0.0
    total_get_qps = 0.0

    with open(summary_log_path, 'w') as summary_file:
        summary_file.write('Instance Results:\n')
        summary_file.write('=' * 60 + '\n')

        for log_file in sorted(TMP_DIR.glob('redis_*.log')):
            set_qps, get_qps = parse_log_file(log_file)

            # Extract instance ID from filename
            id_match = re.search(r'redis_(\d+)\.log', log_file.name)
            instance_id = id_match.group(1) if id_match else 'Unknown'

            if set_qps == 0.0 and get_qps == 0.0:
                print(f'⚠️  Invalid or empty result at instance {instance_id}, skipping...')
                continue

            log_msg = f'Instance {instance_id:4} -> SET: {set_qps:10.2f} QPS, GET: {get_qps:10.2f} QPS'
            summary_file.write(log_msg + '\n')

            total_set_qps += set_qps
            total_get_qps += get_qps
            total_set_ops += TOTAL_REQUESTS
            total_get_ops += TOTAL_REQUESTS
            valid_instances += 1

        # Write summary statistics
        summary_file.write('\n' + '=' * 60 + '\n')
        summary_file.write('SUMMARY STATISTICS:\n')
        summary_file.write(f'Valid instances: {valid_instances}/{INSTANCE_COUNT}\n')
        if valid_instances > 0:
            summary_file.write(f'Average SET QPS per instance: {total_set_qps/valid_instances:.2f}\n')
            summary_file.write(f'Average GET QPS per instance: {total_get_qps/valid_instances:.2f}\n')
            summary_file.write(f'Total SET QPS (all instances): {total_set_qps:.2f}\n')
            summary_file.write(f'Total GET QPS (all instances): {total_get_qps:.2f}\n')

    # Print summary
    total_requests = total_set_ops + total_get_ops
    total_duration = end_time - start_time
    total_qps = total_requests / total_duration if total_duration > 0 else 0

    print('\n================== TOTAL REDIS RESULTS ===================')
    print(f'Instances benchmarked successfully: {valid_instances}/{INSTANCE_COUNT}')
    print(f'Failed instances: {failed}')
    if valid_instances > 0:
        print(f'Average SET QPS per instance: {total_set_qps/valid_instances:.2f}')
        print(f'Average GET QPS per instance: {total_get_qps/valid_instances:.2f}')
        print(f'Total SET QPS (aggregated): {total_set_qps:.2f}')
        print(f'Total GET QPS (aggregated): {total_get_qps:.2f}')
    print(f'Total operations: {total_requests}')
    print(f'Total time: {total_duration:.2f} seconds')
    print(f'TOTAL QPS: {total_qps:.2f} requests per second')
    print(f'\nDetailed results saved to: {summary_log_path}')


if __name__ == '__main__':
    main()
