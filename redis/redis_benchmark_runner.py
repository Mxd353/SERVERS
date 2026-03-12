import subprocess
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

START_PORT = 6380
END_PORT = START_PORT + 1024 - 1
TOTAL_REQUESTS = 100000
CONCURRENCY = 10
DATA_SIZE = 16
TMP_DIR = "/home/mxd/data/SERVERS/redis/redis_benchmark_logs"

os.makedirs(TMP_DIR, exist_ok=True)

summary_log_path = os.path.join(TMP_DIR, "summary.log")

for f in os.listdir(TMP_DIR):
    if f.endswith(".log"):
        os.remove(os.path.join(TMP_DIR, f))

def run_benchmark(port):
    log_file = os.path.join(TMP_DIR, f"redis_{port}.log")
    cmd = [
        "redis-benchmark",
        "-p", str(port),
        "-t", "set,get",
        "-n", str(TOTAL_REQUESTS),
        "-c", str(CONCURRENCY),
        "-d", str(DATA_SIZE),
        "-q"
    ]
    try:
        with open(log_file, "w") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.DEVNULL, timeout=300)
        return port, True
    except Exception as e:
        return port, False

print(f"Launching benchmarks for ports {START_PORT}-{END_PORT} ...")
start_time = time.time()

with ThreadPoolExecutor(max_workers=(END_PORT - START_PORT + 1)) as executor:
    future_to_port = {executor.submit(run_benchmark, port): port for port in range(START_PORT, END_PORT + 1)}

    completed = 0
    for future in as_completed(future_to_port):
        port, success = future.result()
        completed += 1
        if not success:
            print(f"[{completed:4}/{END_PORT - START_PORT + 1}] Port {port}: FAILED")

end_time = time.time()
print(f"Benchmarking completed in {end_time - start_time:.1f} seconds.")

total_set_ops = 0
total_get_ops = 0
valid_ports = 0

for fname in sorted(os.listdir(TMP_DIR)):
    if not fname.endswith(".log"):
        continue
    path = os.path.join(TMP_DIR, fname)

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = [line.strip().replace('\r', '') for line in f]

    set_qps = 0.0
    get_qps = 0.0
    for line in lines:
        if line.startswith("SET:"):
            try:
                val = float(line.split(":")[1].strip())
                if val >= 0:
                    set_qps = val
            except:
                pass
        elif line.startswith("GET:"):
            try:
                val = float(line.split(":")[1].strip())
                if val >= 0:
                    get_qps = val
            except:
                pass

    port_match = re.search(r'redis_(\d+)\.log', fname)
    port = port_match.group(1) if port_match else "Unknown"

    if set_qps == 0.0 and get_qps == 0.0:
        print(f"⚠️  Invalid or empty result at port {port}, skipping...")
        continue

    log_msg = f"Port {port} -> SET: {set_qps:.2f} QPS, GET: {get_qps:.2f} QPS"
    with open(summary_log_path, "a") as f:
        f.write(log_msg + "\n")

    total_set_ops += TOTAL_REQUESTS  # SET
    total_get_ops += TOTAL_REQUESTS  # GET
    valid_ports += 1

total_requests = total_set_ops + total_get_ops
total_duration = end_time - start_time
total_qps = total_requests / total_duration if total_duration > 0 else 0

print("\n================== TOTAL REDIS RESULTS ===================")
print(f"Ports benchmarked successfully: {valid_ports}")
print(f"Total SET operations: {total_set_ops}")
print(f"Total GET operations: {total_get_ops}")
print(f"Total time: {total_duration:.2f} seconds")
print(f"TOTAL QPS: {total_qps:.2f} requests per second")
