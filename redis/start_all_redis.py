#!/usr/bin/env python3
"""
Start all Redis instances in parallel with progress tracking.
Converted from start_all_redis.sh to Python.
"""

import os
import sys
import glob
import time
import subprocess
import fcntl
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
CONF_DIR = DATA_DIR / "confs"
LOG_FILE = DATA_DIR / "logs" / "start_all.log"
LOCK_FILE = Path("/tmp/redis_start.lock")
LOCK_FILE_META = Path("/tmp/redis_start.lock.meta")


def get_redis_server_path() -> Optional[str]:
    """Find redis-server executable path."""
    return shutil.which("redis-server")


def acquire_lock():
    """Acquire exclusive lock for counter updates."""
    lock_fd = open(LOCK_FILE_META, "w")
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
    return lock_fd


def release_lock(lock_fd):
    """Release lock."""
    fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
    lock_fd.close()


def read_count() -> int:
    """Read current counter value."""
    try:
        if LOCK_FILE.exists():
            return int(LOCK_FILE.read_text().strip())
    except (ValueError, IOError):
        pass
    return 0


def update_count():
    """Atomically increment counter."""
    lock_fd = acquire_lock()
    try:
        count = read_count()
        LOCK_FILE.write_text(str(count + 1))
    finally:
        release_lock(lock_fd)


def get_pidfile_from_conf(conf_path: Path) -> Optional[Path]:
    """Extract pidfile path from Redis config."""
    try:
        with open(conf_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('pidfile '):
                    parts = line.split(None, 1)
                    if len(parts) == 2:
                        return Path(parts[1])
    except IOError:
        pass
    return None


def is_redis_running(pidfile: Path) -> bool:
    """Check if Redis instance with given PID is running."""
    try:
        if not pidfile.exists():
            return False
        pid = int(pidfile.read_text().strip())
        # Check if process exists
        os.kill(pid, 0)
        return True
    except (ValueError, IOError, OSError, ProcessLookupError):
        return False


def start_redis(conf_path: Path, log_file: Path) -> bool:
    """Start a single Redis instance."""
    pidfile = get_pidfile_from_conf(conf_path)

    # Check if already running
    if pidfile and is_redis_running(pidfile):
        log_message(log_file, f"⚠️  Redis PID {pidfile.read_text().strip()} already running. Skipping {conf_path}")
        update_count()
        return True

    log_message(log_file, f"Starting Redis with config: {conf_path}")

    try:
        # Start Redis
        result = subprocess.run(
            ["redis-server", str(conf_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=5
        )

        if result.returncode != 0:
            log_message(log_file, f"❌ Failed to start Redis from {conf_path}")
            log_message(log_file, f"Error: {result.stdout}")
            update_count()
            return False

        if result.stdout:
            log_message(log_file, result.stdout)

    except subprocess.TimeoutExpired:
        log_message(log_file, f"⚠️  Timeout starting {conf_path}, but process may still be starting")
    except Exception as e:
        log_message(log_file, f"❌ Error starting {conf_path}: {e}")
        update_count()
        return False

    update_count()
    return True


def log_message(log_file: Path, message: str):
    """Write message to log file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")


def print_progress(current: int, total: int):
    """Print progress bar."""
    progress = int(current * 100 / total)
    done = progress // 2
    left = 50 - done
    bar_done = "#" * done
    bar_left = " " * left
    sys.stdout.write(f"\r[{bar_done}{bar_left}] {progress:3d}% ({current}/{total})")
    sys.stdout.flush()


def main():
    """Main entry point."""
    start_time = datetime.now()

    # Initialize log
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w") as f:
        f.write(f"Starting all Redis instances at {start_time}...\n")

    print(f"Starting all Redis instances at {start_time}...")

    # Check redis-server
    redis_path = get_redis_server_path()
    if not redis_path:
        print("❌ Error: redis-server not found in PATH")
        sys.exit(1)
    print(f"Found redis-server at: {redis_path}")
    log_message(LOG_FILE, f"redis-server path: {redis_path}")

    # Find all config files
    conf_pattern = CONF_DIR / "redis_*.conf"
    confs = sorted(glob.glob(str(conf_pattern)))
    total = len(confs)

    if total == 0:
        print(f"❌ No config files found in {CONF_DIR}")
        sys.exit(1)

    print(f"Found {total} Redis config files")
    log_message(LOG_FILE, f"Found {total} config files to start")

    # Initialize counter
    LOCK_FILE.write_text("0")

    # Progress display thread
    def progress_monitor():
        while True:
            time.sleep(0.2)
            current = read_count()
            print_progress(current, total)
            if current >= total:
                break

    import threading
    progress_thread = threading.Thread(target=progress_monitor)
    progress_thread.daemon = True
    progress_thread.start()

    # Start all Redis instances in parallel
    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=min(128, total)) as executor:
        future_to_conf = {
            executor.submit(start_redis, Path(conf), LOG_FILE): conf
            for conf in confs
        }

        for future in as_completed(future_to_conf):
            conf = future_to_conf[future]
            try:
                success = future.result()
                if success:
                    completed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"\n❌ Exception starting {conf}: {e}")
                failed += 1

    # Wait for progress bar to finish
    progress_thread.join(timeout=2)
    print_progress(total, total)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Cleanup
    LOCK_FILE.unlink(missing_ok=True)
    LOCK_FILE_META.unlink(missing_ok=True)

    # Summary
    summary = f"\n✅ All Redis startup attempts finished at {end_time}"
    summary += f"\n   Total: {total}, Successful: {completed}, Failed: {failed}"
    summary += f"\n   Duration: {duration:.2f} seconds"

    print(summary)
    log_message(LOG_FILE, summary)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
