#!/usr/bin/env python3
"""
Stop all Redis instances in parallel with progress tracking.
Converted from stop_all_redis.sh to Python.
"""

import os
import sys
import glob
import time
import fcntl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
PID_DIR = DATA_DIR / "pids"
LOG_FILE = DATA_DIR / "logs" / "stop_all.log"
LOCK_FILE = Path("/tmp/redis_stop.lock")
LOCK_FILE_META = Path("/tmp/redis_stop.lock.meta")


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


def is_process_running(pid: int) -> bool:
    """Check if process with given PID is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def stop_redis(pidfile: Path, log_file: Path) -> bool:
    """Stop a single Redis instance."""
    try:
        if not pidfile.exists():
            log_message(log_file, f"PID file not found: {pidfile}")
            update_count()
            return True

        pid = int(pidfile.read_text().strip())

        if not is_process_running(pid):
            log_message(log_file, f"PID {pid} in {pidfile} not running")
            update_count()
            return True

        log_message(log_file, f"Stopping Redis (pid {pid}) from {pidfile}")

        try:
            os.kill(pid, 15)  # SIGTERM
            # Wait a bit for graceful shutdown
            for _ in range(10):
                if not is_process_running(pid):
                    break
                time.sleep(0.1)

            # Force kill if still running
            if is_process_running(pid):
                log_message(log_file, f"Force killing Redis (pid {pid})")
                os.kill(pid, 9)  # SIGKILL

        except (OSError, ProcessLookupError) as e:
            log_message(log_file, f"Error stopping PID {pid}: {e}")

    except (ValueError, IOError) as e:
        log_message(log_file, f"Error reading {pidfile}: {e}")

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
        f.write(f"Stopping all Redis instances at {start_time}...\n")

    print(f"Stopping all Redis instances at {start_time}...")
    log_message(LOG_FILE, f"Stopping all Redis instances at {start_time}")

    # Find all PID files
    pid_pattern = PID_DIR / "redis_*.pid"
    pidfiles = sorted(glob.glob(str(pid_pattern)))
    total = len(pidfiles)

    if total == 0:
        print(f"⚠️  No PID files found in {PID_DIR}")
        log_message(LOG_FILE, f"No PID files found in {PID_DIR}")
        sys.exit(0)

    print(f"Found {total} Redis PID files")
    log_message(LOG_FILE, f"Found {total} PID files to stop")

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

    # Stop all Redis instances in parallel
    completed = 0

    with ThreadPoolExecutor(max_workers=min(32, total)) as executor:
        future_to_pidfile = {
            executor.submit(stop_redis, Path(pidfile), LOG_FILE): pidfile
            for pidfile in pidfiles
        }

        for future in as_completed(future_to_pidfile):
            pidfile = future_to_pidfile[future]
            try:
                future.result()
                completed += 1
            except Exception as e:
                print(f"\n❌ Exception stopping {pidfile}: {e}")

    # Wait for progress bar to finish
    progress_thread.join(timeout=2)
    print_progress(total, total)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Cleanup
    LOCK_FILE.unlink(missing_ok=True)
    LOCK_FILE_META.unlink(missing_ok=True)

    # Summary
    summary = f"\n✅ All Redis shutdown attempts finished at {end_time}"
    summary += f"\n   Total: {total}, Completed: {completed}"
    summary += f"\n   Duration: {duration:.2f} seconds"

    print(summary)
    log_message(LOG_FILE, summary)


if __name__ == "__main__":
    main()
