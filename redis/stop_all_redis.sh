#!/bin/bash

PID_DIR="$HOME/data/SERVERS/redis/pids"
LOG_FILE="$HOME/data/SERVERS/redis/stop_all.log"
LOCK_FILE="/tmp/redis_stop.lock"

echo "Stopping all Redis instances at $(date)" > "$LOG_FILE"
echo "Stopping all Redis instances at $(date)"

pidfiles=( "$PID_DIR"/redis_*.pid )
total=${#pidfiles[@]}
stopped=0

echo 0 > "$LOCK_FILE"

print_progress() {
  local count=$(<"$LOCK_FILE")
  local progress=$((count * 100 / total))
  local done=$((progress / 2))
  local left=$((50 - done))
  local bar_done=$(printf "%0.s#" $(seq 1 $done))
  local bar_left=$(printf "%0.s " $(seq 1 $left))
  printf "\r[%s%s] %3d%% (%d/%d)" "$bar_done" "$bar_left" "$progress" "$count" "$total"
}

{
  while :; do
    sleep 0.2
    print_progress
    [[ $(<"$LOCK_FILE") -ge $total ]] && break
  done
} &

kill_redis() {
  local pidfile="$1"
  if [ -f "$pidfile" ]; then
    pid=$(cat "$pidfile")
    if ps -p "$pid" > /dev/null 2>&1; then
      echo "Stopping Redis (pid $pid) from $pidfile" >> "$LOG_FILE"
      kill "$pid"
    else
      echo "PID $pid in $pidfile not running" >> "$LOG_FILE"
    fi
  fi
  {
    flock -x 9
    count=$(<"$LOCK_FILE")
    echo $((count + 1)) > "$LOCK_FILE"
  } 9>"$LOCK_FILE.lock"
}

for pidfile in "${pidfiles[@]}"; do
  kill_redis "$pidfile" &
done

wait

print_progress
echo -e "\n✅ All Redis shutdown attempts finished at $(date)" >> "$LOG_FILE"
echo -e "\n✅ All Redis shutdown attempts finished at $(date)"

rm -f "$LOCK_FILE" "$LOCK_FILE.lock"
