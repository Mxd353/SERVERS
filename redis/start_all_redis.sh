#!/bin/bash

CONF_DIR="$HOME/data/SERVERS/redis/confs"
LOG_FILE="$HOME/data/SERVERS/redis/start_all.log"
LOCK_FILE="/tmp/redis_start.lock"

echo "Starting all Redis instances at $(date)..." > "$LOG_FILE"
echo "Starting all Redis instances at $(date)..."

echo "Checking redis-server path:"
which redis-server

confs=( "$CONF_DIR"/redis_*.conf )
total=${#confs[@]}

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

start_redis() {
  local conf="$1"
  local pidfile=$(grep -E "^pidfile " "$conf" | awk '{print $2}')

  if [[ -n "$pidfile" && -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    if ps -p "$pid" > /dev/null 2>&1; then
      echo "⚠️ Redis PID $pid already running. Skipping $conf" >> "$LOG_FILE"
      update_count
      return
    fi
  fi

  echo "Starting Redis with config: $conf" >> "$LOG_FILE"
  if ! redis-server "$conf" >> "$LOG_FILE" 2>&1; then
    echo "❌ Failed to start Redis from $conf" >&2
  fi

  update_count
}

update_count() {
  {
    flock -x 9
    count=$(<"$LOCK_FILE")
    echo $((count + 1)) > "$LOCK_FILE"
  } 9>"$LOCK_FILE.lock"
}

{
  while :; do
    sleep 0.2
    print_progress
    [[ $(<"$LOCK_FILE") -ge $total ]] && break
  done
} &

for conf in "${confs[@]}"; do
  start_redis "$conf" &
done

wait

print_progress
echo -e "\n✅ All Redis startup attempts finished at $(date)" | tee -a "$LOG_FILE"

rm -f "$LOCK_FILE" "$LOCK_FILE.lock"
