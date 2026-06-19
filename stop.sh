#!/usr/bin/env bash
# stop.sh -- Stop all payment system processes started by start.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.pids.json"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

if [ ! -f "$PID_FILE" ]; then
    echo -e "${YELLOW}No .pids.json found -- nothing to stop.${NC}"
    exit 0
fi

stop_pid() {
    local name=$1 pid=$2
    [ -z "$pid" ] && return
    if kill -0 "$pid" 2>/dev/null; then
        echo "Stopping $name (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        echo -e "  ${GREEN}stopped${NC}"
    else
        echo "$name (PID $pid) is not running"
    fi
}

TEMPORAL_PID=$(grep -o '"temporal":[0-9]*'   "$PID_FILE" | grep -o '[0-9]*$')
INITIATION_PID=$(grep -o '"initiation":[0-9]*' "$PID_FILE" | grep -o '[0-9]*$')
WORKER_PID=$(grep -o '"worker":[0-9]*'       "$PID_FILE" | grep -o '[0-9]*$')

stop_pid "payment-initiation" "$INITIATION_PID"
stop_pid "temporal-worker"    "$WORKER_PID"
stop_pid "temporal"           "$TEMPORAL_PID"

rm -f "$PID_FILE"
echo -e "\n${GREEN}All services stopped.${NC}"
