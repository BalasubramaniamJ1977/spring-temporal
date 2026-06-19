#!/usr/bin/env bash
# start.sh -- Standalone startup: Temporal dev server + payment microservices
# Works on macOS (Intel and Apple Silicon) and Linux.
# Prerequisites: Java 21+, Maven, Temporal CLI (on PATH or at data/temporal-cli/temporal).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPORAL_PORT=7233
TEMPORAL_UI_PORT=8233
INITIATION_PORT=8001
WORKER_PORT=8020
LOG_DIR="$SCRIPT_DIR/logs"
DATA_DIR="$SCRIPT_DIR/data"
SERVICES_DIR="$SCRIPT_DIR/services"
PID_FILE="$SCRIPT_DIR/.pids.json"
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'

step() { echo -e "${CYAN}[$1] $2${NC}"; }
ok()   { echo -e "    ${GREEN}$1${NC}"; }
fail() { echo -e "\n${RED}FATAL: $1${NC}"; exit 1; }

wait_for_http() {
    local url=$1 label=$2 timeout=${3:-90}
    local deadline=$(( $(date +%s) + timeout ))
    printf "    Waiting for %s ..." "$label"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if curl -sf --max-time 3 "$url" >/dev/null 2>&1; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        printf "."
        sleep 3
    done
    echo -e " ${YELLOW}timed out${NC}"
    return 1
}

wait_for_port() {
    local port=$1 timeout=${2:-90}
    local deadline=$(( $(date +%s) + timeout ))
    printf "    Waiting for Temporal on port %s ..." "$port"
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if (echo >/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            echo -e " ${GREEN}ready${NC}"
            return 0
        fi
        printf "."
        sleep 3
    done
    echo -e " ${YELLOW}timed out${NC}"
    return 1
}

# ── Step 1: Locate / download Temporal CLI ─────────────────────────────────────

step 1 "Checking prerequisites"

command -v java >/dev/null 2>&1 || fail "Java not found. Install Java 21+: https://adoptium.net"
command -v mvn  >/dev/null 2>&1 || fail "Maven not found. Install: brew install maven"

TEMPORAL_EXE=""
if command -v temporal >/dev/null 2>&1; then
    TEMPORAL_EXE="temporal"
elif [ -x "$DATA_DIR/temporal-cli/temporal" ]; then
    TEMPORAL_EXE="$DATA_DIR/temporal-cli/temporal"
fi

if [ -z "$TEMPORAL_EXE" ]; then
    echo ""
    echo "  Temporal CLI not found. Download it from:"
    echo "  https://github.com/temporalio/cli/releases"
    echo ""
    echo "  Then either:"
    echo "    a) Add it to your PATH, or"
    echo "    b) Place the binary at: $DATA_DIR/temporal-cli/temporal"
    echo "       and make it executable: chmod +x $DATA_DIR/temporal-cli/temporal"
    exit 1
fi

ok "java     : $(java -version 2>&1 | head -1)"
ok "mvn      : $(mvn -v 2>&1 | head -1)"
ok "temporal : $("$TEMPORAL_EXE" --version 2>&1 | head -1)  ($TEMPORAL_EXE)"

# ── Step 2: Prepare directories ────────────────────────────────────────────────

mkdir -p "$LOG_DIR" "$DATA_DIR"

# ── Step 3: Maven build ────────────────────────────────────────────────────────

step 2 "Building Maven modules (common, temporal-api, temporal-worker, payment-initiation)"
echo "    Full output -> $LOG_DIR/maven-build.log"

set +e
mvn -f "$SERVICES_DIR/pom.xml" \
    -pl "common,temporal-api,temporal-worker,payment-initiation" \
    -am clean package -DskipTests \
    2>&1 | tee "$LOG_DIR/maven-build.log" | grep -E "^\[INFO\] Building |BUILD |ERROR " || true
MVN_EXIT=${PIPESTATUS[0]}
set -e

if [ "$MVN_EXIT" -ne 0 ]; then
    fail "Maven build failed (exit $MVN_EXIT) -- see $LOG_DIR/maven-build.log"
fi
ok "Build successful"

INITIATION_JAR=$(find "$SERVICES_DIR/payment-initiation/target" -name "payment-initiation-*.jar" \
    ! -name "*sources*" ! -name "*javadoc*" 2>/dev/null | head -1)
WORKER_JAR=$(find "$SERVICES_DIR/temporal-worker/target" -name "temporal-worker-*.jar" \
    ! -name "*sources*" ! -name "*javadoc*" 2>/dev/null | head -1)

[ -n "$INITIATION_JAR" ] || fail "payment-initiation JAR not found under $SERVICES_DIR/payment-initiation/target/"
[ -n "$WORKER_JAR" ]     || fail "temporal-worker JAR not found under $SERVICES_DIR/temporal-worker/target/"

ok "payment-initiation : $(basename "$INITIATION_JAR")"
ok "temporal-worker    : $(basename "$WORKER_JAR")"

# ── Step 4: Temporal dev server ────────────────────────────────────────────────

step 3 "Starting Temporal dev server (SQLite, persisted at data/temporal.db)"

"$TEMPORAL_EXE" server start-dev \
    --port "$TEMPORAL_PORT" \
    --ui-port "$TEMPORAL_UI_PORT" \
    --db-filename "$DATA_DIR/temporal.db" \
    > "$LOG_DIR/temporal.log" 2>&1 &
TEMPORAL_PID=$!

wait_for_port "$TEMPORAL_PORT" 90 || fail "Temporal did not start -- check $LOG_DIR/temporal.log"
ok "Temporal running (PID $TEMPORAL_PID)"

# ── Step 5: Spring Boot services ──────────────────────────────────────────────

step 4 "Starting Spring Boot services"

java -Dserver.port="$INITIATION_PORT" \
     -Dtemporal.address="localhost:$TEMPORAL_PORT" \
     -Dtemporal.namespace=default \
     -jar "$INITIATION_JAR" \
     > "$LOG_DIR/payment-initiation.log" 2>&1 &
INITIATION_PID=$!

java -Dserver.port="$WORKER_PORT" \
     -Dtemporal.address="localhost:$TEMPORAL_PORT" \
     -Dtemporal.namespace=default \
     -jar "$WORKER_JAR" \
     > "$LOG_DIR/temporal-worker.log" 2>&1 &
WORKER_PID=$!

wait_for_http "http://localhost:$INITIATION_PORT/actuator/health" "payment-initiation" 90 || true
wait_for_http "http://localhost:$WORKER_PORT/actuator/health"     "temporal-worker"    90 || true

# ── Step 6: Save PIDs ─────────────────────────────────────────────────────────

cat > "$PID_FILE" <<EOF
{"temporal":$TEMPORAL_PID,"initiation":$INITIATION_PID,"worker":$WORKER_PID}
EOF

# ── Step 7: Summary ────────────────────────────────────────────────────────────

echo ""
echo "======================================================="
echo " Payment System Running"
echo "======================================================="
echo ""
echo "  Temporal UI    http://localhost:$TEMPORAL_UI_PORT"
echo "  Payment API    http://localhost:$INITIATION_PORT/initiate  (POST)"
echo "  Status query   http://localhost:$INITIATION_PORT/status/{uetr}"
echo "  Manual approve http://localhost:$INITIATION_PORT/approve/{uetr}  (POST)"
echo "  Worker health  http://localhost:$WORKER_PORT/actuator/health"
echo ""
echo "  Logs  $LOG_DIR"
echo "  Stop  ./stop.sh"
echo ""
echo "  PIDs  temporal=$TEMPORAL_PID  initiation=$INITIATION_PID  worker=$WORKER_PID"
echo "======================================================="
