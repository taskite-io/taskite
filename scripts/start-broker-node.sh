#!/usr/bin/env bash
#
# Starts one Broker in the foreground.
#
# Brokers do not have a static Broker peer list. They register with the placement-driver and
# receive partition membership from it. The placement-drivers argument must contain all three
# PD gRPC endpoints in host:port form. Register the Broker cluster separately with
# scripts/register-broker-cluster.sh before starting the Broker nodes.
#
# Usage:
#   ./scripts/start-broker-node.sh \
#     <broker-id> <host> [pd-grpc-endpoints]
#   ./scripts/start-broker-node.sh stop <broker-id>
#   ./scripts/start-broker-node.sh clean <broker-id>
#
# Examples:
#   # Server 10.0.0.11
#   ./scripts/start-broker-node.sh broker-0 10.0.0.11 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
#   # Server 10.0.0.12
#   ./scripts/start-broker-node.sh broker-1 10.0.0.12 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
#   # Server 10.0.0.13
#   ./scripts/start-broker-node.sh broker-2 10.0.0.13 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
# Optional environment variables:
#   TASKITE_LAUNCHER_JAR, TASKITE_LAUNCHER_DATA_ROOT, TASKITE_LAUNCHER_PID_DIR
#   TASKITE_LAUNCHER_JAVA_BIN, JAVA_OPTS, TASKITE_LAUNCHER_STOP_TIMEOUT
#   TASKITE_BROKER_CLUSTER_CLUSTER_ID, MANAGEMENT_SERVER_PORT
#   Runtime overrides use complete TASKITE_BROKER_* names from the configuration template.
set -euo pipefail

duration_to_seconds() {
  local value="$1"
  if [[ "${value}" =~ ^([0-9]+)ms$ ]]; then
    echo $(((BASH_REMATCH[1] + 999) / 1000))
  elif [[ "${value}" =~ ^([0-9]+)s$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "${value}" =~ ^([0-9]+)m$ ]]; then
    echo $((BASH_REMATCH[1] * 60))
  else
    echo "error: invalid duration '${value}'; expected <number>ms, <number>s, or <number>m" >&2
    return 1
  fi
}

usage() {
  echo "usage:" >&2
  echo "  $0 <broker-id> <host> [pd-host:grpc-port,...]" >&2
  echo "  $0 stop <broker-id>" >&2
  echo "  $0 clean <broker-id>" >&2
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-${REPO_ROOT}/data}"
DATA_DIR="${TASKITE_BROKER_STORAGE_DATA_DIR:-${DATA_ROOT}/broker}"
RUN_ROOT="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/run}"
STOP_TIMEOUT="${TASKITE_LAUNCHER_STOP_TIMEOUT:-30s}"
STOP_TIMEOUT_SECONDS="$(duration_to_seconds "${STOP_TIMEOUT}")"

CLUSTER_ID="${TASKITE_BROKER_CLUSTER_CLUSTER_ID:-demo}"

stop_node() {
  local broker_id="$1"
  local pid_file="${RUN_ROOT}/broker-${broker_id}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "Broker ${broker_id} is not running"
    return
  fi

  local pid
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "stopping Broker ${broker_id} (pid ${pid})"
    kill "${pid}"
    local deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
    while kill -0 "${pid}" 2>/dev/null && ((SECONDS < deadline)); do
      sleep 0.2
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "error: Broker ${broker_id} did not stop within ${STOP_TIMEOUT}" >&2
      return 1
    fi
  fi
  rm -f "${pid_file}"
}

if [[ "${1:-}" == "stop" || "${1:-}" == "clean" ]]; then
  if (($# != 2)); then
    usage
    exit 2
  fi
  BROKER_ID="$2"
  stop_node "${BROKER_ID}"
  if [[ "$1" == "clean" ]]; then
    BROKER_NODE_DATA_DIR="${DATA_DIR}/${CLUSTER_ID}/${BROKER_ID}"
    echo "wiping Broker data ${BROKER_NODE_DATA_DIR}"
    rm -rf "${BROKER_NODE_DATA_DIR}"
  fi
  exit 0
fi

if (($# < 2 || $# > 3)); then
  usage
  exit 2
fi

BROKER_ID="$1"
HOST="$2"
HOST="${HOST#[}"
HOST="${HOST%]}"
DISPLAY_HOST="${HOST}"
if [[ "${HOST}" == *:* ]]; then
  DISPLAY_HOST="[${HOST}]"
fi
PLACEMENT_DRIVERS="${3:-${TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS:-}}"

if [[ -z "${BROKER_ID}" || -z "${HOST}" || -z "${PLACEMENT_DRIVERS}" ]]; then
  usage
  exit 2
fi

IFS=',' read -r -a pd_endpoints <<<"${PLACEMENT_DRIVERS}"
if ((${#pd_endpoints[@]} != 3)); then
  echo "error: a three-node deployment requires exactly three PD gRPC endpoints" >&2
  exit 2
fi

for endpoint in "${pd_endpoints[@]}"; do
  endpoint_pattern='^([[:alnum:]_.%-]+|\[[[:alnum:]_.%-]*:[[:alnum:]:_.%-]*\]):[0-9]+$'
  if [[ ! "${endpoint}" =~ ${endpoint_pattern} ]]; then
    echo "error: invalid PD endpoint '${endpoint}'" >&2
    echo "expected: host:grpc-port or [ipv6]:grpc-port" >&2
    exit 2
  fi
done

PORT="${TASKITE_BROKER_NETWORK_COMMAND_PORT:-26501}"
RAFT_PORT="${TASKITE_BROKER_NETWORK_RAFT_PORT:-26502}"
MANAGEMENT_PORT="${MANAGEMENT_SERVER_PORT:-9600}"
SNAPSHOT_INTERVAL="${TASKITE_BROKER_STORAGE_SNAPSHOT_INTERVAL:-15m}"
SNAPSHOT_LOG_SIZE_THRESHOLD="${TASKITE_BROKER_STORAGE_SNAPSHOT_LOG_SIZE_THRESHOLD:-128MB}"

if [[ "${PORT}" == "${RAFT_PORT}" ]]; then
  echo "error: Broker business and Raft ports must be different" >&2
  exit 2
fi

if [[ ! -f "${JAR}" ]]; then
  echo "error: distribution JAR not found: ${JAR}" >&2
  echo "build it with: mvn -q -pl dist -am package -DskipTests" >&2
  exit 1
fi

JAVA_BIN="${TASKITE_LAUNCHER_JAVA_BIN:-java}"
JAVA_COMMAND=("${JAVA_BIN}")
if [[ -n "${JAVA_OPTS:-}" ]]; then
  read -r -a java_options <<<"${JAVA_OPTS}"
  JAVA_COMMAND+=("${java_options[@]}")
fi

PID_FILE="${RUN_ROOT}/broker-${BROKER_ID}.pid"
mkdir -p "${DATA_DIR}" "${RUN_ROOT}"
if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "error: Broker ${BROKER_ID} is already running (pid ${existing_pid})" >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
fi

JAR="$(launcher_snapshot_jar "${JAR}" "${RUN_ROOT}/runtime")"
echo "starting Broker ${BROKER_ID}"
echo "  runtime settings below: ENV or defaults; external YAML may override defaults."
echo "  business endpoint: ${DISPLAY_HOST}:${PORT}"
echo "  raft endpoint:     ${DISPLAY_HOST}:${RAFT_PORT}"
echo "  metrics endpoint:  http://${DISPLAY_HOST}:${MANAGEMENT_PORT}/actuator/prometheus"
echo "  placement drivers: ${PLACEMENT_DRIVERS}"
echo "  data root:         ${DATA_DIR}"
echo "  snapshot policy:   ${SNAPSHOT_INTERVAL} / ${SNAPSHOT_LOG_SIZE_THRESHOLD}"
echo "  command batch max: ${TASKITE_BROKER_PROCESSING_MAX_COMMANDS_IN_BATCH:-1}"
echo "  record batch max:  ${TASKITE_BROKER_PROCESSING_MAX_RECORDS_IN_BATCH:-0}"
echo "  batch duration:    ${TASKITE_BROKER_PROCESSING_MAX_BATCH_DURATION:-0ms}"
echo "  backpressure:      enabled=${TASKITE_BROKER_BACKPRESSURE_ENABLED:-true}, algorithm=${TASKITE_BROKER_BACKPRESSURE_ALGORITHM:-aimd}"
echo "  fixed limit:       ${TASKITE_BROKER_BACKPRESSURE_FIXED_LIMIT:-20}"
echo "  batch payload max: ${TASKITE_BROKER_PROCESSING_MAX_BATCH_BYTES:-3145728} bytes"

"${JAVA_COMMAND[@]}" -jar "${JAR}" broker \
  --taskite.broker.cluster.broker-id="${BROKER_ID}" \
  --taskite.broker.network.host="${HOST}" \
  --taskite.broker.cluster.cluster-id="${CLUSTER_ID}" \
  --taskite.broker.registration.placement-drivers="${PLACEMENT_DRIVERS}" \
  --taskite.broker.storage.data-dir="${DATA_DIR}" &

child_pid="$!"
echo "${child_pid}" >"${PID_FILE}"

remove_pid_file() {
  if [[ -f "${PID_FILE}" && "$(cat "${PID_FILE}")" == "${child_pid}" ]]; then
    rm -f "${PID_FILE}"
  fi
}

terminate_child() {
  local signal="$1"
  local exit_code="$2"
  trap - INT TERM
  echo
  echo "received ${signal}; stopping Broker ${BROKER_ID} (pid ${child_pid})"
  if kill -0 "${child_pid}" 2>/dev/null; then
    kill -TERM "${child_pid}" 2>/dev/null || true
  fi
  wait "${child_pid}" 2>/dev/null || true
  exit "${exit_code}"
}

trap 'terminate_child SIGINT 130' INT
trap 'terminate_child SIGTERM 143' TERM
trap remove_pid_file EXIT
wait "${child_pid}"
