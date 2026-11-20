#!/usr/bin/env bash
#
# Starts one placement-driver node in the foreground for a three-server deployment.
#
# Each server uses the same gRPC and Raft ports because the hosts are different.
# The peers argument must contain the other two PD nodes and must not contain the local node.
#
# Usage:
#   ./scripts/start-placement-driver-node.sh <node-id> <host> <peers>
#   ./scripts/start-placement-driver-node.sh stop <node-id>
#   ./scripts/start-placement-driver-node.sh clean <node-id>
#
# Examples:
#   # Server 10.0.0.11
#   ./scripts/start-placement-driver-node.sh meta-0 10.0.0.11 \
#     'meta-1@10.0.0.12:26501:26502,meta-2@10.0.0.13:26501:26502'
#
#   # Server 10.0.0.12
#   ./scripts/start-placement-driver-node.sh meta-1 10.0.0.12 \
#     'meta-0@10.0.0.11:26501:26502,meta-2@10.0.0.13:26501:26502'
#
#   # Server 10.0.0.13
#   ./scripts/start-placement-driver-node.sh meta-2 10.0.0.13 \
#     'meta-0@10.0.0.11:26501:26502,meta-1@10.0.0.12:26501:26502'
#
# Optional environment variables:
#   TASKITE_LAUNCHER_JAR, TASKITE_LAUNCHER_DATA_ROOT, TASKITE_LAUNCHER_PID_DIR
#   TASKITE_LAUNCHER_JAVA_BIN, JAVA_OPTS, TASKITE_LAUNCHER_STOP_TIMEOUT
#   SERVER_PORT; runtime overrides use complete TASKITE_PD_* template names.
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
  echo "  $0 <node-id> <host> <peer-id@host:grpc-port:raft-port,...>" >&2
  echo "  $0 stop <node-id>" >&2
  echo "  $0 clean <node-id>" >&2
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-${REPO_ROOT}/data}"
RUN_ROOT="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/run}"
STOP_TIMEOUT="${TASKITE_LAUNCHER_STOP_TIMEOUT:-30s}"
STOP_TIMEOUT_SECONDS="$(duration_to_seconds "${STOP_TIMEOUT}")"

stop_node() {
  local node_id="$1"
  local pid_file="${RUN_ROOT}/placement-driver-${node_id}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "placement-driver ${node_id} is not running"
    return
  fi

  local pid
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "stopping placement-driver ${node_id} (pid ${pid})"
    kill "${pid}"
    local deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
    while kill -0 "${pid}" 2>/dev/null && ((SECONDS < deadline)); do
      sleep 0.2
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "error: placement-driver ${node_id} did not stop within ${STOP_TIMEOUT}" >&2
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
  NODE_ID="$2"
  DATA_DIR="${TASKITE_PD_STORAGE_DATA_DIR:-${DATA_ROOT}/placement-driver/${NODE_ID}}"
  stop_node "${NODE_ID}"
  if [[ "$1" == "clean" ]]; then
    echo "wiping placement-driver data ${DATA_DIR}"
    rm -rf "${DATA_DIR}"
  fi
  exit 0
fi

if (($# != 3)); then
  usage
  exit 2
fi

NODE_ID="$1"
HOST="$2"
HOST="${HOST#[}"
HOST="${HOST%]}"
DISPLAY_HOST="${HOST}"
if [[ "${HOST}" == *:* ]]; then
  DISPLAY_HOST="[${HOST}]"
fi
PD_PEERS="$3"

if [[ -z "${NODE_ID}" || -z "${HOST}" || -z "${PD_PEERS}" ]]; then
  usage
  exit 2
fi

IFS=',' read -r -a peer_entries <<<"${PD_PEERS}"
if ((${#peer_entries[@]} != 2)); then
  echo "error: a three-node PD cluster requires exactly two remote peers" >&2
  exit 2
fi

for peer in "${peer_entries[@]}"; do
  peer_pattern='^[^@[:space:]]+@([[:alnum:]_.%-]+|\[[[:alnum:]_.%-]*:[[:alnum:]:_.%-]*\]):[0-9]+:[0-9]+$'
  if [[ ! "${peer}" =~ ${peer_pattern} ]]; then
    echo "error: invalid PD peer '${peer}'" >&2
    echo "expected: node-id@host:grpc-port:raft-port or node-id@[ipv6]:grpc-port:raft-port" >&2
    exit 2
  fi
  if [[ "${peer%%@*}" == "${NODE_ID}" ]]; then
    echo "error: PD peers must not contain the local node '${NODE_ID}'" >&2
    exit 2
  fi
done

DATA_DIR="${TASKITE_PD_STORAGE_DATA_DIR:-${DATA_ROOT}/placement-driver/${NODE_ID}}"
PID_FILE="${RUN_ROOT}/placement-driver-${NODE_ID}.pid"

HTTP_PORT="${SERVER_PORT:-19001}"
GRPC_PORT="${TASKITE_PD_NETWORK_GRPC_PORT:-26501}"
RAFT_PORT="${TASKITE_PD_NETWORK_RAFT_PORT:-26502}"

if [[ "${GRPC_PORT}" == "${RAFT_PORT}" ]]; then
  echo "error: PD gRPC and Raft ports must be different" >&2
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

mkdir -p "${DATA_DIR}" "${RUN_ROOT}"
if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "error: placement-driver ${NODE_ID} is already running (pid ${existing_pid})" >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
fi

JAR="$(launcher_snapshot_jar "${JAR}" "${RUN_ROOT}/runtime")"
echo "starting placement-driver ${NODE_ID}"
echo "  advertised endpoint: ${DISPLAY_HOST}:${GRPC_PORT}"
echo "  raft endpoint:       ${DISPLAY_HOST}:${RAFT_PORT}"
echo "  admin endpoint:      http://${DISPLAY_HOST}:${HTTP_PORT}/api/v1/pd"
echo "  peers:               ${PD_PEERS}"
echo "  data:                ${DATA_DIR}"
echo "  runtime ports above: ENV or packaged defaults; external YAML may override defaults."

"${JAVA_COMMAND[@]}" -jar "${JAR}" placement-driver \
  --taskite.pd.node-id="${NODE_ID}" \
  --taskite.pd.network.host="${HOST}" \
  --taskite.pd.storage.data-dir="${DATA_DIR}" \
  --taskite.pd.consensus.bootstrap-peers="${PD_PEERS}" &

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
  echo "received ${signal}; stopping placement-driver ${NODE_ID} (pid ${child_pid})"
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
