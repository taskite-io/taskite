#!/usr/bin/env bash
#
# Starts one stateless Gateway in the foreground for a three-server deployment.
#
# gateway-id is a local process label used for PID and cleanup management. Gateway has no
# persistent node identity; it discovers Broker topology from the placement-driver cluster.
#
# Usage:
#   ./scripts/start-gateway-node.sh <gateway-id> <host> <pd-grpc-endpoints>
#   ./scripts/start-gateway-node.sh stop <gateway-id>
#   ./scripts/start-gateway-node.sh clean <gateway-id>
#
# Examples:
#   # Server 10.0.0.11
#   ./scripts/start-gateway-node.sh gateway-0 10.0.0.11 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
#   # Server 10.0.0.12
#   ./scripts/start-gateway-node.sh gateway-1 10.0.0.12 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
#   # Server 10.0.0.13
#   ./scripts/start-gateway-node.sh gateway-2 10.0.0.13 \
#     '10.0.0.11:26501,10.0.0.12:26501,10.0.0.13:26501'
#
# Optional environment variables:
#   TASKITE_LAUNCHER_JAR, TASKITE_LAUNCHER_DATA_ROOT, TASKITE_LAUNCHER_PID_DIR
#   TASKITE_LAUNCHER_JAVA_BIN, JAVA_OPTS, TASKITE_LAUNCHER_STOP_TIMEOUT
#   Runtime overrides use complete TASKITE_GATEWAY_* names from the template.
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
  echo "  $0 <gateway-id> <host> <pd-host:grpc-port,...>" >&2
  echo "  $0 stop <gateway-id>" >&2
  echo "  $0 clean <gateway-id>" >&2
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-${REPO_ROOT}/data}"
RUN_ROOT="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/run}"
STOP_TIMEOUT="${TASKITE_LAUNCHER_STOP_TIMEOUT:-30s}"
STOP_TIMEOUT_SECONDS="$(duration_to_seconds "${STOP_TIMEOUT}")"

stop_node() {
  local gateway_id="$1"
  local pid_file="${RUN_ROOT}/gateway-${gateway_id}.pid"
  if [[ ! -f "${pid_file}" ]]; then
    echo "Gateway ${gateway_id} is not running"
    return
  fi

  local pid
  pid="$(cat "${pid_file}")"
  if [[ "${pid}" =~ ^[0-9]+$ ]] && kill -0 "${pid}" 2>/dev/null; then
    echo "stopping Gateway ${gateway_id} (pid ${pid})"
    kill "${pid}"
    local deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
    while kill -0 "${pid}" 2>/dev/null && ((SECONDS < deadline)); do
      sleep 0.2
    done
    if kill -0 "${pid}" 2>/dev/null; then
      echo "error: Gateway ${gateway_id} did not stop within ${STOP_TIMEOUT}" >&2
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
  GATEWAY_ID="$2"
  stop_node "${GATEWAY_ID}"
  if [[ "$1" == "clean" ]]; then
    GATEWAY_STATE_DIR="${DATA_ROOT}/gateway/${GATEWAY_ID}"
    echo "wiping Gateway runtime state ${GATEWAY_STATE_DIR}"
    rm -rf "${GATEWAY_STATE_DIR}"
  fi
  exit 0
fi

if (($# != 3)); then
  usage
  exit 2
fi

GATEWAY_ID="$1"
GATEWAY_HOST="$2"
GATEWAY_HOST="${GATEWAY_HOST#[}"
GATEWAY_HOST="${GATEWAY_HOST%]}"
DISPLAY_HOST="${GATEWAY_HOST}"
if [[ "${GATEWAY_HOST}" == *:* ]]; then
  DISPLAY_HOST="[${GATEWAY_HOST}]"
fi
PLACEMENT_DRIVERS="$3"

if [[ -z "${GATEWAY_ID}" || -z "${GATEWAY_HOST}" || -z "${PLACEMENT_DRIVERS}" ]]; then
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

GATEWAY_STATE_DIR="${DATA_ROOT}/gateway/${GATEWAY_ID}"
PID_FILE="${RUN_ROOT}/gateway-${GATEWAY_ID}.pid"
mkdir -p "${GATEWAY_STATE_DIR}" "${RUN_ROOT}"
if [[ -f "${PID_FILE}" ]]; then
  existing_pid="$(cat "${PID_FILE}")"
  if [[ "${existing_pid}" =~ ^[0-9]+$ ]] && kill -0 "${existing_pid}" 2>/dev/null; then
    echo "error: Gateway ${GATEWAY_ID} is already running (pid ${existing_pid})" >&2
    exit 1
  fi
  rm -f "${PID_FILE}"
fi

JAR="$(launcher_snapshot_jar "${JAR}" "${RUN_ROOT}/runtime")"
echo "starting Gateway ${GATEWAY_ID}"
echo "  host:               ${DISPLAY_HOST}"
echo "  placement drivers:  ${PLACEMENT_DRIVERS}"
echo "  runtime settings:   ENV > external YAML > packaged defaults (management threads: 20)"

"${JAVA_COMMAND[@]}" -jar "${JAR}" gateway \
  --taskite.gateway.network.host="${GATEWAY_HOST}" \
  --taskite.gateway.placement-driver.endpoints="${PLACEMENT_DRIVERS}" &

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
  echo "received ${signal}; stopping Gateway ${GATEWAY_ID} (pid ${child_pid})"
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
