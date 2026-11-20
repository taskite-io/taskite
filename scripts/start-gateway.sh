#!/usr/bin/env bash
#
# Launches a local logstream gateway. The gateway is stateless: it discovers the
# broker topology from the placement-driver and routes client requests to the
# right broker, so start the PD cluster and the broker cluster first
# (scripts/start-placement-driver-cluster.sh, scripts/start-broker-cluster.sh).
#
# Gateway configuration accepts external YAML and `TASKITE_GATEWAY_*` variables.
# `TASKITE_GATEWAY_PLACEMENT_DRIVER_ENDPOINTS` is a list of PD gRPC endpoints in
# `host:grpcPort` form (NO `http://` scheme).
#
# Ports:
#   gateway   client-port (gRPC)
#   gateway-0 26500
#
# Usage:
#   ./scripts/start-gateway.sh           # build (if needed) + start the gateway
#   ./scripts/start-gateway.sh --build   # force a rebuild first
#   ./scripts/start-gateway.sh stop      # stop the gateway
#   ./scripts/start-gateway.sh clean     # stop the gateway + wipe its logs/pids
#   TASKITE_GATEWAY_NETWORK_PORT=27500
#   TASKITE_GATEWAY_PLACEMENT_DRIVER_ENDPOINTS=127.0.0.1:29001,127.0.0.1:29002,127.0.0.1:29003
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-/tmp/gateway}"
LOG_DIR="${TASKITE_LAUNCHER_LOG_DIR:-${DATA_ROOT}/logs}"
PID_DIR="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/pids}"
JAVA_COMMAND=("${TASKITE_LAUNCHER_JAVA_BIN:-java}")
if [[ -n "${JAVA_OPTS:-}" ]]; then
  read -r -a user_java_options <<<"${JAVA_OPTS}"
  JAVA_COMMAND+=("${user_java_options[@]}")
fi

NODE="gateway-0"

PLACEMENT_DRIVERS="${TASKITE_GATEWAY_PLACEMENT_DRIVER_ENDPOINTS:-127.0.0.1:29001,127.0.0.1:29002,127.0.0.1:29003}"

stop_gateway() {
  pid_file="${PID_DIR}/${NODE}.pid"
  launcher_stop "${pid_file}" "${NODE}"
}

clean_gateway() {
  stop_gateway
  echo "wiping data root ${DATA_ROOT}"
  rm -rf "${DATA_ROOT}"
}

start_gateway() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  launcher_check_pid "${PID_DIR}/${NODE}.pid" "${NODE}"
  JAR="$(launcher_snapshot_jar "${JAR}" "${PID_DIR}/runtime")"
  echo "starting ${NODE}: placementDrivers=${PLACEMENT_DRIVERS}"
  echo "runtime settings: ENV > external YAML > packaged defaults (management threads: 20)"
  "${JAVA_COMMAND[@]}" -jar "${JAR}" gateway \
    --taskite.gateway.placement-driver.endpoints="${PLACEMENT_DRIVERS}" \
    >"${LOG_DIR}/${NODE}.log" 2>&1 &
  echo "$!" >"${PID_DIR}/${NODE}.pid"
  echo
  echo "gateway started. logs in ${LOG_DIR}, pids in ${PID_DIR}."
  echo "client endpoint: ENV or external YAML; default 127.0.0.1:26500"
  echo "stop with:       $0 stop"
}

main() {
  local force_build=false
  case "${1:-}" in
    stop)
      stop_gateway
      exit 0
      ;;
    clean)
      clean_gateway
      exit 0
      ;;
    --build)
      force_build=true
      ;;
    "") ;;
    *)
      echo "usage: $0 [--build|stop|clean]" >&2
      exit 2
      ;;
  esac

  if [[ "${force_build}" == true || ! -f "${JAR}" ]]; then
    echo "building dist jar..."
    mvn -q -f "${REPO_ROOT}/pom.xml" -pl dist -am package -DskipTests
  fi

  start_gateway
}

main "$@"
