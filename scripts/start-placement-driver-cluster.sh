#!/usr/bin/env bash
#
# Launches a local 3-node placement-driver cluster.
#
# Peer format is `nodeId@host:grpcPort:raftPort` (NO `http://` scheme). Each node lists only the
# OTHER nodes as its peers.
#
# Ports per node:
#   node      http   grpc   raft
#   meta-0   19001  29001  29101
#   meta-1   19002  29002  29102
#   meta-2   19003  29003  29103
#
# Usage:
#   ./scripts/start-placement-driver-cluster.sh           # build (if needed) + start all 3
#   ./scripts/start-placement-driver-cluster.sh --build   # force a rebuild first
#   ./scripts/start-placement-driver-cluster.sh stop      # stop all nodes
#   ./scripts/start-placement-driver-cluster.sh clean      # stop all nodes + wipe data dirs
#
# NOTE: persisted data under DATA_ROOT is tied to the current SBE schema. After a
# schema/encoding change, run `clean` first or startup may fail decoding stale records.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
CONFIG_DIR="${TASKITE_LAUNCHER_CONFIG_DIR:-${REPO_ROOT}/dist/target/config}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-/tmp/placement-driver}"
LOG_DIR="${TASKITE_LAUNCHER_LOG_DIR:-${DATA_ROOT}/logs}"
PID_DIR="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/pids}"
ADDITIONAL_CONFIG_LOCATION="${SPRING_CONFIG_ADDITIONAL_LOCATION:-optional:file:${CONFIG_DIR}/}"
JAVA_BIN="${TASKITE_LAUNCHER_JAVA_BIN:-java}"
JAVA_COMMAND=("${JAVA_BIN}")
if [[ -n "${JAVA_OPTS:-}" ]]; then
  read -r -a user_java_options <<<"${JAVA_OPTS}"
  if ((${#user_java_options[@]} > 0)); then
    JAVA_COMMAND+=("${user_java_options[@]}")
  fi
fi

HOST="${TASKITE_PD_NETWORK_HOST:-127.0.0.1}"
NODES=("meta-0" "meta-1" "meta-2")
HTTP_PORT_BASE="${SERVER_PORT:-19001}"
GRPC_PORT_BASE="${TASKITE_PD_NETWORK_GRPC_PORT:-29001}"
RAFT_PORT_BASE="${TASKITE_PD_NETWORK_RAFT_PORT:-29101}"
launcher_validate_ports "${HTTP_PORT_BASE}" "${GRPC_PORT_BASE}" "${RAFT_PORT_BASE}"
HTTP_PORTS=("${HTTP_PORT_BASE}" "$((HTTP_PORT_BASE + 1))" "$((HTTP_PORT_BASE + 2))")
GRPC_PORTS=("${GRPC_PORT_BASE}" "$((GRPC_PORT_BASE + 1))" "$((GRPC_PORT_BASE + 2))")
RAFT_PORTS=("${RAFT_PORT_BASE}" "$((RAFT_PORT_BASE + 1))" "$((RAFT_PORT_BASE + 2))")
launcher_validate_ports "${HTTP_PORTS[@]}" "${GRPC_PORTS[@]}" "${RAFT_PORTS[@]}"
PEER_HOST="${HOST}"
if [[ "${HOST}" == *:* && "${HOST}" != \[*\] ]]; then
  PEER_HOST="[${HOST}]"
fi

stop_cluster() {
  for node in "${NODES[@]}"; do
    pid_file="${PID_DIR}/${node}.pid"
    launcher_stop "${pid_file}" "${node}"
  done
}

clean_cluster() {
  stop_cluster
  echo "wiping data root ${DATA_ROOT}"
  rm -rf "${DATA_ROOT}"
}

# Builds the comma-separated peer list for node index $1: every other node as
# `nodeId@host:grpcPort:raftPort`.
peers_for() {
  local self_index="$1"
  local peers=()
  for i in "${!NODES[@]}"; do
    if [[ "${i}" != "${self_index}" ]]; then
      peers+=("${NODES[i]}@${PEER_HOST}:${GRPC_PORTS[i]}:${RAFT_PORTS[i]}")
    fi
  done
  local IFS=,
  echo "${peers[*]}"
}

start_cluster() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  for node in "${NODES[@]}"; do
    launcher_check_pid "${PID_DIR}/${node}.pid" "${node}"
  done
  JAR="$(launcher_snapshot_jar "${JAR}" "${PID_DIR}/runtime")"
  for i in "${!NODES[@]}"; do
    local node="${NODES[i]}"
    local peers
    peers="$(peers_for "${i}")"
    echo "starting ${node}: http=${HTTP_PORTS[i]} grpc=${GRPC_PORTS[i]} raft=${RAFT_PORTS[i]} peers=${peers}"
    "${JAVA_COMMAND[@]}" -jar "${JAR}" placement-driver \
      --taskite.pd.node-id="${node}" \
      --taskite.pd.network.host="${HOST}" \
      --taskite.pd.network.grpc-port="${GRPC_PORTS[i]}" \
      --taskite.pd.network.raft-port="${RAFT_PORTS[i]}" \
      --taskite.pd.storage.data-dir="${TASKITE_PD_STORAGE_DATA_DIR:-${DATA_ROOT}}/${node}" \
      --taskite.pd.consensus.bootstrap-peers="${peers}" \
      --server.port="${HTTP_PORTS[i]}" \
      --spring.config.additional-location="${ADDITIONAL_CONFIG_LOCATION}" \
      >"${LOG_DIR}/${node}.log" 2>&1 &
    echo "$!" >"${PID_DIR}/${node}.pid"
  done
  echo
  echo "cluster started. logs in ${LOG_DIR}, pids in ${PID_DIR}."
  echo "health check: curl http://${HOST}:${HTTP_PORTS[0]}/api/v1/pd/health"
  echo "metrics:      curl http://${HOST}:${HTTP_PORTS[0]}/actuator/prometheus"
  echo "stop with:    $0 stop"
}

main() {
  local force_build=false
  case "${1:-}" in
    stop)
      stop_cluster
      exit 0
      ;;
    clean)
      clean_cluster
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

  if [[ "${force_build}" == true ]]; then
    echo "clean-building dist jar..."
    mvn -q -f "${REPO_ROOT}/pom.xml" -pl dist -am clean package -DskipTests
  elif [[ ! -f "${JAR}" ]]; then
    echo "building dist jar..."
    mvn -q -f "${REPO_ROOT}/pom.xml" -pl dist -am package -DskipTests
  fi

  start_cluster
}

main "$@"
