#!/usr/bin/env bash
#
# Launches a local 3-broker cluster (clusterSize=3, partitionSize=5,
# replicationFactor=3). The script registers the cluster with the
# placement-driver before starting brokers, so start the PD cluster first
# (scripts/start-placement-driver-cluster.sh).
#
# `TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS` is a list of PD gRPC endpoints
# `host:grpcPort` (NO `http://` scheme), matching the PD cluster's gRPC ports.
# The registration HTTP base defaults to http://127.0.0.1:19001 and can be
# overridden with TASKITE_LAUNCHER_PD_HTTP_BASE.
#
# Ports per broker:
#   broker      client-port   raft-port   management-port
#   broker-0    26501         26601       9600
#   broker-1    26502         26602       9601
#   broker-2    26503         26603       9602
#
# Usage:
#   ./scripts/start-broker-cluster.sh           # build (if needed) + start all 3
#   ./scripts/start-broker-cluster.sh --build   # force a rebuild first
#   ./scripts/start-broker-cluster.sh stop      # stop all brokers
#   ./scripts/start-broker-cluster.sh clean      # stop all brokers + wipe data dirs
#
# NOTE: persisted data under DATA_ROOT is tied to the current SBE schema. After a
# schema/encoding change, run `clean` first or startup may fail decoding stale records.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${REPO_ROOT}/scripts/launcher-common.sh"
JAR="${TASKITE_LAUNCHER_JAR:-${REPO_ROOT}/dist/target/taskite-distribution-0.1.0-SNAPSHOT.jar}"
DATA_ROOT="${TASKITE_LAUNCHER_DATA_ROOT:-/tmp/broker-cluster}"
LOG_DIR="${TASKITE_LAUNCHER_LOG_DIR:-${DATA_ROOT}/logs}"
PID_DIR="${TASKITE_LAUNCHER_PID_DIR:-${DATA_ROOT}/pids}"
JAVA_BIN="${TASKITE_LAUNCHER_JAVA_BIN:-java}"
JAVA_COMMAND=("${JAVA_BIN}")
if [[ -n "${JAVA_OPTS:-}" ]]; then
  read -r -a user_java_options <<<"${JAVA_OPTS}"
  if ((${#user_java_options[@]} > 0)); then
    JAVA_COMMAND+=("${user_java_options[@]}")
  fi
fi

HOST="${TASKITE_BROKER_NETWORK_HOST:-127.0.0.1}"
CLUSTER_ID="${TASKITE_BROKER_CLUSTER_CLUSTER_ID:-demo}"
PARTITION_SIZE=5
REPLICATION_FACTOR=3

NODES=("broker-0" "broker-1" "broker-2")
PORT_BASE="${TASKITE_BROKER_NETWORK_COMMAND_PORT:-26501}"
RAFT_PORT_BASE="${TASKITE_BROKER_NETWORK_RAFT_PORT:-26601}"
launcher_validate_ports "${PORT_BASE}" "${RAFT_PORT_BASE}" "${MANAGEMENT_SERVER_PORT:-9600}"
PORTS=("${PORT_BASE}" "$((PORT_BASE + 1))" "$((PORT_BASE + 2))")
RAFT_PORTS=("${RAFT_PORT_BASE}" "$((RAFT_PORT_BASE + 1))" "$((RAFT_PORT_BASE + 2))")
MANAGEMENT_PORT_BASE="${MANAGEMENT_SERVER_PORT:-9600}"
MANAGEMENT_PORTS=(
  "${MANAGEMENT_PORT_BASE}"
  "$((MANAGEMENT_PORT_BASE + 1))"
  "$((MANAGEMENT_PORT_BASE + 2))"
)
launcher_validate_ports "${PORTS[@]}" "${RAFT_PORTS[@]}" "${MANAGEMENT_PORTS[@]}"

# PD gRPC endpoints the brokers register with (matches start-placement-driver-cluster.sh).
PLACEMENT_DRIVERS="${TASKITE_BROKER_REGISTRATION_PLACEMENT_DRIVERS:-127.0.0.1:29001,127.0.0.1:29002,127.0.0.1:29003}"
PD_HTTP_BASE="${TASKITE_LAUNCHER_PD_HTTP_BASE:-http://127.0.0.1:19001}"

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

start_cluster() {
  mkdir -p "${LOG_DIR}" "${PID_DIR}"
  for node in "${NODES[@]}"; do
    launcher_check_pid "${PID_DIR}/${node}.pid" "${node}"
  done
  JAR="$(launcher_snapshot_jar "${JAR}" "${PID_DIR}/runtime")"
  "${REPO_ROOT}/scripts/register-broker-cluster.sh" \
    "${PD_HTTP_BASE}" "${CLUSTER_ID}" "${#NODES[@]}" "${PARTITION_SIZE}" "${REPLICATION_FACTOR}"
  for i in "${!NODES[@]}"; do
    local node="${NODES[i]}"
    echo "starting ${node}: port=${PORTS[i]} raftPort=${RAFT_PORTS[i]} managementPort=${MANAGEMENT_PORTS[i]}"
    "${JAVA_COMMAND[@]}" -jar "${JAR}" broker \
      --taskite.broker.cluster.broker-id="${node}" \
      --taskite.broker.network.host="${HOST}" \
      --taskite.broker.network.command-port="${PORTS[i]}" \
      --taskite.broker.network.raft-port="${RAFT_PORTS[i]}" \
      --management.server.port="${MANAGEMENT_PORTS[i]}" \
      --taskite.broker.cluster.cluster-id="${CLUSTER_ID}" \
      --taskite.broker.registration.placement-drivers="${PLACEMENT_DRIVERS}" \
      --taskite.broker.storage.data-dir="${TASKITE_BROKER_STORAGE_DATA_DIR:-${DATA_ROOT}}" \
      >"${LOG_DIR}/${node}.log" 2>&1 &
    echo "$!" >"${PID_DIR}/${node}.pid"
  done
  echo
  echo "broker cluster started. logs in ${LOG_DIR}, pids in ${PID_DIR}."
  echo "topology check: curl ${PD_HTTP_BASE%/}/api/v1/broker/clusters/${CLUSTER_ID}/topology"
  echo "metrics:"
  for i in "${!NODES[@]}"; do
    echo "  ${NODES[i]} http://${HOST}:${MANAGEMENT_PORTS[i]}/actuator/prometheus"
  done
  echo "stop with:      $0 stop"
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
