#!/usr/bin/env bash
#
# Registers Broker cluster sizing through the placement-driver operations API.
# The operation is idempotent when the requested configuration matches the existing cluster.
#
# Usage:
#   ./scripts/register-broker-cluster.sh \
#     <pd-http-base-url> <cluster-id> <cluster-size> <partition-size> <replication-factor>
#
# Example:
#   ./scripts/register-broker-cluster.sh \
#     http://10.0.0.11:19001 demo 3 5 3
#
# Any reachable PD HTTP endpoint can be used. A follower routes the operation to the Leader.
set -euo pipefail

usage() {
  echo "usage: $0 <pd-http-base-url> <cluster-id> <cluster-size> <partition-size> <replication-factor>" >&2
}

if (($# != 5)); then
  usage
  exit 2
fi

PD_HTTP_BASE="${1%/}"
CLUSTER_ID="$2"
CLUSTER_SIZE="$3"
PARTITION_SIZE="$4"
REPLICATION_FACTOR="$5"

if [[ ! "${PD_HTTP_BASE}" =~ ^https?://[^/]+$ ]]; then
  echo "error: invalid PD HTTP base URL '${PD_HTTP_BASE}'" >&2
  echo "expected: http://host:port" >&2
  exit 2
fi

if [[ ! "${CLUSTER_ID}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "error: cluster-id must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}" >&2
  exit 2
fi

for value in "${CLUSTER_SIZE}" "${PARTITION_SIZE}" "${REPLICATION_FACTOR}"; do
  if [[ ! "${value}" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: cluster-size, partition-size and replication-factor must be positive integers" >&2
    exit 2
  fi
done

if ((REPLICATION_FACTOR > CLUSTER_SIZE)); then
  echo "error: replication-factor must not exceed cluster-size" >&2
  exit 2
fi

command -v curl >/dev/null 2>&1 || {
  echo "error: curl is required" >&2
  exit 1
}

echo "registering Broker cluster ${CLUSTER_ID} through ${PD_HTTP_BASE}"
curl --fail-with-body \
  --silent \
  --show-error \
  --connect-timeout 2 \
  --max-time 10 \
  --request POST \
  --header 'Content-Type: application/json' \
  --data "{\"clusterId\":\"${CLUSTER_ID}\",\"clusterSize\":${CLUSTER_SIZE},\"partitionSize\":${PARTITION_SIZE},\"replicationFactor\":${REPLICATION_FACTOR}}" \
  "${PD_HTTP_BASE}/api/v1/broker/clusters"
echo
