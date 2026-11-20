/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.placementdriver.broker.topology;

import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import java.util.Objects;

/** Persisted PD observation of one broker's replica for a partition. */
public record ReplicaState(
    BrokerPartitionState.Status status,
    BrokerPartitionState.Replication replication,
    BrokerPartitionState.Operation operation,
    long lastReportTimeEpochMillis,
    BrokerPartitionElectionState election) {

  public ReplicaState {
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(replication, "replication");
    Objects.requireNonNull(operation, "operation");
    election = election == null ? BrokerPartitionElectionState.UNKNOWN : election;
  }

  public MemberRole role() {
    return status.role();
  }

  public ReplicaLifecycleState lifecycle() {
    return ReplicaLifecycleState.valueOf(status.lifecycleState().name());
  }

  public int priority() {
    return status.priority();
  }

  public long recoveryEpoch() {
    return status.recoveryEpoch();
  }

  public long matchIndex() {
    return replication.matchIndex();
  }

  public long lag() {
    return replication.lag();
  }

  public boolean readyForPromotion() {
    return replication.readyForPromotion();
  }

  public String lastOpId() {
    return operation.id();
  }

  public BrokerTopologyOperationStatus lastOpStatus() {
    return operation.status();
  }

  public static ReplicaState observed(
      final BrokerPartitionState state, final long reportTimeEpochMillis) {
    return new ReplicaState(
        state.status(),
        state.replication(),
        state.operation(),
        reportTimeEpochMillis,
        state.election());
  }
}
