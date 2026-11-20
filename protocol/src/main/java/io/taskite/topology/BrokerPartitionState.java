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
package io.taskite.topology;

import java.util.Objects;

public record BrokerPartitionState(
    int partitionId,
    Status status,
    Replication replication,
    Operation operation,
    BrokerPartitionElectionState election) {

  public BrokerPartitionState(final int partitionId, final MemberRole role, final long term) {
    this(
        partitionId,
        new Status(
            role,
            term,
            role == MemberRole.LEARNER
                ? BrokerPartitionLifecycle.CATCHING_UP
                : BrokerPartitionLifecycle.ACTIVE,
            -1,
            0),
        new Replication(0, Long.MAX_VALUE, false),
        new Operation("", BrokerTopologyOperationStatus.NONE),
        BrokerPartitionElectionState.UNKNOWN);
  }

  public BrokerPartitionState {
    Objects.requireNonNull(status, "status");
    Objects.requireNonNull(replication, "replication");
    Objects.requireNonNull(operation, "operation");
    election = election == null ? BrokerPartitionElectionState.UNKNOWN : election;
  }

  public MemberRole role() {
    return status.role();
  }

  public long term() {
    return status.term();
  }

  public BrokerPartitionLifecycle lifecycleState() {
    return status.lifecycleState();
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

  public BrokerPartitionLifecycle lifecycle() {
    return lifecycleState();
  }

  public BrokerTopologyOperationStatus operationStatus() {
    return lastOpStatus();
  }

  public record Status(
      MemberRole role,
      long term,
      BrokerPartitionLifecycle lifecycleState,
      int priority,
      long recoveryEpoch) {
    public Status {
      role = role == null ? MemberRole.UNKNOWN : role;
      lifecycleState = lifecycleState == null ? BrokerPartitionLifecycle.ABSENT : lifecycleState;
      recoveryEpoch = Math.max(0, recoveryEpoch);
    }
  }

  public record Replication(long matchIndex, long lag, boolean readyForPromotion) {
    public Replication {
      lag = Math.max(0, lag);
    }
  }

  public record Operation(String id, BrokerTopologyOperationStatus status) {
    public Operation {
      id = id == null ? "" : id;
      status = status == null ? BrokerTopologyOperationStatus.NONE : status;
    }
  }
}
