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
package io.taskite.placementdriver.state.immutable;

import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Key;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Task;
import java.util.List;
import java.util.Optional;

public interface TopologyMembershipChangeState {
  Optional<Task> task(Key key);

  List<Task> tasks(Scope scope, String clusterId, String changeId);

  Optional<PartitionExecutionRecord.Schedule> partition(String clusterId, int partitionId);

  Optional<MemberExecutionRecord.Schedule> member(MemberChangeScope scope, String clusterId);

  List<Task> dueBefore(long now, int limit);

  long nextDueAt();

  int reserved(long now, int limit);

  static Scope scope(final MemberChangeScope scope) {
    return scope == MemberChangeScope.BROKER ? Scope.BROKER_MEMBER : Scope.PLACEMENT_DRIVER_MEMBER;
  }

  static PartitionExecutionRecord.Plan identity(final ChangePlan plan) {
    return new PartitionExecutionRecord.Plan(
        plan.clusterId(),
        plan.partitionId(),
        plan.changeId(),
        plan.status().name(),
        plan.nextOperationIndex(),
        plan.execution().generation(),
        plan.execution().authorityIndex(),
        plan.execution().successor());
  }

  static MemberExecutionRecord.Operation identity(final MemberChangeRecord change) {
    final int index = change.getNextOperationIndex();
    final var operation =
        index < change.getOperations().size() ? change.getOperations().get(index) : null;
    return new MemberExecutionRecord.Operation(
        change.getResourceId(),
        change.getChangeId(),
        change.getSourceEpoch(),
        change.getTargetEpoch(),
        index,
        operation == null ? "" : operation.getOperationId(),
        operation == null ? null : operation.getType(),
        operation == null ? "" : operation.getMemberId(),
        operation == null ? 0 : operation.getBarrierIndex());
  }
}
