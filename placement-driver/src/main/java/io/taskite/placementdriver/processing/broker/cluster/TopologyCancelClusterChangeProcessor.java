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
package io.taskite.placementdriver.processing.broker.cluster;

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;

/** Validates and atomically cancels cluster, member, or standalone partition changes. */
public final class TopologyCancelClusterChangeProcessor
    implements TypedRecordProcessor<ClusterConfigRecord> {
  private final Writers writers;
  private static final String CHANGE_NOT_FOUND =
      "CHANGE_NOT_FOUND: Expected to cancel topology change '%s' in cluster '%s', but no matching change was found";
  private static final String UNSAFE_CANCELLATION =
      "UNSAFE_CONFIGURATION_CANCELLATION: Expected to cancel topology change '%s' in cluster '%s', but restoring its source broker count would be below the current replicationFactor";
  private static final String CANCELLATION_REJECTED =
      "%s: Expected to cancel topology change '%s' in cluster '%s', but its current plan state does not allow cancellation";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;

  public TopologyCancelClusterChangeProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
  }

  @Override
  public void processRecord(final TypedRecord<ClusterConfigRecord> record) {
    final ClusterConfigRecord value = record.getValue();
    final var target =
        BrokerClusterChangeTarget.resolve(state, value.getClusterId(), value.getChangeId());
    if (target.isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          CHANGE_NOT_FOUND.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    final var change = target.get();
    if (change.cancellationReason(state)
        == ChangeCancellationReason.UNSAFE_CONFIGURATION_CANCELLATION) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          UNSAFE_CANCELLATION.formatted(value.getChangeId(), value.getClusterId()));
      return;
    }
    if (change.clusterChange() == null && change.memberChange() == null) {
      final ChangeCancellationReason reason = change.cancellationReason(state);
      if (reason != ChangeCancellationReason.NONE) {
        reject(
            record,
            RejectionType.INVALID_STATE,
            CANCELLATION_REJECTED.formatted(
                reason.wireValue(), value.getChangeId(), value.getClusterId()));
        return;
      }
      change.plans().stream()
          .filter(plan -> !plan.isTerminal())
          .forEach(
              plan ->
                  writers
                      .state()
                      .appendFollowUpEvent(
                          keyGenerator.nextKey(),
                          ValueType.PARTITION_CHANGE,
                          BrokerPartitionChangeIntent.CANCELLED,
                          BrokerPartitionChangeRecordMapper.fromPlan(plan)));
      // Preserve the public command response type without inventing an aggregate state event.
      writers
          .response()
          .writeEventOnCommand(
              record.getKey(), TopologyIntent.CLUSTER_CHANGE_CANCELLED, value, record);
      return;
    }
    if (change.clusterChange() == null) {
      if (!appendMemberCancellation(value, true)) {
        reject(
            record,
            RejectionType.NOT_FOUND,
            CHANGE_NOT_FOUND.formatted(value.getChangeId(), value.getClusterId()));
      } else {
        writers
            .response()
            .writeEventOnCommand(
                record.getKey(), TopologyIntent.CLUSTER_CHANGE_CANCELLED, value, record);
      }
      return;
    }
    if (change.clusterChange().status().isTerminal()) {
      writers
          .response()
          .writeEventOnCommand(
              record.getKey(), TopologyIntent.CLUSTER_CHANGE_CANCELLED, value, record);
      return;
    }
    appendMemberCancellation(value, false);
    writers
        .state()
        .appendFollowUpEvent(record.getKey(), TopologyIntent.CLUSTER_CHANGE_CANCELLED, value);
    writers
        .response()
        .writeEventOnCommand(
            record.getKey(), TopologyIntent.CLUSTER_CHANGE_CANCELLED, value, record);
  }

  private void reject(
      final TypedRecord<ClusterConfigRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }

  private boolean appendMemberCancellation(
      final ClusterConfigRecord value, final boolean cancelChildren) {
    final var memberChange =
        state
            .getMemberChangeState()
            .get(MemberChangeScope.BROKER, value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (memberChange == null) {
      return false;
    }
    if (!memberChange.isTerminal()) {
      if (cancelChildren) {
        memberChange
            .getChildPartitionChangeIds()
            .forEach(
                changeId ->
                    state
                        .getBrokerTopologyChangeState()
                        .currentDescendant(value.getClusterId(), changeId)
                        .filter(plan -> !plan.isTerminal())
                        .filter(plan -> plan.execution().predecessor().isBlank())
                        .filter(plan -> plan.status() != BrokerPartitionChangeStatus.SUPERSEDING)
                        .ifPresent(
                            plan ->
                                writers
                                    .state()
                                    .appendFollowUpEvent(
                                        keyGenerator.nextKey(),
                                        ValueType.PARTITION_CHANGE,
                                        BrokerPartitionChangeIntent.CANCELLED,
                                        BrokerPartitionChangeRecordMapper.fromPlan(plan))));
      }
      final var cancelled = memberChange.copy().setStatus(MemberChangeStatus.CANCELLED);
      cancelled.getOperations().stream()
          .filter(operation -> operation.getStatus() == MemberChangeOperationStatus.PENDING)
          .forEach(operation -> operation.setStatus(MemberChangeOperationStatus.CANCELLED));
      writers
          .state()
          .appendFollowUpEvent(
              keyGenerator.nextKey(),
              ValueType.MEMBER_CHANGE,
              MemberChangeIntent.CANCELLED,
              cancelled);
    }
    return true;
  }
}
