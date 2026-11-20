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
package io.taskite.placementdriver.common.mapper;

import static io.taskite.placementdriver.broker.cluster.TopologyOperationType.parse;
import static io.taskite.protocol.record.BrokerPartitionChangeStatus.valueOf;

import io.taskite.placementdriver.broker.cluster.BrokerOperationConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.BrokerPartitionChangePurpose;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RecoveryFence;
import java.util.ArrayList;
import java.util.List;

/** Conversion between the immutable PD model and its event-log record value. */
public final class BrokerPartitionChangeRecordMapper {
  private BrokerPartitionChangeRecordMapper() {}

  public static PartitionChangeRecord fromPlan(final ChangePlan plan) {
    final List<PartitionChangeRecord.Operation> operations = new ArrayList<>();
    for (final TopologyOperation operation : plan.operations()) {
      operations.add(
          new PartitionChangeRecord.Operation(
              operation.opId(),
              TopologyOperationType.valueOf(operation.type().name()).value(),
              operation.targetBroker(),
              operation.priority(),
              toRecordConfiguration(operation.configuration())));
    }
    return new PartitionChangeRecord()
        .setClusterId(plan.clusterId())
        .setChangeId(plan.changeId())
        .setPartitionId(plan.partitionId())
        .setSourceTopologyVersion(plan.sourceTopologyVersion())
        .setTargetTopologyVersion(plan.targetTopologyVersion())
        .setNextOperationIndex(plan.nextOperationIndex())
        .setStartedAtEpochMillis(plan.startedAtEpochMillis())
        .setDeadlineEpochMillis(plan.deadlineEpochMillis())
        .setExecutionGeneration(plan.execution().generation())
        .setAuthorityIndex(plan.execution().authorityIndex())
        .setPredecessorChangeId(plan.execution().predecessor())
        .setSuccessorChangeId(plan.execution().successor())
        .setRootChangeId(plan.execution().root())
        .setBlockedReason(plan.execution().blockedReason())
        .setBlockedSinceEpochMillis(plan.execution().blockedSinceEpochMillis())
        .setPurpose(BrokerPartitionChangePurpose.valueOf(plan.purpose().name()))
        .setStatus(valueOf(plan.status().name()))
        .setOperations(operations);
  }

  public static ChangePlan toPlan(
      final PartitionChangeRecord record, final BrokerPartitionChangeStatus status) {
    final List<TopologyOperation> operations = new ArrayList<>();
    for (final PartitionChangeRecord.Operation operation : record.getOperations()) {
      final TopologyOperationType wireType = TopologyOperationType.get(operation.type());
      if (wireType == TopologyOperationType.NULL_VAL
          || wireType == TopologyOperationType.SBE_UNKNOWN) {
        throw new IllegalArgumentException("unknown topology operation type: " + operation.type());
      }
      operations.add(
          new TopologyOperation(
              operation.opId(),
              parse(wireType.name()),
              operation.targetBroker(),
              operation.priority(),
              toConfiguration(operation.configuration())));
    }
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(
                record.getClusterId(), record.getChangeId(), record.getPartitionId()),
            new ChangePlan.Versions(
                record.getSourceTopologyVersion(), record.getTargetTopologyVersion()),
            operations,
            ChangePlan.Purpose.valueOf(record.getPurpose().name())),
        new ChangePlan.Progress(record.getNextOperationIndex(), status),
        new ChangePlan.Deadline(record.getStartedAtEpochMillis(), record.getDeadlineEpochMillis()),
        new ChangePlan.Execution(
            record.getExecutionGeneration(),
            record.getAuthorityIndex(),
            record.getPredecessorChangeId(),
            record.getSuccessorChangeId(),
            record.getRootChangeId(),
            record.getBlockedReason(),
            record.getBlockedSinceEpochMillis()));
  }

  private static PartitionChangeRecord.Configuration toRecordConfiguration(
      final BrokerOperationConfiguration configuration) {
    if (configuration == null) {
      return null;
    }
    return new PartitionChangeRecord.Configuration(
        configuration.sourceMembers().stream()
            .map(BrokerPartitionChangeRecordMapper::toRecordMember)
            .toList(),
        configuration.targetMembers().stream()
            .map(BrokerPartitionChangeRecordMapper::toRecordMember)
            .toList(),
        configuration.expectedMembershipEpoch(),
        configuration.recoveryFence());
  }

  private static PartitionChangeRecord.Member toRecordMember(final BrokerPartitionMember member) {
    return new PartitionChangeRecord.Member(member.brokerId(), member.voter(), member.priority());
  }

  private static BrokerOperationConfiguration toConfiguration(
      final PartitionChangeRecord.Configuration configuration) {
    if (configuration == null) {
      return null;
    }
    return new BrokerOperationConfiguration(
        configuration.sourceMembers().stream()
            .map(BrokerPartitionChangeRecordMapper::toMember)
            .toList(),
        configuration.targetMembers().stream()
            .map(BrokerPartitionChangeRecordMapper::toMember)
            .toList(),
        configuration.expectedMembershipEpoch(),
        new RecoveryFence(
            configuration.expectedRecoveryEpoch(),
            configuration.targetRecoveryEpoch(),
            configuration.fencingProofId()));
  }

  private static BrokerPartitionMember toMember(final PartitionChangeRecord.Member member) {
    return new BrokerPartitionMember(member.brokerId(), member.voter(), member.priority());
  }
}
