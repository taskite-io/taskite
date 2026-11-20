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
package io.taskite.protocol.impl.record.value.placementdriver.topology;

import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.PartitionStatus;
import java.util.List;
import java.util.Map;

public final class BrokerClusterChangeStatusRecord extends UnifiedRecordValue {
  private String clusterId = "";
  private String changeId = "";
  private ChangeStatus status;
  private boolean terminal;
  private ChangeType changeType = ChangeType.NONE;
  private boolean cancellable;
  private ChangeCancellationReason cancellationReason = ChangeCancellationReason.NONE;
  private List<PartitionPlacement> currentTopology = List.of();
  private List<Operation> plannedChanges = List.of();
  private List<PartitionChange> partitionChanges = List.of();
  private List<PartitionChange> executionHistory = List.of();
  private List<PartitionPlacement> expectedTopology = List.of();

  public String getClusterId() {
    return clusterId;
  }

  public BrokerClusterChangeStatusRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  public String getChangeId() {
    return changeId;
  }

  public BrokerClusterChangeStatusRecord setChangeId(final String changeId) {
    this.changeId = changeId == null ? "" : changeId;
    return this;
  }

  public ChangeStatus getStatus() {
    return status;
  }

  public BrokerClusterChangeStatusRecord setStatus(final ChangeStatus status) {
    this.status = status;
    return this;
  }

  public boolean isTerminal() {
    return terminal;
  }

  public BrokerClusterChangeStatusRecord setTerminal(final boolean terminal) {
    this.terminal = terminal;
    return this;
  }

  public ChangeType getChangeType() {
    return changeType;
  }

  public BrokerClusterChangeStatusRecord setChangeType(final ChangeType changeType) {
    this.changeType = changeType;
    return this;
  }

  public boolean isCancellable() {
    return cancellable;
  }

  public BrokerClusterChangeStatusRecord setCancellable(final boolean cancellable) {
    this.cancellable = cancellable;
    return this;
  }

  public ChangeCancellationReason getCancellationReason() {
    return cancellationReason;
  }

  public BrokerClusterChangeStatusRecord setCancellationReason(
      final ChangeCancellationReason cancellationReason) {
    this.cancellationReason = cancellationReason;
    return this;
  }

  public List<PartitionPlacement> getCurrentTopology() {
    return currentTopology;
  }

  public BrokerClusterChangeStatusRecord setCurrentTopology(
      final List<PartitionPlacement> currentTopology) {
    this.currentTopology = List.copyOf(currentTopology == null ? List.of() : currentTopology);
    return this;
  }

  public List<Operation> getPlannedChanges() {
    return plannedChanges;
  }

  public BrokerClusterChangeStatusRecord setPlannedChanges(final List<Operation> plannedChanges) {
    this.plannedChanges = List.copyOf(plannedChanges == null ? List.of() : plannedChanges);
    return this;
  }

  public List<PartitionChange> getPartitionChanges() {
    return partitionChanges;
  }

  public BrokerClusterChangeStatusRecord setPartitionChanges(
      final List<PartitionChange> partitionChanges) {
    this.partitionChanges = List.copyOf(partitionChanges == null ? List.of() : partitionChanges);
    return this;
  }

  public List<PartitionPlacement> getExpectedTopology() {
    return expectedTopology;
  }

  public List<PartitionChange> getExecutionHistory() {
    return executionHistory;
  }

  public BrokerClusterChangeStatusRecord setExecutionHistory(final List<PartitionChange> history) {
    executionHistory = List.copyOf(history == null ? List.of() : history);
    return this;
  }

  public BrokerClusterChangeStatusRecord setExpectedTopology(
      final List<PartitionPlacement> expectedTopology) {
    this.expectedTopology = List.copyOf(expectedTopology == null ? List.of() : expectedTopology);
    return this;
  }

  public record Operation(
      String opId, TopologyOperationType type, String targetBroker, int priority) {}

  public record PartitionPlacement(
      int partitionId,
      PartitionStatus status,
      List<String> desiredVoters,
      List<String> currentVoters,
      List<String> learners,
      String desiredPrimary,
      Map<String, Integer> priorities) {}

  public record PartitionChange(
      String changeId,
      int partitionId,
      ChangeStatus status,
      int nextOperationIndex,
      int operationCount,
      boolean terminal,
      Operation currentOperation,
      List<Operation> remainingOperations,
      String startedAt,
      String deadline,
      long executionGeneration,
      long authorityIndex,
      String rootChangeId,
      String supersedes,
      String supersededBy,
      PlanBlockedReason blockedReason,
      String blockedSince) {
    public PartitionChange(
        final String changeId,
        final int partitionId,
        final ChangeStatus status,
        final int nextOperationIndex,
        final int operationCount,
        final boolean terminal,
        final Operation currentOperation,
        final List<Operation> remainingOperations,
        final String startedAt,
        final String deadline) {
      this(
          changeId,
          partitionId,
          status,
          nextOperationIndex,
          operationCount,
          terminal,
          currentOperation,
          remainingOperations,
          startedAt,
          deadline,
          0,
          0,
          changeId,
          "",
          "",
          PlanBlockedReason.NONE,
          null);
    }
  }
}
