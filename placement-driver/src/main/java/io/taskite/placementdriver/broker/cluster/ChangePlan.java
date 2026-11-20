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
package io.taskite.placementdriver.broker.cluster;

import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import java.util.List;
import java.util.Objects;

/**
 * A PD-persisted plan that moves one partition from its source membership to the target one, as an
 * ordered list of {@link TopologyOperation}s. The plan advances one operation at a time ({@code
 * nextOperationIndex}); the {@link BrokerPartitionChangeStatus} tracks lifecycle so a restarted PD
 * can resume it from event-sourced state.
 *
 * <p>Single-partition serialization: at most one non-terminal plan may exist per partition
 * (enforced via the pending index), so operations for a partition never interleave.
 */
public record ChangePlan(
    Definition definition, Progress progress, Deadline deadline, Execution execution) {

  public ChangePlan(final Definition definition, final Progress progress, final Deadline deadline) {
    this(definition, progress, deadline, Execution.LEGACY);
  }

  public record Execution(
      long generation,
      long authorityIndex,
      String predecessor,
      String successor,
      String root,
      PlanBlockedReason blockedReason,
      long blockedSinceEpochMillis) {
    public static final Execution LEGACY =
        new Execution(0, 0, "", "", "", PlanBlockedReason.NONE, 0);

    public Execution {
      Objects.requireNonNull(predecessor, "predecessor");
      Objects.requireNonNull(successor, "successor");
      Objects.requireNonNull(root, "root");
      Objects.requireNonNull(blockedReason, "blockedReason");
      if (generation < 0 || authorityIndex < 0) {
        throw new IllegalArgumentException("negative execution authority");
      }
    }
  }

  public ChangePlan {
    Objects.requireNonNull(definition, "definition");
    Objects.requireNonNull(progress, "progress");
    Objects.requireNonNull(deadline, "deadline");
    Objects.requireNonNull(execution, "execution");
  }

  public String clusterId() {
    return definition.identity().clusterId();
  }

  public String changeId() {
    return definition.identity().changeId();
  }

  public int partitionId() {
    return definition.identity().partitionId();
  }

  public long sourceTopologyVersion() {
    return definition.versions().source();
  }

  public long targetTopologyVersion() {
    return definition.versions().target();
  }

  public List<TopologyOperation> operations() {
    return definition.operations();
  }

  public enum Purpose {
    MIGRATION,
    BOOTSTRAP
  }

  public Purpose purpose() {
    return definition.purpose();
  }

  public int nextOperationIndex() {
    return progress.nextOperationIndex();
  }

  public BrokerPartitionChangeStatus status() {
    return progress.status();
  }

  public long startedAtEpochMillis() {
    return deadline.startedAtEpochMillis();
  }

  public long deadlineEpochMillis() {
    return deadline.deadlineEpochMillis();
  }

  public boolean isTerminal() {
    return status() == BrokerPartitionChangeStatus.COMPLETED
        || status() == BrokerPartitionChangeStatus.FAILED
        || status() == BrokerPartitionChangeStatus.SUPERSEDED;
  }

  public boolean hasMoreOperations() {
    return nextOperationIndex() < operations().size();
  }

  /** Explicit epoch-fenced recovery has its own admission path, independent of migration width. */
  public boolean isQuorumRecovery() {
    return operations().size() == 1
        && operations().getFirst().type() == TopologyOperationType.RESET_PEERS
        && operations().getFirst().priority() > 0;
  }

  public TopologyOperation nextOperation() {
    return operations().get(nextOperationIndex());
  }

  /** Returns a copy advanced past the current operation, marking COMPLETED when none remain. */
  public ChangePlan advanced() {
    final int advancedIndex = nextOperationIndex() + 1;
    final BrokerPartitionChangeStatus nextStatus =
        advancedIndex >= operations().size()
            ? BrokerPartitionChangeStatus.COMPLETED
            : BrokerPartitionChangeStatus.IN_PROGRESS;
    final var progressed =
        new Execution(
            execution.generation(),
            execution.authorityIndex(),
            execution.predecessor(),
            execution.successor(),
            execution.root(),
            PlanBlockedReason.NONE,
            0);
    return new ChangePlan(
        definition, new Progress(advancedIndex, nextStatus), deadline, progressed);
  }

  public ChangePlan withStatus(final BrokerPartitionChangeStatus newStatus) {
    return new ChangePlan(
        definition, new Progress(nextOperationIndex(), newStatus), deadline, execution);
  }

  public ChangePlan withDeadline(final long startedAt, final long deadline) {
    return new ChangePlan(definition, progress, new Deadline(startedAt, deadline), execution);
  }

  public ChangePlan withExecution(final Execution execution) {
    return new ChangePlan(definition, progress, deadline, execution);
  }

  public String rootChangeId() {
    return execution.root().isBlank() ? changeId() : execution.root();
  }

  public record Identity(String clusterId, String changeId, int partitionId) {}

  public record Versions(long source, long target) {}

  public record Deadline(long startedAtEpochMillis, long deadlineEpochMillis) {
    public static final Deadline NONE = new Deadline(0, 0);
  }

  public record Progress(int nextOperationIndex, BrokerPartitionChangeStatus status) {
    public Progress {
      status = status == null ? BrokerPartitionChangeStatus.PENDING : status;
    }
  }

  public record Definition(
      Identity identity, Versions versions, List<TopologyOperation> operations, Purpose purpose) {
    public Definition(
        final Identity identity,
        final Versions versions,
        final List<TopologyOperation> operations) {
      this(identity, versions, operations, Purpose.MIGRATION);
    }

    public Definition {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(versions, "versions");
      Objects.requireNonNull(purpose, "purpose");
      final String prefix = identity.changeId() + ":";
      operations =
          (operations == null ? List.<TopologyOperation>of() : operations)
              .stream()
                  .map(
                      operation ->
                          operation.opId().startsWith(prefix)
                              ? operation
                              : new TopologyOperation(
                                  prefix + operation.opId(),
                                  operation.type(),
                                  operation.targetBroker(),
                                  operation.priority(),
                                  operation.configuration()))
                  .toList();
    }
  }
}
