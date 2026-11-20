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
package io.taskite.placementdriver.state.topology;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbForeignKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.placementdriver.state.mutable.MutableTopologyMembershipChangeState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Key;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Status;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Task;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/*
 * 241: [scope, cluster, change, partition, operation] ->   task status and concrete execution.
 * 242: [kind, deadline, FK -> 241] -> nil; kind 0 observes tasks, kind 1 reserves RPC quota.
 * 243: [scope, cluster, partition] -> FK -> 241. Terminal tasks retain history without indexes.
 */
public final class DbTopologyMembershipChangeState implements MutableTopologyMembershipChangeState {
  // [scope, clusterId, changeId, partitionId, operationId]: task identity.
  private final TopologyChangeTaskKey taskKey;
  // Task FK -> PD_TOPOLOGY_CHANGE_TASKS: active pointer value.
  private final DbForeignKey<TopologyChangeTaskKey> taskReference;
  // [scope, clusterId, partitionId]: active task lookup key.
  private final DbCompositeKey<DbInt, DbCompositeKey<DbString, DbInt>> resourceKey;
  // [kind, dueAt, task FK -> PD_TOPOLOGY_CHANGE_TASKS]: operation key.
  private final DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbForeignKey<TopologyChangeTaskKey>>>
      deadlineKey;
  // PD_TOPOLOGY_CHANGE_TASKS: [scope, clusterId, changeId, partitionId, operationId]
  // -> TopologyChangeTaskRecord bytes. Empty operationId represents the plan's final wait step.
  private final ColumnFamily<TopologyChangeTaskKey, TopologyChangeTaskRecordValue>
      tasksColumnFamilies;
  // PD_TOPOLOGY_CHANGE_TASK_DUE_DATES: [kind, dueAt, task FK -> PD_TOPOLOGY_CHANGE_TASKS] -> nil.
  // kind 0 scans due tasks; kind 1 counts RPC reservations until their deadlines.
  private final ColumnFamily<
          DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbForeignKey<TopologyChangeTaskKey>>>, DbNil>
      deadlinesColumnFamilies;
  // PD_ACTIVE_TOPOLOGY_CHANGE_TASKS: [scope, clusterId, partitionId]
  // -> task FK -> PD_TOPOLOGY_CHANGE_TASKS. Member changes use the synthetic partitionId 1.
  private final ColumnFamily<
          DbCompositeKey<DbInt, DbCompositeKey<DbString, DbInt>>,
          DbForeignKey<TopologyChangeTaskKey>>
      activeColumnFamilies;

  public DbTopologyMembershipChangeState(final ProcessingStateStore storage) {
    taskKey = new TopologyChangeTaskKey();
    taskReference =
        new DbForeignKey<>(new TopologyChangeTaskKey(), ColumnFamilies.PD_TOPOLOGY_CHANGE_TASKS);
    resourceKey =
        new DbCompositeKey<>(new DbInt(), new DbCompositeKey<>(new DbString(), new DbInt()));
    deadlineKey =
        new DbCompositeKey<>(
            new DbInt(),
            new DbCompositeKey<>(
                new DbLong(),
                new DbForeignKey<>(
                    new TopologyChangeTaskKey(), ColumnFamilies.PD_TOPOLOGY_CHANGE_TASKS)));

    tasksColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_TOPOLOGY_CHANGE_TASKS, taskKey, new TopologyChangeTaskRecordValue());
    deadlinesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_TOPOLOGY_CHANGE_TASK_DUE_DATES, deadlineKey, DbNil.INSTANCE);
    activeColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_ACTIVE_TOPOLOGY_CHANGE_TASKS, resourceKey, taskReference);
  }

  @Override
  public Optional<Task> task(final Key key) {
    return decode(
        tasksColumnFamilies.get(taskKey.wrapKey(key), TopologyChangeTaskRecordValue::new));
  }

  @Override
  public List<Task> tasks(final Scope scope, final String clusterId, final String changeId) {
    final List<Task> result = new ArrayList<>();
    tasksColumnFamilies.whileTrue(
        taskKey.wrapKey(scope, clusterId, changeId, 0, ""),
        (key, value) -> {
          final var identity = key.key();
          if (identity.scope() != scope
              || !identity.clusterId().equals(clusterId)
              || !identity.changeId().equals(changeId)) {
            return false;
          }
          decode(value).ifPresent(result::add);
          return true;
        });
    return result;
  }

  @Override
  public Optional<PartitionExecutionRecord.Schedule> partition(
      final String clusterId, final int partitionId) {
    return current(Scope.BROKER_PARTITION, clusterId, partitionId).map(Task::partitionSchedule);
  }

  @Override
  public Optional<MemberExecutionRecord.Schedule> member(
      final MemberChangeScope scope, final String clusterId) {
    return current(TopologyMembershipChangeState.scope(scope), clusterId, 1)
        .map(Task::memberSchedule);
  }

  @Override
  public List<Task> dueBefore(final long now, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final List<Task> due = new ArrayList<>();
    deadlinesColumnFamilies.whileTrue(
        (key, ignored) -> {
          if (key.first().getValue() != 0 || key.second().first().getValue() > now) {
            return false;
          }
          task(key.second().second().inner().key()).ifPresent(due::add);
          return due.size() < limit;
        });
    return due;
  }

  @Override
  public long nextDueAt() {
    final long[] next = {-1};
    deadlinesColumnFamilies.whileTrue(
        (key, ignored) -> {
          if (key.first().getValue() == 0) {
            next[0] = key.second().first().getValue();
          }
          return false;
        });
    return next[0];
  }

  @Override
  public int reserved(final long now, final int limit) {
    if (limit <= 0 || now == Long.MAX_VALUE) {
      return 0;
    }
    final int[] count = {0};
    deadlinesColumnFamilies.whileTrue(
        wrapDueKey(1, now + 1, Scope.BROKER_PARTITION, "", "", 0, ""),
        (key, ignored) -> {
          if (key.first().getValue() != 1) {
            return false;
          }
          return ++count[0] < limit;
        });
    return count[0];
  }

  @Override
  public void markPartition(final ChangePlan plan, final boolean executable) {
    final int size = plan.operations().size();
    for (int index = 0; index <= size; index++) {
      final String operationId = index == size ? "" : plan.operations().get(index).opId();
      final var key =
          new Key(
              Scope.BROKER_PARTITION,
              plan.clusterId(),
              plan.changeId(),
              plan.partitionId(),
              operationId);
      final var identity = TopologyMembershipChangeState.identity(plan);
      final var schedule =
          new PartitionExecutionRecord.Schedule(
              new PartitionExecutionRecord.Plan(
                  identity.clusterId(),
                  identity.partitionId(),
                  identity.changeId(),
                  identity.status(),
                  index,
                  identity.generation(),
                  identity.authorityIndex(),
                  identity.successor()),
              PartitionExecutionRecord.Phase.READY,
              0,
              0,
              0,
              null,
              null);
      final Status status =
          index < plan.nextOperationIndex()
              ? Status.COMPLETED
              : plan.isTerminal()
                  ? terminalStatus(plan.status().name())
                  : index == plan.nextOperationIndex() && executable
                      ? Status.IN_PROGRESS
                      : Status.PENDING;
      final var previous = task(key).orElse(null);
      final var next =
          previous != null
                  && (index != plan.nextOperationIndex()
                      || previous.partitionSchedule().plan().equals(schedule.plan()))
              ? previous.withStatus(status)
              : new Task(key, status, schedule, null);
      save(next, executable && !plan.isTerminal() && index == plan.nextOperationIndex());
    }
    if (plan.isTerminal()) {
      removeIfOwned(Scope.BROKER_PARTITION, plan.clusterId(), plan.partitionId(), plan.changeId());
    }
  }

  @Override
  public void markMember(final MemberChangeRecord change, final boolean executable) {
    final Scope scope = TopologyMembershipChangeState.scope(change.getScope());
    final int size = change.getOperations().size();
    for (int index = 0; index <= size; index++) {
      final var operation = index == size ? null : change.getOperations().get(index);
      final var key =
          new Key(
              scope,
              change.getResourceId(),
              change.getChangeId(),
              1,
              operation == null ? "" : operation.getOperationId());
      final var schedule =
          new MemberExecutionRecord.Schedule(
              new MemberExecutionRecord.Operation(
                  change.getResourceId(),
                  change.getChangeId(),
                  change.getSourceEpoch(),
                  change.getTargetEpoch(),
                  index,
                  key.operationId(),
                  operation == null ? null : operation.getType(),
                  operation == null ? "" : operation.getMemberId(),
                  operation == null ? 0 : operation.getBarrierIndex()),
              MemberExecutionRecord.Phase.READY,
              0,
              0,
              0,
              0,
              0);
      final Status status =
          index < change.getNextOperationIndex()
                  || operation != null
                      && operation.getStatus() == MemberChangeOperationStatus.COMPLETED
              ? Status.COMPLETED
              : change.isTerminal()
                  ? terminalStatus(change.getStatus().name())
                  : index == change.getNextOperationIndex() && executable
                      ? Status.IN_PROGRESS
                      : Status.PENDING;
      final var previous = task(key).orElse(null);
      var next =
          previous != null && previous.memberSchedule().operation().equals(schedule.operation())
              ? previous.withStatus(status)
              : new Task(key, status, null, schedule);
      if (index == change.getNextOperationIndex()
          && next.memberSchedule().phase() == MemberExecutionRecord.Phase.WAIT_ADVANCE) {
        next = new Task(key, status, null, next.memberSchedule().ready(0));
      }
      save(next, executable && !change.isTerminal() && index == change.getNextOperationIndex());
    }
    if (change.isTerminal()) {
      removeIfOwned(scope, change.getResourceId(), 1, change.getChangeId());
    }
  }

  @Override
  public void putPartition(final PartitionExecutionRecord.Schedule schedule) {
    current(Scope.BROKER_PARTITION, schedule.plan().clusterId(), schedule.plan().partitionId())
        .filter(task -> task.partitionSchedule().plan().equals(schedule.plan()))
        .ifPresent(task -> save(new Task(task.key(), task.status(), schedule, null), true));
  }

  @Override
  public void putMember(
      final MemberChangeScope scope, final MemberExecutionRecord.Schedule schedule) {
    current(TopologyMembershipChangeState.scope(scope), schedule.operation().resourceId(), 1)
        .filter(task -> task.memberSchedule().operation().equals(schedule.operation()))
        .ifPresent(task -> save(new Task(task.key(), task.status(), null, schedule), true));
  }

  @Override
  public void removeActive(final Scope scope, final String clusterId, final int partitionId) {
    current(scope, clusterId, partitionId).ifPresent(this::removeIndexes);
    activeColumnFamilies.deleteIfExists(wrapResourceKey(scope, clusterId, partitionId));
  }

  @Override
  public boolean deletePlan(final Scope scope, final String clusterId, final String changeId) {
    final var entries = tasks(scope, clusterId, changeId);
    if (entries.stream().anyMatch(task -> !task.status().terminal())) {
      return false;
    }
    entries.forEach(this::delete);
    return true;
  }

  @Override
  public void deleteResource(final Scope scope, final String clusterId) {
    select(task -> task.key().scope() == scope && task.key().clusterId().equals(clusterId))
        .forEach(this::delete);
  }

  private Optional<Task> current(final Scope scope, final String clusterId, final int partitionId) {
    final var pointer = activeColumnFamilies.get(wrapResourceKey(scope, clusterId, partitionId));
    return pointer == null ? Optional.empty() : task(pointer.inner().key());
  }

  private void save(final Task task, final boolean executable) {
    final var key = task.key();
    task(key).ifPresent(this::removeIndexes);
    final var record = new TopologyChangeTaskRecord().setTask(task);
    final var value = new TopologyChangeTaskRecordValue();
    value.setRecord(record);
    tasksColumnFamilies.upsert(taskKey.wrapKey(key), value);
    if (executable && !task.status().terminal()) {
      current(key.scope(), key.clusterId(), key.partitionId())
          .filter(previous -> !previous.key().equals(key))
          .ifPresent(this::removeIndexes);
      activeColumnFamilies.upsert(
          wrapResourceKey(key.scope(), key.clusterId(), key.partitionId()), wrapReference(key));
      deadlinesColumnFamilies.upsert(wrapDueKey(0, task.dueAt(), key), DbNil.INSTANCE);
      if (task.reserved()) {
        deadlinesColumnFamilies.upsert(wrapDueKey(1, task.deadline(), key), DbNil.INSTANCE);
      }
    }
  }

  private void removeIfOwned(
      final Scope scope, final String clusterId, final int partitionId, final String changeId) {
    current(scope, clusterId, partitionId)
        .filter(task -> task.key().changeId().equals(changeId))
        .ifPresent(task -> removeActive(scope, clusterId, partitionId));
  }

  private void removeIndexes(final Task task) {
    deadlinesColumnFamilies.deleteIfExists(wrapDueKey(0, task.dueAt(), task.key()));
    if (task.reserved()) {
      deadlinesColumnFamilies.deleteIfExists(wrapDueKey(1, task.deadline(), task.key()));
    }
  }

  private void delete(final Task task) {
    final var key = task.key();
    current(key.scope(), key.clusterId(), key.partitionId())
        .filter(current -> current.key().equals(key))
        .ifPresent(current -> removeActive(key.scope(), key.clusterId(), key.partitionId()));
    removeIndexes(task);
    tasksColumnFamilies.deleteIfExists(taskKey.wrapKey(key));
  }

  private List<Task> select(final Predicate<Task> predicate) {
    final List<Task> result = new ArrayList<>();
    tasksColumnFamilies.forEach(
        (key, value) -> decode(value).filter(predicate).ifPresent(result::add));
    return result;
  }

  private static Optional<Task> decode(final TopologyChangeTaskRecordValue stored) {
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord().getTask());
  }

  private static Status terminalStatus(final String status) {
    return switch (status) {
      case "COMPLETED" -> Status.COMPLETED;
      case "SUPERSEDED" -> Status.SUPERSEDED;
      default -> Status.CANCELLED;
    };
  }

  // [taskKey] -> PD_TOPOLOGY_CHANGE_TASKS; also stored as the active-task index value.
  private DbForeignKey<TopologyChangeTaskKey> wrapReference(final Key key) {
    taskReference.inner().wrapKey(key);
    return taskReference;
  }

  // [scope ordinal, clusterId, partitionId]: prefix-independent active task lookup.
  private DbCompositeKey<DbInt, DbCompositeKey<DbString, DbInt>> wrapResourceKey(
      final Scope scope, final String clusterId, final int partitionId) {
    resourceKey.first().wrapInt(scope.ordinal());
    resourceKey.second().first().wrapString(clusterId);
    resourceKey.second().second().wrapInt(partitionId);
    return resourceKey;
  }

  // [kind, dueAt, task FK -> PD_TOPOLOGY_CHANGE_TASKS]: kind 0 due work / 1 RPC reservation.
  private DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbForeignKey<TopologyChangeTaskKey>>>
      wrapDueKey(final int kind, final long dueAt, final Key task) {
    return wrapDueKey(
        kind,
        dueAt,
        task.scope(),
        task.clusterId(),
        task.changeId(),
        task.partitionId(),
        task.operationId());
  }

  private DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbForeignKey<TopologyChangeTaskKey>>>
      wrapDueKey(
          final int kind,
          final long dueAt,
          final Scope scope,
          final String clusterId,
          final String changeId,
          final int partitionId,
          final String operationId) {
    deadlineKey.first().wrapInt(kind);
    deadlineKey.second().first().wrapLong(dueAt);
    deadlineKey
        .second()
        .second()
        .inner()
        .wrapKey(scope, clusterId, changeId, partitionId, operationId);
    return deadlineKey;
  }
}
