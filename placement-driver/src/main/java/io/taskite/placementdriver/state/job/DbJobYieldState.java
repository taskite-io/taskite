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
package io.taskite.placementdriver.state.job;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbForeignKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableJobYieldState;
import io.taskite.placementdriver.state.mutable.MutableJobYieldSyncState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class DbJobYieldState implements MutableJobYieldState {
  // [yieldId]: pending-plan lookup and pagination cursor.
  private final DbLong pendingKey;
  // [clusterId, yieldId]: plan lookup and cluster-scoped pagination cursor.
  private final DbCompositeKey<DbString, DbLong> planKey;
  // [kind, dueAt, yieldId FK -> PD_PENDING_JOB_YIELDS, partitionId]: operation key.
  // Deadline and yield ID have independent mutable leaves.
  private final DbCompositeKey<
          DbInt, DbCompositeKey<DbLong, DbCompositeKey<DbForeignKey<DbLong>, DbInt>>>
      deadlineKey;
  // PD_JOB_YIELDS: [clusterId, yieldId] -> JobYieldPlan MessagePack.
  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, JobYieldPlanRecordValue>
      plansColumnFamilies;
  // PD_PENDING_JOB_YIELDS: [yieldId] -> clusterId; resolves the pending plan in PD_JOB_YIELDS.
  // Ordered yield IDs support bounded pagination across clusters.
  private final ColumnFamily<DbLong, DbString> pendingColumnFamilies;
  // PD_JOB_YIELD_DUE_DATES:
  // [kind, dueAt, yieldId FK -> PD_PENDING_JOB_YIELDS, partitionId] -> clusterId.
  // kind 0 scans due release attempts; kind 1 counts persisted RPC reservations.
  private final ColumnFamily<
          DbCompositeKey<
              DbInt, DbCompositeKey<DbLong, DbCompositeKey<DbForeignKey<DbLong>, DbInt>>>,
          DbString>
      dueColumnFamilies;
  private final MutableJobYieldSyncState synchronization;

  public DbJobYieldState(ProcessingStateStore storage) {
    pendingKey = new DbLong();
    planKey = new DbCompositeKey<>(new DbString(), new DbLong());
    deadlineKey =
        new DbCompositeKey<>(
            new DbInt(),
            new DbCompositeKey<>(
                new DbLong(),
                new DbCompositeKey<>(
                    new DbForeignKey<>(new DbLong(), ColumnFamilies.PD_PENDING_JOB_YIELDS),
                    new DbInt())));

    plansColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_YIELDS, planKey, new JobYieldPlanRecordValue());
    pendingColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PENDING_JOB_YIELDS, pendingKey, new DbString());
    dueColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_YIELD_DUE_DATES, deadlineKey, new DbString());
    synchronization = new DbJobYieldSyncState(storage);
  }

  @Override
  public MutableJobYieldSyncState synchronization() {
    return synchronization;
  }

  @Override
  public List<DueTarget> dueBefore(long now, int limit) {
    final List<DueTarget> result = new ArrayList<>();
    if (limit > 0) {
      dueColumnFamilies.whileTrue(
          (key, cluster) -> {
            if (key.first().getValue() != 0 || key.second().first().getValue() > now) {
              return false;
            }
            result.add(
                new DueTarget(
                    cluster.toString(),
                    key.second().second().first().inner().getValue(),
                    key.second().second().second().getValue(),
                    key.second().first().getValue()));
            return result.size() < limit;
          });
    }
    return result;
  }

  @Override
  public int reservedTargets(long now, int limit) {
    final int[] count = {0};
    if (limit > 0) {
      dueColumnFamilies.whileTrue(
          wrapIndexKey(1, now + 1, 0, 0),
          (key, cluster) -> {
            if (key.first().getValue() != 1) {
              return false;
            }
            return ++count[0] < limit;
          });
    }
    return count[0];
  }

  @Override
  public long nextDueAt() {
    final long[] next = {-1};
    dueColumnFamilies.whileTrue(
        (key, ignored) -> {
          if (key.first().getValue() == 0) {
            next[0] = key.second().first().getValue();
          }
          return false;
        });
    return next[0];
  }

  @Override
  public List<JobYieldPlan> plans() {
    final List<JobYieldPlan> result = new ArrayList<>();
    plansColumnFamilies.forEach(value -> result.add(value.getRecord()));
    return result;
  }

  @Override
  public List<JobYieldPlan> pendingPlans(long afterId, int limit) {
    requireLimit(limit);
    final List<JobYieldPlan> result = new ArrayList<>();
    pendingColumnFamilies.whileTrue(
        wrapPendingKey(afterId),
        (id, cluster) -> {
          if (id.getValue() > afterId) {
            readPlan(cluster.toString(), id.getValue()).ifPresent(result::add);
          }
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public List<JobYieldPlan> plansAfter(String clusterId, long afterId, int limit) {
    requireLimit(limit);
    final List<JobYieldPlan> result = new ArrayList<>();
    final var start = wrapPlanKey(clusterId, afterId);
    plansColumnFamilies.whileEqualPrefix(
        start.first(),
        start,
        (id, value) -> {
          if (id.second().getValue() > afterId) {
            result.add(value.getRecord());
          }
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public Optional<JobYieldPlan> plan(String clusterId, long id) {
    return readPlan(clusterId, id);
  }

  private Optional<JobYieldPlan> readPlan(String clusterId, long id) {
    final var value =
        plansColumnFamilies.get(wrapPlanKey(clusterId, id), JobYieldPlanRecordValue::new);
    return value == null ? Optional.empty() : Optional.of(value.getRecord());
  }

  @Override
  public void create(JobYieldPlan plan) {
    if (readPlan(plan.clusterId(), plan.yieldId()).isPresent()) {
      return;
    }
    if (plan.partitions().isEmpty()) {
      plan =
          new JobYieldPlan(
              plan.yieldId(),
              plan.clusterId(),
              plan.tenantId(),
              plan.workerId(),
              plan.workerLeaseToken(),
              plan.reason(),
              plan.createdAtEpochMillis(),
              plan.createdAtEpochMillis(),
              plan.workerSnapshot(),
              plan.partitions());
    }
    put(plan);
    if (!plan.partitions().isEmpty()) {
      pendingColumnFamilies.upsert(wrapPendingKey(plan.yieldId()), clusterValue(plan.clusterId()));
    }
  }

  @Override
  public void updateProgress(JobYieldRecord progress, boolean acknowledged) {
    final var plan = plan(progress.getClusterId(), progress.getYieldId()).orElseThrow();
    final var targets =
        plan.partitions().stream()
            .map(
                target -> {
                  if (target.partitionId() != progress.getPartitionId()) {
                    return target;
                  }
                  return new JobYieldPlan.Partition(
                      target.partitionId(),
                      acknowledged ? JobYieldStatus.COMPLETED : JobYieldStatus.PENDING,
                      Math.max(target.releasedJobCount(), progress.getReleasedJobCount()),
                      progress.getAttempts(),
                      progress.getTimestampEpochMillis(),
                      progress.getNextAttemptAtEpochMillis(),
                      acknowledged ? progress.getTimestampEpochMillis() : 0,
                      progress.getLastError(),
                      progress.getPauseReason());
                })
            .toList();
    final boolean completed =
        targets.stream().allMatch(target -> target.status() != JobYieldStatus.PENDING);
    put(
        new JobYieldPlan(
            plan.yieldId(),
            plan.clusterId(),
            plan.tenantId(),
            plan.workerId(),
            plan.workerLeaseToken(),
            plan.reason(),
            plan.createdAtEpochMillis(),
            completed ? progress.getTimestampEpochMillis() : 0,
            plan.workerSnapshot(),
            targets));
    if (completed) {
      pendingColumnFamilies.deleteIfExists(wrapPendingKey(plan.yieldId()));
    }
  }

  private void put(JobYieldPlan plan) {
    readPlan(plan.clusterId(), plan.yieldId()).ifPresent(this::removeDeadlines);
    final var value = new JobYieldPlanRecordValue();
    value.setRecord(plan);
    plansColumnFamilies.upsert(wrapPlanKey(plan.clusterId(), plan.yieldId()), value);
    for (final var target : plan.partitions()) {
      if (target.status() == JobYieldStatus.PENDING) {
        dueColumnFamilies.upsert(
            wrapDueKey(target.nextAttemptAtEpochMillis(), plan.yieldId(), target.partitionId()),
            clusterValue(plan.clusterId()));
        if (target.attempts() > 0) {
          dueColumnFamilies.upsert(
              wrapIndexKey(
                  1, target.nextAttemptAtEpochMillis(), plan.yieldId(), target.partitionId()),
              clusterValue(plan.clusterId()));
        }
      }
    }
  }

  @Override
  public void deleteForWorker(String clusterId, String tenantId, String workerId) {
    final List<Long> ids = new ArrayList<>();
    final var start = wrapPlanKey(clusterId, 0);
    plansColumnFamilies.whileEqualPrefix(
        start.first(),
        start,
        (id, value) -> {
          final var plan = value.getRecord();
          if (plan.tenantId().equals(tenantId) && plan.workerId().equals(workerId)) {
            ids.add(plan.yieldId());
          }
          return true;
        });
    for (final long id : ids) {
      readPlan(clusterId, id).ifPresent(this::removeDeadlines);
      plansColumnFamilies.deleteIfExists(wrapPlanKey(clusterId, id));
      pendingColumnFamilies.deleteIfExists(wrapPendingKey(id));
    }
  }

  private void removeDeadlines(JobYieldPlan plan) {
    for (final var target : plan.partitions()) {
      dueColumnFamilies.deleteIfExists(
          wrapDueKey(target.nextAttemptAtEpochMillis(), plan.yieldId(), target.partitionId()));
      dueColumnFamilies.deleteIfExists(
          wrapIndexKey(1, target.nextAttemptAtEpochMillis(), plan.yieldId(), target.partitionId()));
    }
  }

  // [0, dueAt, yieldId FK -> PD_PENDING_JOB_YIELDS, partitionId]: due release-attempt key.
  private DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbCompositeKey<DbForeignKey<DbLong>, DbInt>>>
      wrapDueKey(long dueAt, long yieldId, int partitionId) {
    return wrapIndexKey(0, dueAt, yieldId, partitionId);
  }

  // [kind, dueAt, yieldId FK -> PD_PENDING_JOB_YIELDS, partitionId]: kind 0 due / 1 reservation.
  private DbCompositeKey<DbInt, DbCompositeKey<DbLong, DbCompositeKey<DbForeignKey<DbLong>, DbInt>>>
      wrapIndexKey(int kind, long dueAt, long yieldId, int partitionId) {
    deadlineKey.first().wrapInt(kind);
    deadlineKey.second().first().wrapLong(dueAt);
    deadlineKey.second().second().first().inner().wrapLong(yieldId);
    deadlineKey.second().second().second().wrapInt(partitionId);
    return deadlineKey;
  }

  // [yieldId]: pending-plan lookup and exclusive pagination cursor.
  private DbLong wrapPendingKey(long id) {
    pendingKey.wrapLong(id);
    return pendingKey;
  }

  private static DbString clusterValue(String clusterId) {
    final var value = new DbString();
    value.wrapString(clusterId);
    return value;
  }

  // [clusterId, yieldId]: primary plan key and cluster-scoped pagination cursor.
  private DbCompositeKey<DbString, DbLong> wrapPlanKey(String cluster, long id) {
    planKey.first().wrapString(cluster);
    planKey.second().wrapLong(id);
    return planKey;
  }

  private static void requireLimit(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("Job yield page limit must be positive");
    }
  }
}
