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
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableJobYieldSyncState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldSyncRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** CF237 [cluster, partition]; CF238 [kind, due, FK237] -> nil, kind 0 due / 1 reservation. */
public final class DbJobYieldSyncState implements MutableJobYieldSyncState {
  // [clusterId, partitionId]: synchronization entry key.
  private final DbCompositeKey<DbString, DbInt> partitionKey;
  // [kind, dueAt, partition FK -> PD_JOB_YIELD_SYNCHRONIZATION]: operation key.
  private final DbCompositeKey<
          DbInt, DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>>>
      deadlineKey;
  // PD_JOB_YIELD_SYNCHRONIZATION: [clusterId, partitionId] -> JobYieldSyncRecord MessagePack.
  // Tracks cursor, attempt, next due date and whether the partition has an in-flight sync RPC.
  private final ColumnFamily<DbCompositeKey<DbString, DbInt>, JobYieldSyncRecordValue>
      entriesColumnFamilies;
  // PD_JOB_YIELD_SYNC_DUE_DATES:
  // [kind, dueAt, partition FK -> PD_JOB_YIELD_SYNCHRONIZATION] -> nil.
  // kind 0 scans due synchronization; kind 1 counts in-flight RPC reservations.
  private final ColumnFamily<
          DbCompositeKey<
              DbInt, DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>>>,
          DbNil>
      dueColumnFamilies;

  public DbJobYieldSyncState(final ProcessingStateStore storage) {
    partitionKey = new DbCompositeKey<>(new DbString(), new DbInt());
    deadlineKey =
        new DbCompositeKey<>(
            new DbInt(),
            new DbCompositeKey<>(
                new DbLong(),
                new DbForeignKey<>(
                    new DbCompositeKey<>(new DbString(), new DbInt()),
                    ColumnFamilies.PD_JOB_YIELD_SYNCHRONIZATION)));

    entriesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_YIELD_SYNCHRONIZATION,
            partitionKey,
            new JobYieldSyncRecordValue());
    dueColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_YIELD_SYNC_DUE_DATES, deadlineKey, DbNil.INSTANCE);
  }

  @Override
  public Optional<JobYieldSyncRecord> get(final String cluster, final int partition) {
    final var value =
        entriesColumnFamilies.get(
            wrapPartitionKey(cluster, partition), JobYieldSyncRecordValue::new);
    return value == null ? Optional.empty() : Optional.of(value.getRecord());
  }

  @Override
  public void ensurePartition(final String cluster, final int partition) {
    if (get(cluster, partition).isEmpty()) {
      put(new JobYieldSyncRecord(cluster, partition, 0, 0, 0, false));
    }
  }

  @Override
  public void put(final JobYieldSyncRecord entry) {
    get(entry.clusterId(), entry.partitionId()).ifPresent(this::removeDeadlines);
    final var value = new JobYieldSyncRecordValue();
    value.setRecord(entry);
    entriesColumnFamilies.upsert(wrapPartitionKey(entry.clusterId(), entry.partitionId()), value);
    dueColumnFamilies.upsert(
        wrapDueKey(entry.dueAt(), entry.clusterId(), entry.partitionId()), DbNil.INSTANCE);
    if (entry.inFlight()) {
      dueColumnFamilies.upsert(
          wrapIndexKey(1, entry.dueAt(), entry.clusterId(), entry.partitionId()), DbNil.INSTANCE);
    }
  }

  @Override
  public void delete(final String cluster, final int partition) {
    get(cluster, partition).ifPresent(this::removeDeadlines);
    entriesColumnFamilies.deleteIfExists(wrapPartitionKey(cluster, partition));
  }

  @Override
  public List<JobYieldSyncRecord> dueBefore(final long now, final int limit) {
    final List<JobYieldSyncRecord> result = new ArrayList<>();
    if (limit > 0) {
      dueColumnFamilies.whileTrue(
          (key, ignored) -> {
            if (key.first().getValue() != 0 || key.second().first().getValue() > now) {
              return false;
            }
            get(
                    key.second().second().inner().first().toString(),
                    key.second().second().inner().second().getValue())
                .ifPresent(result::add);
            return result.size() < limit;
          });
    }
    return result;
  }

  @Override
  public int reservedTargets(final long now, final int limit) {
    final int[] count = {0};
    if (limit > 0) {
      dueColumnFamilies.whileTrue(
          wrapIndexKey(1, now + 1, "", 0),
          (key, ignored) -> {
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

  // [clusterId, partitionId]: primary synchronization entry key.
  private DbCompositeKey<DbString, DbInt> wrapPartitionKey(
      final String cluster, final int partition) {
    partitionKey.first().wrapString(cluster);
    partitionKey.second().wrapInt(partition);
    return partitionKey;
  }

  private void removeDeadlines(final JobYieldSyncRecord old) {
    dueColumnFamilies.deleteIfExists(wrapDueKey(old.dueAt(), old.clusterId(), old.partitionId()));
    dueColumnFamilies.deleteIfExists(
        wrapIndexKey(1, old.dueAt(), old.clusterId(), old.partitionId()));
  }

  // [0, dueAt, FK(clusterId, partitionId) -> PD_JOB_YIELD_SYNCHRONIZATION]
  private DbCompositeKey<
          DbInt, DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>>>
      wrapDueKey(final long time, final String cluster, final int partition) {
    return wrapIndexKey(0, time, cluster, partition);
  }

  // [kind, dueAt, FK(clusterId, partitionId) -> PD_JOB_YIELD_SYNCHRONIZATION]
  // kind 0 is a due entry; kind 1 is a reservation expiring at the same deadline.
  private DbCompositeKey<
          DbInt, DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>>>
      wrapIndexKey(final int kind, final long time, final String cluster, final int partition) {
    deadlineKey.first().wrapInt(kind);
    deadlineKey.second().first().wrapLong(time);
    deadlineKey.second().second().inner().first().wrapString(cluster);
    deadlineKey.second().second().inner().second().wrapInt(partition);
    return deadlineKey;
  }
}
