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
import io.taskite.placementdriver.state.mutable.MutableBrokerTopologyCoordinatorState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.TaskType;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 239: [cluster, partition] -> task revision/cursor/due. Partition 0 expands one cluster; ["", 0]
 * pages recovery over topology index 233.
 *
 * <p>240: [dueAt, FK -> 239] -> nil. Event Appliers explicitly enqueue coordination work in the
 * same transaction as the topology change; lease and replica progress writes do not enqueue work.
 *
 * <p>244: [revisionSequence | queueTime] -> long. Global metadata survives task and cluster
 * deletion. The legacy ["", -1] row is transferred on the first metadata mutation within the
 * Applier transaction.
 */
public final class DbBrokerTopologyCoordinatorState
    implements MutableBrokerTopologyCoordinatorState {
  private static final String REVISION_SEQUENCE = "revisionSequence";
  private static final String QUEUE_TIME = "queueTime";
  private static final String RECOVERY_CLUSTER_ID = "";
  private static final int SCAN_PARTITION_ID = 0;
  private static final int LEGACY_METADATA_PARTITION_ID = -1;

  // [clusterId, partitionId]: task identity, including the reserved recovery scan.
  private final DbCompositeKey<DbString, DbInt> taskKey;
  // [dueAt, task FK -> PD_BROKER_TOPOLOGY_SCHEDULES]: operation key.
  private final DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>> deadlineKey;
  // [revisionSequence | queueTime]: reused global metadata key and value.
  private final DbString metadataKey;
  private final DbLong metadataValue;
  // PD_BROKER_TOPOLOGY_SCHEDULES: [clusterId, partitionId] -> BrokerTopologyCoordinationRecord.
  // Partition 0 expands a cluster; ["", 0] is the recovery scan. No new metadata rows are stored
  // here.
  private final ColumnFamily<DbCompositeKey<DbString, DbInt>, BrokerTopologyCoordinationRecordValue>
      tasksColumnFamilies;
  // PD_BROKER_TOPOLOGY_DUE_DATES: [dueAt, task FK -> PD_BROKER_TOPOLOGY_SCHEDULES] -> nil.
  // Orders coordination work by its next observation/dispatch deadline.
  private final ColumnFamily<
          DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>>, DbNil>
      deadlinesColumnFamilies;
  // PD_BROKER_TOPOLOGY_COORDINATOR_META: [revisionSequence | queueTime] -> long.
  // Kept independently of task lifecycles; never included in the due-date index.
  private final ColumnFamily<DbString, DbLong> metadataColumnFamilies;

  public DbBrokerTopologyCoordinatorState(final ProcessingStateStore storage) {
    taskKey = new DbCompositeKey<>(new DbString(), new DbInt());
    deadlineKey =
        new DbCompositeKey<>(
            new DbLong(),
            new DbForeignKey<>(
                new DbCompositeKey<>(new DbString(), new DbInt()),
                ColumnFamilies.PD_BROKER_TOPOLOGY_SCHEDULES));
    metadataKey = new DbString();
    metadataValue = new DbLong();
    tasksColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_TOPOLOGY_SCHEDULES,
            taskKey,
            new BrokerTopologyCoordinationRecordValue());
    deadlinesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_TOPOLOGY_DUE_DATES, deadlineKey, DbNil.INSTANCE);
    metadataColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_TOPOLOGY_COORDINATOR_META, metadataKey, metadataValue);
  }

  @Override
  public Optional<Entry> getTask(final String clusterId, final int partitionId) {
    TaskType.validateTaskIdentity(clusterId, partitionId);
    final var stored =
        tasksColumnFamilies.get(
            wrapTaskKey(clusterId, partitionId), BrokerTopologyCoordinationRecordValue::new);
    if (stored == null) {
      return Optional.empty();
    }
    final var record = stored.getRecord();
    return Optional.of(
        new Entry(
            clusterId,
            partitionId,
            record.getRevision(),
            record.getCursorCluster(),
            record.getCursorPartition(),
            record.getDueAt(),
            record.isObservingSubmit()));
  }

  @Override
  public Optional<Entry> getRecoveryScan() {
    return getTask(RECOVERY_CLUSTER_ID, SCAN_PARTITION_ID);
  }

  @Override
  public long getQueueTime() {
    final long current = readMetadata(QUEUE_TIME);
    final var legacy = readLegacyMetadata();
    return legacy == null ? current : Math.max(current, legacy.getDueAt());
  }

  @Override
  public void markCluster(final String clusterId) {
    markCluster(clusterId, getQueueTime());
  }

  @Override
  public void markCluster(final String clusterId, final long enqueuedAt) {
    requireType(clusterId, SCAN_PARTITION_ID, TaskType.CLUSTER_EXPANSION);
    enqueueTask(clusterId, SCAN_PARTITION_ID, enqueuedAt);
  }

  @Override
  public void markPartition(final String clusterId, final int partitionId) {
    markPartition(clusterId, partitionId, getQueueTime());
  }

  @Override
  public void markPartition(final String clusterId, final int partitionId, final long enqueuedAt) {
    requireType(clusterId, partitionId, TaskType.PARTITION_COORDINATION);
    enqueueTask(clusterId, partitionId, enqueuedAt);
  }

  @Override
  public void startRecoveryScan() {
    if (getRecoveryScan().isEmpty()) {
      enqueueTask(RECOVERY_CLUSTER_ID, SCAN_PARTITION_ID, getQueueTime());
    }
  }

  @Override
  public void advanceTime(final long coordinatedAt) {
    migrateMetadata();
    if (coordinatedAt > readMetadata(QUEUE_TIME)) {
      writeMetadata(QUEUE_TIME, coordinatedAt);
    }
  }

  private void enqueueTask(final String clusterId, final int partitionId, final long enqueuedAt) {
    migrateMetadata();
    final long revision = readMetadata(REVISION_SEQUENCE) + 1;
    final long now = Math.max(readMetadata(QUEUE_TIME), enqueuedAt);
    writeMetadata(REVISION_SEQUENCE, revision);
    writeMetadata(QUEUE_TIME, now);
    final var previous = getTask(clusterId, partitionId).orElse(null);
    // A new dirty notification cannot shorten a pending SUBMIT's observation window.
    putTask(
        new Entry(
            clusterId,
            partitionId,
            revision,
            "",
            0,
            previous == null
                ? now
                : previous.observingSubmit() ? previous.dueAt() : Math.min(previous.dueAt(), now),
            previous != null && previous.observingSubmit()));
  }

  @Override
  public void putTask(final Entry entry) {
    removeTask(entry.clusterId(), entry.partitionId());
    store(entry);
    deadlinesColumnFamilies.upsert(
        wrapDueKey(entry.dueAt(), entry.clusterId(), entry.partitionId()), DbNil.INSTANCE);
  }

  private void store(final Entry entry) {
    final var record =
        new BrokerTopologyCoordinationRecord()
            .setRevision(entry.revision())
            .setCursorCluster(entry.cursorCluster())
            .setCursorPartition(entry.cursorPartition())
            .setDueAt(entry.dueAt())
            .setObservingSubmit(entry.observingSubmit());
    final var value = new BrokerTopologyCoordinationRecordValue();
    value.setRecord(record);
    tasksColumnFamilies.upsert(wrapTaskKey(entry.clusterId(), entry.partitionId()), value);
  }

  @Override
  public void removeTask(final String clusterId, final int partitionId) {
    getTask(clusterId, partitionId)
        .ifPresent(
            entry ->
                deadlinesColumnFamilies.deleteIfExists(
                    wrapDueKey(entry.dueAt(), clusterId, partitionId)));
    tasksColumnFamilies.deleteIfExists(wrapTaskKey(clusterId, partitionId));
  }

  @Override
  public void deleteCluster(final String clusterId) {
    requireType(clusterId, SCAN_PARTITION_ID, TaskType.CLUSTER_EXPANSION);
    tasksColumnFamilies.whileTrue(
        wrapTaskKey(clusterId, SCAN_PARTITION_ID),
        (task, value) -> {
          if (!task.first().toString().equals(clusterId)) {
            return false;
          }
          removeTask(clusterId, task.second().getValue());
          return true;
        });
  }

  @Override
  public List<Entry> dueBefore(final long now, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final List<Entry> result = new ArrayList<>();
    deadlinesColumnFamilies.whileTrue(
        (key, value) -> {
          if (key.first().getValue() > now) {
            return false;
          }
          final var task = key.second().inner();
          getTask(task.first().toString(), task.second().getValue()).ifPresent(result::add);
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public long nextDueAt() {
    final long[] next = {-1};
    deadlinesColumnFamilies.whileTrue(
        (key, value) -> {
          next[0] = key.first().getValue();
          return false;
        });
    return next[0];
  }

  @Override
  public int reservedPartitions(final long now, final int limit) {
    final int[] count = {0};
    deadlinesColumnFamilies.whileTrue(
        wrapDueKey(now + 1, RECOVERY_CLUSTER_ID, SCAN_PARTITION_ID),
        (key, value) -> {
          final var task = key.second().inner();
          if (getTask(task.first().toString(), task.second().getValue())
              .filter(Entry::observingSubmit)
              .isPresent()) {
            count[0]++;
          }
          return count[0] < limit;
        });
    return count[0];
  }

  private long readMetadata(final String name) {
    final var stored = metadataColumnFamilies.get(wrapMetadataKey(name));
    return stored == null ? 0 : stored.getValue();
  }

  private void writeMetadata(final String name, final long value) {
    metadataValue.wrapLong(value);
    metadataColumnFamilies.upsert(wrapMetadataKey(name), metadataValue);
  }

  private BrokerTopologyCoordinationRecord readLegacyMetadata() {
    final var stored =
        tasksColumnFamilies.get(
            wrapTaskKey(RECOVERY_CLUSTER_ID, LEGACY_METADATA_PARTITION_ID),
            BrokerTopologyCoordinationRecordValue::new);
    if (stored == null) {
      return null;
    }
    return stored.getRecord();
  }

  // Called only by mutations in the current transaction; no cached migration flag, so restoring
  // an older checkpoint into the same storage also transfers its metadata on the next mutation.
  private void migrateMetadata() {
    final var legacy = readLegacyMetadata();
    if (legacy == null) {
      return;
    }
    writeMetadata(
        REVISION_SEQUENCE, Math.max(readMetadata(REVISION_SEQUENCE), legacy.getRevision()));
    writeMetadata(QUEUE_TIME, Math.max(readMetadata(QUEUE_TIME), legacy.getDueAt()));
    tasksColumnFamilies.deleteIfExists(
        wrapTaskKey(RECOVERY_CLUSTER_ID, LEGACY_METADATA_PARTITION_ID));
  }

  private static void requireType(
      final String clusterId, final int partitionId, final TaskType expected) {
    if (TaskType.forIdentity(clusterId, partitionId) != expected) {
      throw new IllegalArgumentException(
          "Expected " + expected + ": clusterId=" + clusterId + ", partitionId=" + partitionId);
    }
  }

  // [revisionSequence | queueTime] -> long.
  private DbString wrapMetadataKey(final String name) {
    metadataKey.wrapString(name);
    return metadataKey;
  }

  // [clusterId, partitionId]: business task or the named recovery scan's reserved identity.
  private DbCompositeKey<DbString, DbInt> wrapTaskKey(
      final String clusterId, final int partitionId) {
    taskKey.first().wrapString(clusterId);
    taskKey.second().wrapInt(partitionId);
    return taskKey;
  }

  // [dueAt, FK(clusterId, partitionId) -> PD_BROKER_TOPOLOGY_SCHEDULES]
  private DbCompositeKey<DbLong, DbForeignKey<DbCompositeKey<DbString, DbInt>>> wrapDueKey(
      final long dueAt, final String clusterId, final int partitionId) {
    deadlineKey.first().wrapLong(dueAt);
    deadlineKey.second().inner().first().wrapString(clusterId);
    deadlineKey.second().inner().second().wrapInt(partitionId);
    return deadlineKey;
  }
}
