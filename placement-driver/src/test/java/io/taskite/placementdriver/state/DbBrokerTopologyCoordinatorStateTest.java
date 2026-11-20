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
package io.taskite.placementdriver.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState;
import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState.Entry;
import io.taskite.placementdriver.state.mutable.MutableBrokerTopologyCoordinatorState;
import io.taskite.placementdriver.state.topology.DbBrokerTopologyCoordinatorState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.TaskType;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DbBrokerTopologyCoordinatorStateTest {
  @TempDir Path directory;

  @Test
  void metadataTaskAndDeadlineMutationsRollbackTogether() throws Exception {
    try (final var f = new Fixture(directory)) {
      f.store.getCurrentTransaction();
      f.queue.advanceTime(100);
      f.queue.markPartition("demo", 1);
      f.commit();
      final var original = f.queue.getTask("demo", 1).orElseThrow();

      f.store.getCurrentTransaction();
      f.queue.removeTask("demo", 1);
      f.queue.markPartition("demo", 2, 200);
      assertEquals(200, f.queue.getQueueTime());
      assertEquals(2, f.queue.getTask("demo", 2).orElseThrow().revision());
      assertEquals(200, f.queue.nextDueAt());
      f.store.getCurrentTransaction().rollback();

      assertEquals(100, f.queue.getQueueTime());
      assertEquals(List.of(original), f.queue.dueBefore(Long.MAX_VALUE, 10));
      assertTrue(f.queue.getTask("demo", 2).isEmpty());
      f.store.getCurrentTransaction();
      f.queue.markPartition("demo", 3);
      assertEquals(2, f.queue.getTask("demo", 3).orElseThrow().revision());
      f.queue.deleteCluster("demo");
      f.commit();
      assertEquals(100, f.metadataValue("queueTime"));
      assertEquals(2, f.metadataValue("revisionSequence"));
      assertEquals(-1, f.queue.nextDueAt());
      assertTrue(f.queue.dueBefore(Long.MAX_VALUE, 10).isEmpty());
      assertNull(f.tasksColumnFamilies.get(legacyKey()));
    }
  }

  @Test
  void legacyMetadataReadsArePureAndMigrationIsAtomicAndSurvivesRestart() throws Exception {
    try (final var f = new Fixture(directory)) {
      f.writeLegacy(42, 9000);
      f.commit();
      assertEquals(9000, f.query.getQueueTime());
      assertNull(f.metadataColumnFamilies.get(text("queueTime")));
      assertNull(f.metadataColumnFamilies.get(text("revisionSequence")));
      assertNotNull(f.tasksColumnFamilies.get(legacyKey()));
      assertEquals(-1, f.query.nextDueAt());

      f.store.getCurrentTransaction();
      f.queue.markPartition("demo", 1, 1);
      assertEquals(43, f.queue.getTask("demo", 1).orElseThrow().revision());
      assertEquals(9000, f.queue.getTask("demo", 1).orElseThrow().dueAt());
      assertNull(f.tasksColumnFamilies.get(legacyKey()));
      f.store.getCurrentTransaction().rollback();
      assertNotNull(f.tasksColumnFamilies.get(legacyKey()));
      assertNull(f.metadataColumnFamilies.get(text("revisionSequence")));
      assertTrue(f.queue.getTask("demo", 1).isEmpty());
      assertEquals(-1, f.queue.nextDueAt());

      f.store.getCurrentTransaction();
      f.queue.markPartition("demo", 1, 1);
      f.commit();
      assertNull(f.tasksColumnFamilies.get(legacyKey()));
      assertEquals(9000, f.query.getQueueTime());
      assertEquals(43, f.query.getTask("demo", 1).orElseThrow().revision());
    }
    try (final var f = new Fixture(directory)) {
      assertEquals(9000, f.query.getQueueTime());
      f.store.getCurrentTransaction();
      f.queue.markPartition("demo", 2, 0);
      assertEquals(44, f.queue.getTask("demo", 2).orElseThrow().revision());
      assertEquals(9000, f.queue.getTask("demo", 2).orElseThrow().dueAt());
      f.commit();
    }
  }

  @Test
  void migrationMergesEachMetadataValueWithoutOverwritingHigherValues() throws Exception {
    final long[][] cases = {{60, 10_000}, {60, 100}, {10, 10_000}, {40, 9000}};
    for (int index = 0; index < cases.length; index++) {
      try (final var f = new Fixture(directory.resolve("merge-" + index))) {
        f.store.getCurrentTransaction();
        f.writeLegacy(40, 9000);
        f.writeMetadata("revisionSequence", cases[index][0]);
        f.writeMetadata("queueTime", cases[index][1]);
        f.commit();
        final long expectedRevision = Math.max(40, cases[index][0]);
        final long expectedTime = Math.max(9000, cases[index][1]);
        assertEquals(expectedTime, f.query.getQueueTime());
        f.store.getCurrentTransaction();
        f.queue.advanceTime(0);
        f.queue.advanceTime(0);
        assertEquals(expectedRevision, f.metadataValue("revisionSequence"));
        assertEquals(expectedTime, f.metadataValue("queueTime"));
        assertNull(f.tasksColumnFamilies.get(legacyKey()));
        f.queue.markPartition("demo", 1, 1);
        f.queue.markPartition("demo", 2, 2);
        assertEquals(expectedRevision + 2, f.queue.getTask("demo", 2).orElseThrow().revision());
        assertEquals(expectedTime, f.queue.getTask("demo", 2).orElseThrow().dueAt());
        f.commit();
      }
    }
  }

  @Test
  void oldCheckpointImportRemigratesThroughExistingStateAndQueryObjects() throws Exception {
    final var checkpoint = directory.resolve("legacy-checkpoint");
    try (final var f = new Fixture(directory.resolve("live"))) {
      f.store.getCurrentTransaction();
      f.writeLegacy(50, 7000);
      f.queue.putTask(new Entry("", 0, 49, "east", 100, 7000, false));
      f.queue.putTask(new Entry("east", 1, 50, "", 0, 25_000, true));
      f.commit();
      f.storage.createCheckpoint(checkpoint);

      for (int restore = 0; restore < 2; restore++) {
        f.store.getCurrentTransaction();
        f.queue.markPartition("east", 2, 8000);
        f.commit();
        assertEquals(8000, f.query.getQueueTime());
        assertNull(f.tasksColumnFamilies.get(legacyKey()));

        f.storage.importFromCheckpoint(checkpoint);
        assertEquals(7000, f.query.getQueueTime());
        assertTrue(f.query.getTask("east", 2).isEmpty());
        assertEquals(100, f.query.getRecoveryScan().orElseThrow().cursorPartition());
        assertEquals(25_000, f.query.getTask("east", 1).orElseThrow().dueAt());
        assertEquals(1, f.query.reservedPartitions(7000, 10));
        assertNotNull(f.tasksColumnFamilies.get(legacyKey()));

        f.store.getCurrentTransaction();
        f.queue.advanceTime(7000);
        f.queue.markPartition("east", 3, 0);
        f.commit();
        assertEquals(51, f.query.getTask("east", 3).orElseThrow().revision());
        assertEquals(7000, f.query.getQueueTime());
        assertNull(f.tasksColumnFamilies.get(legacyKey()));
      }
    }
  }

  @Test
  void namedTasksShareDueIndexAndRepeatedRecoveryKeepsItsCursor() throws Exception {
    try (final var f = new Fixture(directory)) {
      f.store.getCurrentTransaction();
      f.queue.markCluster("a", 100);
      f.queue.markPartition("b", 1, 100);
      f.queue.markPartition("b", 2, 100);
      f.queue.startRecoveryScan();
      final var recovery = f.queue.getRecoveryScan().orElseThrow();
      final var page = new Entry("", 0, recovery.revision(), "b", 100, 100, false);
      f.queue.putTask(page);
      f.queue.startRecoveryScan();
      f.commit();
      assertEquals(page, f.query.getRecoveryScan().orElseThrow());
      assertEquals(
          List.of(
              TaskType.RECOVERY_SCAN,
              TaskType.CLUSTER_EXPANSION,
              TaskType.PARTITION_COORDINATION,
              TaskType.PARTITION_COORDINATION),
          f.query.dueBefore(100, 10).stream().map(Entry::taskType).toList());
      for (final var task : f.query.dueBefore(100, 10)) {
        final var record =
            new BrokerTopologyCoordinationRecord()
                .setClusterId(task.clusterId())
                .setPartitionId(task.partitionId());
        assertEquals(task.taskType(), record.getTaskType());
        assertFalse(record.toJson().contains("\"taskType\""));
      }
      assertEquals(List.of(), f.query.dueBefore(99, 10));
      f.store.getCurrentTransaction();
      f.queue.deleteCluster("a");
      f.queue.markPartition("c", 1);
      f.commit();
      assertEquals(page, f.query.getRecoveryScan().orElseThrow());
      assertEquals(5, f.query.getTask("c", 1).orElseThrow().revision());
      assertEquals(100, f.query.getQueueTime());
      assertEquals(4, f.query.dueBefore(100, 10).size());
    }
  }

  @Test
  void metadataAndInvalidScopesCannotBeUsedAsTasks() {
    try (final var f = new Fixture(directory)) {
      assertThrows(IllegalArgumentException.class, () -> f.queue.getTask("", -1));
      assertThrows(
          IllegalArgumentException.class,
          () -> f.queue.putTask(new Entry("", -1, 1, "", 0, 0, false)));
      assertThrows(IllegalArgumentException.class, () -> f.queue.markCluster(""));
      assertThrows(IllegalArgumentException.class, () -> f.queue.markPartition("demo", 0));
      assertThrows(IllegalArgumentException.class, () -> f.queue.markPartition("", 1));
      assertThrows(IllegalArgumentException.class, () -> f.queue.deleteCluster(""));
      assertEquals(0, f.query.getQueueTime());
      assertTrue(f.query.dueBefore(Long.MAX_VALUE, 10).isEmpty());
      assertNull(f.metadataColumnFamilies.get(text("revisionSequence")));
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private final MutableBrokerTopologyCoordinatorState queue;
    private final BrokerTopologyCoordinatorState query;
    private final ColumnFamily<DbCompositeKey<DbString, DbInt>, DbBytes> tasksColumnFamilies;
    private final ColumnFamily<DbString, DbLong> metadataColumnFamilies;

    private Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      queue = new DbBrokerTopologyCoordinatorState(store);
      query = new DbBrokerTopologyCoordinatorState(new StateStoreProcessingStore(storage));
      tasksColumnFamilies =
          store.createColumnFamily(
              ColumnFamilies.PD_BROKER_TOPOLOGY_SCHEDULES,
              new DbCompositeKey<>(new DbString(), new DbInt()),
              new DbBytes());
      metadataColumnFamilies =
          store.createColumnFamily(
              ColumnFamilies.PD_BROKER_TOPOLOGY_COORDINATOR_META, new DbString(), new DbLong());
    }

    private void writeLegacy(final long revision, final long queueTime) {
      final var record =
          new BrokerTopologyCoordinationRecord().setRevision(revision).setDueAt(queueTime);
      final var buffer = new ExpandableArrayBuffer();
      record.write(buffer, 0);
      final byte[] bytes = new byte[record.getLength()];
      buffer.getBytes(0, bytes);
      final var value = new DbBytes();
      value.wrapBytes(bytes);
      tasksColumnFamilies.upsert(legacyKey(), value);
    }

    private void writeMetadata(final String name, final long value) {
      final var number = new DbLong();
      number.wrapLong(value);
      metadataColumnFamilies.upsert(text(name), number);
    }

    private long metadataValue(final String name) {
      return metadataColumnFamilies.get(text(name)).getValue();
    }

    private void commit() throws Exception {
      store.getCurrentTransaction().commit();
    }

    @Override
    public void close() {
      storage.close();
    }
  }

  private static DbString text(final String value) {
    final var text = new DbString();
    text.wrapString(value);
    return text;
  }

  private static DbCompositeKey<DbString, DbInt> legacyKey() {
    final var partition = new DbInt();
    partition.wrapInt(-1);
    return new DbCompositeKey<>(text(""), partition);
  }
}
