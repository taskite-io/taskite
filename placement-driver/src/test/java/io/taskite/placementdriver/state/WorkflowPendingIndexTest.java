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
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.common.codec.PartitionTopologyRecordCodec;
import io.taskite.placementdriver.state.immutable.BrokerDistributionScheduleState;
import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState;
import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class WorkflowPendingIndexTest {
  @Test
  void scheduleDeadlinesResolveDistinctTargetsAndReplaceOnlyTheirOwnEntries(
      @TempDir final Path directory) throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var distributions = new MutableProcessingState(store).getCommandDistributionState();
      for (final long id : List.of(3L, 91L)) {
        distributions.putDistribution(
            new CommandDistributionRecord<JobSubscriptionRecord>()
                .setDistributionKey(id)
                .setClusterId("origin")
                .setValueType(ValueType.JOB_SUBSCRIPTION)
                .setIntent(JobSubscriptionIntent.CREATE)
                .setValue(new JobSubscriptionRecord().setClusterId("origin")));
      }
      final var first =
          new BrokerDistributionScheduleState.Entry(new PendingTarget(91, "a:b", 2), 7, 11, 100);
      final var second =
          new BrokerDistributionScheduleState.Entry(new PendingTarget(3, "other", 1), 8, 12, 200);
      final var schedules = distributions.scheduling();
      schedules.put(first);
      schedules.put(second);
      store.getCurrentTransaction().commit();
      assertEquals(List.of(first, second), schedules.dueBefore(200, 10));
      assertEquals(List.of(first), schedules.dueBefore(200, 1));
      assertEquals(1, schedules.reservedTargets(100, 10));
      assertEquals(first, schedules.get(first.target()).orElseThrow());
      assertEquals(second, schedules.get(second.target()).orElseThrow());
      final var replacement = new BrokerDistributionScheduleState.Entry(first.target(), 9, 13, 300);
      store.getCurrentTransaction();
      schedules.put(replacement);
      assertEquals(List.of(second), schedules.dueBefore(200, 10));
      assertEquals(List.of(second, replacement), schedules.dueBefore(300, 10));
      store.getCurrentTransaction().rollback();
      assertEquals(List.of(first, second), schedules.dueBefore(200, 10));
      schedules.remove(first.target());
      assertEquals(List.of(second), schedules.dueBefore(300, 10));
      assertEquals(200, schedules.nextDueAt());
      schedules.remove(second.target());
      assertEquals(-1, schedules.nextDueAt());
      assertTrue(schedules.dueBefore(Long.MAX_VALUE, 10).isEmpty());
    }
  }

  @Test
  void coordinatorClusterDeletionKeepsOtherResourcesAndRevisionSequence(
      @TempDir final Path directory) throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var coordinator = new MutableProcessingState(store).getBrokerTopologyCoordinatorState();
      store.getCurrentTransaction();
      coordinator.markPartition("a", 1, 0);
      coordinator.markPartition("a:b", 2, 0);
      assertEquals(2, coordinator.getTask("a:b", 2).orElseThrow().revision());
      final var first = new BrokerTopologyCoordinatorState.Entry("a", 1, 1, "cursor", 9, 100, true);
      final var second = new BrokerTopologyCoordinatorState.Entry("a", 2, 2, "", 0, 150, true);
      final var survivor =
          new BrokerTopologyCoordinatorState.Entry("a:b", 2, 3, "other", 4, 200, false);
      coordinator.putTask(first);
      coordinator.putTask(second);
      coordinator.putTask(survivor);
      assertEquals(List.of(first, second, survivor), coordinator.dueBefore(200, 10));
      assertEquals(2, coordinator.reservedPartitions(0, 10));
      assertEquals(1, coordinator.reservedPartitions(100, 10));
      coordinator.deleteCluster("a");
      assertTrue(coordinator.getTask("a", 1).isEmpty());
      assertTrue(coordinator.getTask("a", 2).isEmpty());
      assertEquals(List.of(survivor), coordinator.dueBefore(200, 10));
      assertEquals(0, coordinator.reservedPartitions(0, 10));
      assertEquals(200, coordinator.nextDueAt());
      coordinator.markPartition("next", 1, 0);
      assertEquals(3, coordinator.getTask("next", 1).orElseThrow().revision());
      assertEquals(survivor, coordinator.getTask("a:b", 2).orElseThrow());
      store.getCurrentTransaction().commit();
    }
  }

  @Test
  void distributionPaginationIsExclusiveAndKeepsColonContainingClusters(
      @TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var distributions = state.getCommandDistributionState();
      distributions.putDistribution(
          new CommandDistributionRecord<JobSubscriptionRecord>()
              .setDistributionKey(1)
              .setClusterId("demo:east")
              .setValueType(ValueType.JOB_SUBSCRIPTION)
              .setIntent(JobSubscriptionIntent.CREATE)
              .setValue(new JobSubscriptionRecord().setClusterId("demo:east")));
      distributions.putDistribution(
          new CommandDistributionRecord<JobSubscriptionRecord>()
              .setDistributionKey(11)
              .setClusterId("demo")
              .setValueType(ValueType.JOB_SUBSCRIPTION)
              .setIntent(JobSubscriptionIntent.CREATE)
              .setValue(new JobSubscriptionRecord().setClusterId("demo")));
      distributions.resetTarget(1, "demo:east", 3);
      distributions.resetTarget(11, "demo", 1);
      final var visited = new ArrayList<PendingTarget>();
      var cursor = PendingTarget.START;
      while (true) {
        final var page = distributions.pendingTargetsAfter(cursor, 1);
        if (page.isEmpty()) {
          break;
        }
        assertEquals(1, page.size());
        visited.addAll(page);
        cursor = page.getLast();
      }
      assertEquals(
          List.of(
              new PendingTarget(1, "demo:east", 1),
              new PendingTarget(1, "demo:east", 2),
              new PendingTarget(1, "demo:east", 3),
              new PendingTarget(11, "demo", 1)),
          visited);
      distributions.acknowledge(1, "demo:east", 2);
      distributions.rebuildPendingIndex();
      assertEquals(3, distributions.pendingTargetsAfter(PendingTarget.START, 10).size());
      distributions.deleteCluster("demo:east");
      assertEquals(
          List.of(new PendingTarget(11, "demo", 1)),
          distributions.pendingTargetsAfter(PendingTarget.START, 10));
    }
  }

  @Test
  void legacyPrimaryStateRebuildsIndexesAfterRestart(@TempDir final Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      final var pending =
          store.createColumnFamily(
              ColumnFamilies.PD_PENDING_CHANGES, new DbString(), new DbString());
      final var plans =
          store.createColumnFamily(ColumnFamilies.PD_CHANGE_PLANS, new DbString(), new DbBytes());
      final var topologies =
          store.createColumnFamily(
              ColumnFamilies.PD_PARTITION_TOPOLOGY, new DbString(), new DbBytes());
      final var distributions =
          store.createColumnFamily(
              ColumnFamilies.PD_PENDING_DISTRIBUTIONS, new DbString(), DbNil.INSTANCE);
      plans.upsert(text("a:b:change1"), bytes(ChangePlanRecordCodec.toBytes(plan("a:b", 1))));
      pending.upsert(text("a:b:1"), text("change1"));
      topologies.upsert(
          text("a:b:1"), bytes(PartitionTopologyRecordCodec.toBytes(topology("a:b", 1))));
      distributions.upsert(text("17:a:b:1"), DbNil.INSTANCE);
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlansAfter("", 0, 10).isEmpty());
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerTopologyChangeState().rebuildPendingIndex();
      state.getBrokerPartitionTopologyState().rebuildTopologyIndex();
      state.getCommandDistributionState().rebuildPendingIndex();
      assertEquals(
          List.of(plan("a:b", 1)),
          state.getBrokerTopologyChangeState().pendingChangePlansAfter("", 0, 10));
      assertEquals(
          List.of(topology("a:b", 1)),
          state.getBrokerPartitionTopologyState().partitionTopologiesAfter("", 0, 10));
      assertEquals(
          List.of(new PendingTarget(17, "a:b", 1)),
          state.getCommandDistributionState().pendingTargetsAfter(PendingTarget.START, 10));
    }
  }

  @Test
  void pendingAndTopologyPagesRespectCursorAndAtomicRollback(@TempDir final Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      for (final String cluster : List.of("a", "a:b", "ab")) {
        for (int partition = 1; partition <= 2; partition++) {
          final var plan = plan(cluster, partition);
          state.getBrokerTopologyChangeState().putChangePlan(plan);
          state.getBrokerTopologyChangeState().markPending(plan);
          state
              .getBrokerPartitionTopologyState()
              .putPartitionTopology(topology(cluster, partition));
        }
      }
      store.getCurrentTransaction().commit();
      final var first =
          state.getBrokerTopologyChangeState().pendingChangePlansAfter("", 0, 1).getFirst();
      assertEquals(1, first.partitionId());
      assertEquals(
          2,
          state
              .getBrokerTopologyChangeState()
              .pendingChangePlansAfter(first.clusterId(), first.partitionId(), 1)
              .getFirst()
              .partitionId());
      store.getCurrentTransaction();
      state.getBrokerTopologyChangeState().removePending("a:b", 1);
      state.getBrokerPartitionTopologyState().deletePartitionTopology("a:b", 1);
      store.getCurrentTransaction().rollback();
      assertEquals(
          6, state.getBrokerTopologyChangeState().pendingChangePlansAfter("", 0, 10).size());
      assertEquals(
          6, state.getBrokerPartitionTopologyState().partitionTopologiesAfter("", 0, 10).size());
      state.getBrokerTopologyChangeState().deleteCluster("a:b");
      state.getBrokerPartitionTopologyState().deleteCluster("a:b");
      assertEquals(
          4, state.getBrokerTopologyChangeState().pendingChangePlansAfter("", 0, 10).size());
      assertEquals(
          4, state.getBrokerPartitionTopologyState().partitionTopologiesAfter("", 0, 10).size());
    }
  }

  private static ChangePlan plan(final String cluster, final int partition) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(cluster, "change" + partition, partition),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.join(partition, "broker"))),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        new ChangePlan.Deadline(1, Long.MAX_VALUE));
  }

  private static PartitionTopology topology(final String cluster, final int partition) {
    return new PartitionTopology(
        new PartitionTopology.Identity(cluster, partition),
        new PartitionTopology.Placement(1, List.of("broker"), "broker"),
        PartitionTopology.Membership.initial(List.of("broker"), List.of(), "broker"),
        PartitionStatus.ACTIVE,
        Map.of());
  }

  private static DbString text(final String value) {
    final var text = new DbString();
    text.wrapString(value);
    return text;
  }

  private static DbBytes bytes(final byte[] value) {
    final var bytes = new DbBytes();
    bytes.wrapBytes(value);
    return bytes;
  }
}
