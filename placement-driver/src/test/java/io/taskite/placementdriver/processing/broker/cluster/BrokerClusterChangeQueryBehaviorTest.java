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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.common.codec.ClusterChangeOperationCodec;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.appliers.BrokerClusterConfiguredApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerClusterChangeQueryBehaviorTest {
  @TempDir Path tempDir;

  @Test
  void activeQueryFallsBackToPendingPartitionPlan() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("pending-change"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final ChangePlan pending =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "rebalance:broker-4:1:change-1", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-4"),
                      TopologyOperation.promoteLearner(1, "broker-4"))),
              new ChangePlan.Progress(1, BrokerPartitionChangeStatus.WAITING_READY),
              new ChangePlan.Deadline(100, 200));
      state.getBrokerTopologyChangeState().putChangePlan(pending);
      state.getBrokerTopologyChangeState().markPending(pending);

      final BrokerClusterChangeStatusRecord result =
          (BrokerClusterChangeStatusRecord)
              new BrokerClusterChangeQueryBehavior(state)
                  .createResponseValue(new TopologyQueryRecord().setClusterId("demo"));

      assertEquals(pending.changeId(), result.getChangeId());
      assertEquals(ChangeStatus.WAITING_READY, result.getStatus());
      assertFalse(result.isTerminal());
      assertEquals(1, result.getPartitionChanges().size());
      assertEquals(pending.changeId(), result.getPartitionChanges().getFirst().changeId());
      assertEquals(2, result.getPlannedChanges().size());
      assertEquals(
          TopologyOperationType.PROMOTE_LEARNER,
          result.getPartitionChanges().getFirst().remainingOperations().getFirst().type());
    }
  }

  @Test
  void completedChangeRetainsPlanAndClearsPendingChanges() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("completed-change"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final ChangePlan completed =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "rebalance:broker-4:1:change-1", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-4"),
                      TopologyOperation.promoteLearner(1, "broker-4"))),
              new ChangePlan.Progress(2, BrokerPartitionChangeStatus.COMPLETED),
              new ChangePlan.Deadline(100, 200));
      state.getBrokerTopologyChangeState().putChangePlan(completed);

      final BrokerClusterChangeStatusRecord result =
          (BrokerClusterChangeStatusRecord)
              new BrokerClusterChangeQueryBehavior(state)
                  .createResponseValue(
                      new TopologyQueryRecord()
                          .setClusterId("demo")
                          .setChangeId(completed.changeId()));

      assertEquals(ChangeStatus.COMPLETED, result.getStatus());
      assertTrue(result.isTerminal());
      assertEquals(2, result.getPlannedChanges().size());
      assertEquals(List.of(), result.getPartitionChanges());
    }
  }

  @Test
  void configurationWithoutPartitionWorkPersistsCompletedManifest() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("noop-configuration"))) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(storage);
      final MutableProcessingState state = new MutableProcessingState(store);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerClusterConfiguredApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(1)
                  .setReplicationFactor(1)
                  .setAdminOperation(TopologyConfigureClusterProcessor.REBALANCE)
                  .setChangeId("noop-change")
                  .setRequestedAtEpochMillis(1234)
                  .setOutcome(ClusterConfigurationOutcome.NO_CHANGES));

      final DbString key = new DbString();
      key.wrapString("demo:noop-change");
      final byte[] stored =
          store
              .createColumnFamily(ColumnFamilies.PD_CLUSTER_CHANGE_OPERATIONS, key, new DbBytes())
              .get(key)
              .getBytes();
      // Assert the stored phase as well as the view, so legacy decoding cannot hide a writer bug.
      assertTrue(MsgPackConverter.convertToJson(stored).contains("\"phase\":\"COMPLETED\""));
      assertTrue(state.getBrokerTopologyChangeState().activeClusterChange("demo").isEmpty());
      final BrokerClusterChangeStatusRecord result = query(state, "noop-change");
      assertEquals(ChangeStatus.COMPLETED, result.getStatus());
      assertTrue(result.isTerminal());
      assertTrue(result.getPlannedChanges().isEmpty());
      assertTrue(result.getPartitionChanges().isEmpty());
      assertEquals(
          1234,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "noop-change")
              .orElseThrow()
              .startedAtEpochMillis());
    }
  }

  @Test
  void legacyPendingEmptyManifestNoLongerBlocksAfterStorageReopen() {
    final Path path = tempDir.resolve("legacy-noop");
    final ClusterChangeOperation legacy = aggregate(BrokerClusterChangeStatus.PENDING, List.of());
    assertTrue(
        MsgPackConverter.convertToJson(ClusterChangeOperationCodec.toBytes(legacy))
            .contains("\"phase\":\"PENDING\""));
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerTopologyChangeState().putClusterChangeOperation(legacy);
    }
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertTrue(state.getBrokerTopologyChangeState().activeClusterChange("demo").isEmpty());
      final BrokerClusterChangeStatusRecord result = query(state, legacy.changeId());
      assertEquals(ChangeStatus.COMPLETED, result.getStatus());
      assertTrue(result.isTerminal());
      assertEquals(
          BrokerClusterChangeStatus.COMPLETED,
          state.getBrokerTopologyChangeState().recentClusterChanges("demo", 1).getFirst().status());
      assertEquals(
          "ACTIVE_CHANGE_NOT_FOUND: Expected to query the active topology change in cluster 'demo', but no active change was found",
          assertThrows(TopologyQueryRejection.class, () -> query(state, "")).getMessage());
    }
  }

  @Test
  void aggregateWithMissingPlanRemainsPendingAndQueryable() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("missing-child"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final ClusterChangeOperation operation =
          aggregate(
              BrokerClusterChangeStatus.PENDING, List.of("admin:rebalance:aggregate-change:1"));
      state.getBrokerTopologyChangeState().putClusterChangeOperation(operation);

      final BrokerClusterChangeStatusRecord result = query(state, "");
      assertEquals(operation.changeId(), result.getChangeId());
      assertEquals(ChangeStatus.PENDING, result.getStatus());
      assertFalse(result.isTerminal());
      assertEquals(1, result.getExpectedTopology().size());
      assertEquals("broker-0", result.getExpectedTopology().getFirst().desiredPrimary());
      assertTrue(state.getBrokerTopologyChangeState().activeClusterChange("demo").isPresent());
    }
  }

  @Test
  void aggregateTerminalStatusIsAuthoritativeWithoutChildPlans() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("cancelled-aggregate"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(aggregate(BrokerClusterChangeStatus.CANCELLED, List.of()));

      final BrokerClusterChangeStatusRecord result = query(state, "aggregate-change");
      assertEquals(ChangeStatus.CANCELLED, result.getStatus());
      assertTrue(result.isTerminal());
    }
  }

  @Test
  void unknownChangeStillReturnsNotFound() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("unknown-change"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertEquals(
          "CHANGE_NOT_FOUND: Expected to query topology change 'unknown' in cluster 'demo', but no matching change was found",
          assertThrows(TopologyQueryRejection.class, () -> query(state, "unknown")).getMessage());
    }
  }

  private static BrokerClusterChangeStatusRecord query(
      final MutableProcessingState state, final String changeId) {
    return (BrokerClusterChangeStatusRecord)
        new BrokerClusterChangeQueryBehavior(state)
            .createResponseValue(
                new TopologyQueryRecord().setClusterId("demo").setChangeId(changeId));
  }

  private static ClusterChangeOperation aggregate(
      final BrokerClusterChangeStatus status, final List<String> planIds) {
    return new ClusterChangeOperation(
        "demo",
        "aggregate-change",
        1,
        1,
        1,
        1,
        List.of("broker-0"),
        List.of(),
        List.of("broker-0"),
        List.of(
            new ClusterChangeOperation.ExpectedPartition(
                1,
                List.of("broker-0"),
                List.of("broker-0"),
                List.of(),
                "broker-0",
                Map.of("broker-0", 100))),
        planIds,
        status,
        100,
        200);
  }
}
