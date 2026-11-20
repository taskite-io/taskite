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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ClusterChangeOperationStateTest {

  @Test
  void aggregateManifestAndActiveIndexSurviveStateReopen(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("manifest"))) {
      final MutableProcessingState state = state(storage);
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(operation(BrokerClusterChangeStatus.PENDING));

      final MutableProcessingState reopened = state(storage);
      final ClusterChangeOperation restored =
          reopened
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "change-1")
              .orElseThrow();

      assertEquals(11, restored.sourceTopologyVersion());
      assertEquals(List.of("broker-0", "broker-1"), restored.selectedBrokerIds());
      assertEquals(List.of("broker-1"), restored.removedBrokers());
      assertEquals("broker-0", restored.expectedTopology().getFirst().desiredPrimary());
      assertEquals(
          "change-1",
          reopened
              .getBrokerTopologyChangeState()
              .activeClusterChange("demo")
              .orElseThrow()
              .changeId());

      reopened
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(restored.withStatus(BrokerClusterChangeStatus.COMPLETED));
      assertTrue(reopened.getBrokerTopologyChangeState().activeClusterChange("demo").isEmpty());
      assertEquals(
          List.of("change-1"),
          reopened.getBrokerTopologyChangeState().recentClusterChanges("demo", 20).stream()
              .map(ClusterChangeOperation::changeId)
              .toList());
      reopened
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(operation("change-2", 200, BrokerClusterChangeStatus.PENDING));
      assertEquals(
          List.of("change-2"),
          reopened.getBrokerTopologyChangeState().recentClusterChanges("demo", 1).stream()
              .map(ClusterChangeOperation::changeId)
              .toList());

      reopened.getBrokerTopologyChangeState().deleteCluster("demo");
      assertTrue(
          reopened
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "change-1")
              .isEmpty());
      assertTrue(
          reopened.getBrokerTopologyChangeState().recentClusterChanges("demo", 20).isEmpty());
    }
  }

  @Test
  void retainsOnlyConfiguredNumberOfCompletedChanges(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("retention"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 0, () -> 0, 2);

      for (int index = 1; index <= 4; index++) {
        state
            .getBrokerTopologyChangeState()
            .putClusterChangeOperation(
                operation("change-" + index, index, BrokerClusterChangeStatus.COMPLETED));
      }

      assertEquals(
          List.of("change-4", "change-3"),
          state.getBrokerTopologyChangeState().recentClusterChanges("demo", 10).stream()
              .map(ClusterChangeOperation::changeId)
              .toList());
      assertTrue(
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "change-1")
              .isEmpty());
      assertTrue(
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "change-2")
              .isEmpty());

      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(operation("change-5", 5, BrokerClusterChangeStatus.PENDING));
      assertEquals(
          List.of("change-5", "change-4", "change-3"),
          state.getBrokerTopologyChangeState().recentClusterChanges("demo", 10).stream()
              .map(ClusterChangeOperation::changeId)
              .toList());
    }
  }

  private static ClusterChangeOperation operation(final BrokerClusterChangeStatus status) {
    return operation("change-1", 100, status);
  }

  private static ClusterChangeOperation operation(
      final String changeId,
      final long startedAtEpochMillis,
      final BrokerClusterChangeStatus status) {
    return new ClusterChangeOperation(
        "demo",
        changeId,
        11,
        1,
        1,
        1,
        List.of("broker-0"),
        List.of("broker-1"),
        List.of("broker-0", "broker-1"),
        List.of(
            new ClusterChangeOperation.ExpectedPartition(
                1,
                List.of("broker-0"),
                List.of("broker-0"),
                List.of(),
                "broker-0",
                Map.of("broker-0", 100))),
        List.of("admin:configuration-patch:change-1:1"),
        status,
        startedAtEpochMillis,
        200);
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }
}
