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
package io.taskite.placementdriver.state.appliers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerClusterDeletedApplierTest {
  @TempDir Path tempDir;

  @Test
  void removesSourceTargetAndReciprocalMirrorsAndTheirPairIndexes() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("both-directions"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      seed(state);

      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));

      assertDeleted(state);
    }
  }

  @Test
  void deletionSurvivesRestartAndClusterRecreationHasNoStalePairIndex() throws Exception {
    final Path directory = tempDir.resolve("restart");
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      seed(state);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));
      store.getCurrentTransaction().commit();
    }

    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      assertDeleted(state);
      // Replaying the deletion is harmless, including when the cluster has no mirrors.
      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));
      assertDeleted(state);
      registerCluster(state, "a");
      putMirror(state, 11, "a", "b");
      putMirror(state, 12, "b", "a");
      assertEquals(
          11, state.getJobSubscriptionMirrorState().mirror("a", "b").orElseThrow().getMirrorKey());
      assertEquals(
          12, state.getJobSubscriptionMirrorState().mirror("b", "a").orElseThrow().getMirrorKey());
      store.getCurrentTransaction().commit();
    }

    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertEquals(
          11, state.getJobSubscriptionMirrorState().mirror("a", "b").orElseThrow().getMirrorKey());
      assertEquals(
          12, state.getJobSubscriptionMirrorState().mirror("b", "a").orElseThrow().getMirrorKey());
      assertTrue(state.getJobSubscriptionMirrorState().mirror(1).isEmpty());
      assertTrue(state.getJobSubscriptionMirrorState().mirror(2).isEmpty());
    }
  }

  @Test
  void rollingBackDeletionRestoresClusterAndAllAssociatedMirrors() throws Exception {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("rollback"))) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      seed(state);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      new BrokerClusterDeletedApplier(state)
          .applyState(1, new ClusterConfigRecord().setClusterId("a"));
      assertDeleted(state);

      store.getCurrentTransaction().rollback();

      assertTrue(state.getBrokerClusterState().clusterIds().contains("a"));
      assertTrue(state.getBrokerClusterConfigState().findClusterConfig("a").isPresent());
      assertEquals(6, state.getJobSubscriptionMirrorState().mirrors().size());
      assertEquals(
          1, state.getJobSubscriptionMirrorState().mirror("a", "b").orElseThrow().getMirrorKey());
      assertEquals(
          2, state.getJobSubscriptionMirrorState().mirror("b", "a").orElseThrow().getMirrorKey());
      assertEquals(
          3, state.getJobSubscriptionMirrorState().mirror("a", "c").orElseThrow().getMirrorKey());
      assertEquals(
          4, state.getJobSubscriptionMirrorState().mirror("c", "a").orElseThrow().getMirrorKey());
    }
  }

  private static void seed(final MutableProcessingState state) {
    for (final String clusterId : List.of("a", "b", "c", "aa")) {
      registerCluster(state, clusterId);
    }
    putMirror(state, 1, "a", "b");
    putMirror(state, 2, "b", "a");
    putMirror(state, 3, "a", "c");
    putMirror(state, 4, "c", "a");
    putMirror(state, 5, "b", "c");
    putMirror(state, 6, "aa", "b");
  }

  private static void assertDeleted(final MutableProcessingState state) {
    assertFalse(state.getBrokerClusterState().clusterIds().contains("a"));
    assertTrue(state.getBrokerClusterConfigState().findClusterConfig("a").isEmpty());
    assertEquals(
        List.of(5L, 6L),
        state.getJobSubscriptionMirrorState().mirrors().stream()
            .map(JobSubscriptionMirrorRecord::getMirrorKey)
            .toList());
    for (final long key : List.of(1L, 2L, 3L, 4L)) {
      assertTrue(state.getJobSubscriptionMirrorState().mirror(key).isEmpty());
    }
    assertTrue(state.getJobSubscriptionMirrorState().mirrorsFrom("a").isEmpty());
    assertTrue(state.getJobSubscriptionMirrorState().mirrorsTo("a").isEmpty());
    assertTrue(state.getJobSubscriptionMirrorState().mirror("a", "b").isEmpty());
    assertTrue(state.getJobSubscriptionMirrorState().mirror("b", "a").isEmpty());
    assertTrue(state.getJobSubscriptionMirrorState().mirror("a", "c").isEmpty());
    assertTrue(state.getJobSubscriptionMirrorState().mirror("c", "a").isEmpty());
    assertEquals(
        5, state.getJobSubscriptionMirrorState().mirror("b", "c").orElseThrow().getMirrorKey());
    assertEquals(
        6, state.getJobSubscriptionMirrorState().mirror("aa", "b").orElseThrow().getMirrorKey());
    assertTrue(state.getBrokerClusterConfigState().findClusterConfig("aa").isPresent());
  }

  private static void registerCluster(final MutableProcessingState state, final String clusterId) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId(clusterId)
                .setClusterSize(1)
                .setPartitionSize(1)
                .setReplicationFactor(1));
  }

  private static void putMirror(
      final MutableProcessingState state,
      final long mirrorKey,
      final String sourceClusterId,
      final String targetClusterId) {
    new JobSubscriptionMirrorCreatedApplier(state)
        .applyState(
            mirrorKey,
            new JobSubscriptionMirrorRecord()
                .setMirrorKey(mirrorKey)
                .setSourceClusterId(sourceClusterId)
                .setTargetClusterId(targetClusterId)
                .setCreatedAtEpochMillis(1));
  }
}
