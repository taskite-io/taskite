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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.broker.DbBrokerClusterState;
import io.taskite.protocol.Protocol;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerClusterKeyStateTest {
  @Test
  void allocationIsTransactionalAndNeverRecycled(@TempDir final Path directory) throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new DbBrokerClusterState(store);
      assertEquals(0, state.latestClusterKey());
      store.getCurrentTransaction();
      state.remember("first", 1);
      assertEquals(1, state.clusterKey("first"));
      state.remember("second", 2);
      assertEquals(2, state.latestClusterKey());
      store.getCurrentTransaction().rollback();
      assertTrue(state.clusterIds().isEmpty());
      assertEquals(0, state.latestClusterKey());

      store.getCurrentTransaction();
      state.remember("first", 1);
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      state.delete("first");
      store.getCurrentTransaction().commit();
      assertEquals(1, state.latestClusterKey());
      store.getCurrentTransaction();
      state.remember("first", 2);
      store.getCurrentTransaction().commit();
      assertEquals(2, state.clusterKey("first"));
    }
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new DbBrokerClusterState(store);
      assertEquals(2, state.clusterKey("first"));
      for (long key = 3; key < Protocol.MAXIMUM_CLUSTERS; key++) {
        state.remember("cluster-" + key, key);
      }
      assertEquals(15, state.latestClusterKey());
      assertThrows(IllegalArgumentException.class, () -> state.remember("overflow", 16));
      assertThrows(IllegalStateException.class, () -> state.remember("reuse", 0));
    }
  }

  @Test
  void reservedClusterKeyCannotBeAllocatedToABroker(@TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new DbBrokerClusterState(store);
      assertThrows(IllegalStateException.class, () -> state.remember("broker", 0));
      assertEquals(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, state.latestClusterKey());
      assertTrue(state.clusterIds().isEmpty());
    }
  }
}
