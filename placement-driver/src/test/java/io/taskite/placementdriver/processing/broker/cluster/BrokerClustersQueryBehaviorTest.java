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

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClustersRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerClustersQueryBehaviorTest {
  @TempDir Path tempDir;

  @Test
  void returnsConfiguredAndCurrentBrokerCounts() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("clusters"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state.getBrokerClusterState().remember("demo", 1);
      state.getBrokerClusterState().remember("demo-2", 2);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(3, 5, 3));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo-2", new BrokerClusterConfig(3, 5, 3));
      register(state, "broker-0", 26501, 26601);
      register(state, "broker-1", 26502, 26602);
      state
          .getBrokerRegistrationState()
          .markBrokerOffline("demo", "broker-1", Instant.ofEpochSecond(2));

      final BrokerClustersRecord result =
          (BrokerClustersRecord)
              new BrokerClustersQueryBehavior(state).createResponseValue(new TopologyQueryRecord());

      assertEquals(2, result.getCount());
      assertEquals(
          List.of("demo", "demo-2"),
          result.getClusters().stream().map(BrokerClustersRecord.Cluster::getClusterId).toList());
      assertEquals(3, result.getClusters().getFirst().getClusterSize());
      assertEquals(5, result.getClusters().getFirst().getPartitionSize());
      assertEquals(3, result.getClusters().getFirst().getReplicationFactor());
      assertEquals(2, result.getClusters().getFirst().getRegisteredBrokerCount());
      assertEquals(1, result.getClusters().getFirst().getOnlineBrokerCount());
      assertEquals(0, result.getClusters().get(1).getRegisteredBrokerCount());
      assertEquals(0, result.getClusters().get(1).getOnlineBrokerCount());
    }
  }

  private static void register(
      final MutableProcessingState state,
      final String brokerId,
      final int port,
      final int raftPort) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            brokerId,
            "127.0.0.1",
            port,
            raftPort,
            "test",
            30_000,
            Instant.EPOCH,
            List.of());
  }
}
