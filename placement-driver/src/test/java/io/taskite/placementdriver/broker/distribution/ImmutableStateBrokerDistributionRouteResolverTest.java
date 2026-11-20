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
package io.taskite.placementdriver.broker.distribution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ImmutableStateBrokerDistributionRouteResolverTest {
  @TempDir Path tempDir;

  @Test
  void routesCreatingPartitionToObservedLeader() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("creating"))) {
      final MutableProcessingState state = state(storage);
      configurePartition(state, PartitionStatus.CREATING, List.of("broker-0"));
      registerBroker(state, "broker-0", "LEADER", 7);

      final BrokerDistributionRoute route =
          new ImmutableStateBrokerDistributionRouteResolver(state)
              .resolveBrokerDistributionRoute("demo");

      assertEquals(1, route.partitionCount());
      assertEquals("broker-0", route.leaderEndpoint(1).orElseThrow().brokerId());
      assertEquals(26501, route.leaderEndpoint(1).orElseThrow().port());
    }
  }

  @Test
  void excludesOfflineLeader() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("offline"))) {
      final MutableProcessingState state = state(storage);
      configurePartition(state, PartitionStatus.CREATING, List.of("broker-0"));
      registerBroker(state, "broker-0", "LEADER", 7);
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", Instant.EPOCH);

      final BrokerDistributionRoute route =
          new ImmutableStateBrokerDistributionRouteResolver(state)
              .resolveBrokerDistributionRoute("demo");

      assertTrue(route.leaderEndpoint(1).isEmpty());
    }
  }

  @Test
  void suppressesConflictingLeadersAtSameTerm() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("conflicting"))) {
      final MutableProcessingState state = state(storage);
      configurePartition(state, PartitionStatus.CREATING, List.of("broker-0", "broker-1"));
      registerBroker(state, "broker-0", "LEADER", 7);
      registerBroker(state, "broker-1", "LEADER", 7);

      final BrokerDistributionRoute route =
          new ImmutableStateBrokerDistributionRouteResolver(state)
              .resolveBrokerDistributionRoute("demo");

      assertTrue(route.leaderEndpoint(1).isEmpty());
    }
  }

  @Test
  void rejectsUnknownCluster() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("unknown"))) {
      final MutableProcessingState state = state(storage);

      final IllegalStateException error =
          assertThrows(
              IllegalStateException.class,
              () ->
                  new ImmutableStateBrokerDistributionRouteResolver(state)
                      .resolveBrokerDistributionRoute("missing"));

      assertEquals("CLUSTER_NOT_REGISTERED: missing", error.getMessage());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void configurePartition(
      final MutableProcessingState state, final PartitionStatus status, final List<String> voters) {
    state
        .getBrokerClusterConfigState()
        .putClusterConfig("demo", new BrokerClusterConfig(voters.size(), 1, voters.size()));
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            new PartitionTopology(
                new PartitionTopology.Identity("demo", 1),
                new PartitionTopology.Placement(voters.size(), voters, voters.getFirst()),
                PartitionTopology.Membership.initial(voters, List.of(), voters.getFirst()),
                status,
                Map.of()));
  }

  private static void registerBroker(
      final MutableProcessingState state,
      final String brokerId,
      final String role,
      final long term) {
    final var legacy = new BrokerPartitionState(1, MemberRole.parse(role), term);
    final var report =
        new BrokerPartitionState(
            1,
            legacy.status(),
            legacy.replication(),
            legacy.operation(),
            new BrokerPartitionElectionState(
                new RaftConfigurationReference(1, "test"),
                new BrokerPartitionElectionState.Priorities(100, 100),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.valueOf("STATE_" + role), brokerId, true, 1000)));
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            brokerId,
            "127.0.0.1",
            "broker-0".equals(brokerId) ? 26501 : 26502,
            "broker-0".equals(brokerId) ? 26601 : 26602,
            "test",
            30_000,
            Instant.ofEpochMilli(1000),
            List.of(report));
  }
}
