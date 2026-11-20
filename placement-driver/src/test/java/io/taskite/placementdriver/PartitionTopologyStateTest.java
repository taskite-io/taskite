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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.appliers.BrokerClusterConfiguredApplier;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.BrokerMarkedOfflineApplier;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.BrokerRenewedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Phase 1 coverage: the persisted {@link PartitionTopology} materialized by the appliers must equal
 * the historical round-robin assignment, and must survive a restart via event-sourced state (no
 * recomputation needed).
 */
class PartitionTopologyStateTest {
  @TempDir Path tempDir;

  private static final Instant T0 = Instant.parse("2024-01-01T00:00:00Z");

  @Test
  void materializesRoundRobinPlacementOnClusterAndBrokerEvents() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerBroker(state, "demo", "broker-1");
      registerBroker(state, "demo", "broker-2");
      registerCluster(state, "demo", 3, 6, 3);

      final List<PartitionTopology> persisted =
          state.getBrokerPartitionTopologyState().partitionTopologies("demo");
      assertEquals(6, persisted.size());

      // Compare against the exact same planner the read path falls back to.
      final BrokerClusterConfig config = state.getBrokerClusterConfigState().clusterConfig("demo");
      final List<BrokerRegistration> registrations =
          state.getBrokerRegistrationState().registrations("demo");
      final List<PartitionTopology> expected =
          BrokerTopologyPlanner.plan("demo", config, registrations).stream()
              .map(topology -> topology.withInitializing(true))
              .toList();
      assertEquals(expected, persisted);

      // partition 1 (index 0): replicas = broker-0,1,2 (rf=3, cluster=3)
      final PartitionTopology p1 = persisted.get(0);
      assertEquals(List.of("broker-0", "broker-1", "broker-2"), p1.currentVoters());
      assertEquals(p1.currentVoters(), p1.desiredVoters());
      assertEquals(PartitionStatus.ACTIVE, p1.status());
      assertEquals("broker-0", p1.desiredPrimary());
      assertTrue(p1.learners().isEmpty());
    }
  }

  @Test
  void keepsBrokersBeyondClusterSizeAsSpareLearners() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerBroker(state, "demo", "broker-1");
      registerBroker(state, "demo", "broker-2");
      registerCluster(state, "demo", 2, 2, 2);

      for (final PartitionTopology partition :
          state.getBrokerPartitionTopologyState().partitionTopologies("demo")) {
        assertEquals(2, partition.currentVoters().size());
        assertEquals(List.of("broker-2"), partition.learners());
        assertEquals(0, partition.priorities().get("broker-2"));
        assertFalse(partition.currentVoters().contains("broker-2"));
      }
    }
  }

  @Test
  void doesNotAssignUnselectedVoterPoolBrokersAsLearners() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("voter-pool"))) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerBroker(state, "demo", "broker-1");
      registerBroker(state, "demo", "broker-2");
      registerBroker(state, "demo", "broker-3");
      registerBroker(state, "demo", "broker-4");
      registerCluster(state, "demo", 5, 5, 3);

      for (final PartitionTopology partition :
          state.getBrokerPartitionTopologyState().partitionTopologies("demo")) {
        assertEquals(3, partition.currentVoters().size());
        assertTrue(partition.learners().isEmpty());
      }
    }
  }

  @Test
  void topologyConfigurationPersistsClusterSizeAndReplicationFactor() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("configuration"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state, "demo", 3, 2, 2);

      new BrokerClusterConfiguredApplier(state)
          .applyState(
              2,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(5)
                  .setPartitionSize(2)
                  .setReplicationFactor(3));

      assertEquals(
          new BrokerClusterConfig(5, 2, 3),
          state.getBrokerClusterConfigState().clusterConfig("demo"));
    }
  }

  @Test
  void preservesCurrentMembershipWhenBrokerGoesOffline() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerBroker(state, "demo", "broker-1");
      registerBroker(state, "demo", "broker-2");
      registerCluster(state, "demo", 3, 3, 3);

      new BrokerMarkedOfflineApplier(state, () -> T0)
          .applyState(1, brokerRecord("demo", "broker-2"));

      assertEquals(
          BrokerStatus.OFFLINE,
          state.getBrokerRegistrationState().registrations("demo").stream()
              .filter(registration -> registration.brokerId().equals("broker-2"))
              .findFirst()
              .orElseThrow()
              .status());
      // Membership is a raft fact and remains unchanged until an explicit recovery plan applies.
      for (final PartitionTopology partition :
          state.getBrokerPartitionTopologyState().partitionTopologies("demo")) {
        assertTrue(partition.currentVoters().contains("broker-2"));
      }
    }
  }

  @Test
  void brokerObservationDoesNotOverwriteDesiredPriority() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("priority-observation"))) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerCluster(state, "demo", 1, 1, 1);

      new BrokerRenewedApplier(state, () -> T0)
          .applyState(
              1,
              brokerRecord("demo", "broker-0")
                  .setPartitions(
                      List.of(
                          new BrokerRegistrationRecord.PartitionStateRecord()
                              .setPartitionId(1)
                              .setRole(MemberRole.LEADER)
                              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
                              .setPriority(10))));

      final PartitionTopology topology =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(PartitionTopology.PRIMARY_PRIORITY, topology.priorities().get("broker-0"));
      assertEquals(10, topology.replicaStates().get("broker-0").priority());
    }
  }

  @Test
  void newRegistrationWithEmptyReportInvalidatesPreviousReplica() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("incarnation-report"))) {
      final MutableProcessingState state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, brokerRecord("demo", "broker-0").setIncarnationId("old-startup"));
      registerCluster(state, "demo", 1, 1, 1);
      new BrokerRenewedApplier(state, () -> T0.plusSeconds(1))
          .applyState(
              1,
              brokerRecord("demo", "broker-0")
                  .setIncarnationId("old-startup")
                  .setPartitions(
                      List.of(
                          new BrokerRegistrationRecord.PartitionStateRecord()
                              .setPartitionId(1)
                              .setRole(MemberRole.LEADER)
                              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
                              .setPriority(100))));
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .containsKey("broker-0"));
      new BrokerMarkedOfflineApplier(state, () -> T0.plusSeconds(2))
          .applyState(1, brokerRecord("demo", "broker-0"));
      new BrokerRegisteredApplier(state, () -> T0.plusSeconds(3))
          .applyState(1, brokerRecord("demo", "broker-0").setIncarnationId("new-startup"));
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .replicaStates()
              .isEmpty(),
          "empty startup REGISTER must not yield the previous process observation");
    }
  }

  @Test
  void persistedPlacementSurvivesRestart() {
    final Path runtime = tempDir.resolve("runtime");
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "demo", "broker-0");
      registerBroker(state, "demo", "broker-1");
      registerBroker(state, "demo", "broker-2");
      registerCluster(state, "demo", 3, 3, 3);
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState reopened = state(storage);
      final List<PartitionTopology> persisted =
          reopened.getBrokerPartitionTopologyState().partitionTopologies("demo");
      assertEquals(3, persisted.size());
      assertEquals(List.of("broker-0", "broker-1", "broker-2"), persisted.get(0).currentVoters());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void registerCluster(
      final MutableProcessingState state,
      final String clusterId,
      final int clusterSize,
      final int partitionSize,
      final int replicationFactor) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId(clusterId)
                .setClusterSize(clusterSize)
                .setPartitionSize(partitionSize)
                .setReplicationFactor(replicationFactor));
  }

  private static void registerBroker(
      final MutableProcessingState state, final String clusterId, final String brokerId) {
    if (!state.getBrokerClusterState().clusterIds().contains(clusterId)) {
      state
          .getBrokerClusterState()
          .remember(clusterId, state.getBrokerClusterState().latestClusterKey() + 1);
    }
    new BrokerRegisteredApplier(state, () -> T0).applyState(1, brokerRecord(clusterId, brokerId));
  }

  private static BrokerRegistrationRecord brokerRecord(
      final String clusterId, final String brokerId) {
    return new BrokerRegistrationRecord()
        .setClusterId(clusterId)
        .setBrokerId(brokerId)
        .setHost("127.0.0.1")
        .setPort(26501)
        .setRaftPort(26601)
        .setVersion("0.1.0-SNAPSHOT");
  }
}
