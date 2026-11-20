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
package io.taskite.placementdriver.broker.topology;

import static io.taskite.placementdriver.client.mapper.PlacementDriverResponseMapper.toClusterTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.placementdriver.api.PlacementDriverResponseMapper;
import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyQueryBehavior;
import io.taskite.placementdriver.state.appliers.PartitionChangeAdmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyRecord;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerTopologyCalculatorTest {
  @TempDir Path tempDir;

  @Test
  void expandedPartitionCountIsVisibleInQueryAndWatchResponses() throws Exception {
    try (var storage = new PartitionStorage(tempDir)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store);
      store.getCurrentTransaction();
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 2, 1));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 5, 1));
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-0",
              "127.0.0.1",
              26501,
              26601,
              "test",
              30000,
              Instant.EPOCH,
              List.of());
      final var topology =
          new BrokerTopologyCalculator(state).calculateBrokerTopology("demo", Instant.EPOCH);
      assertEquals(5, topology.partitionSize());
      final var mapper = new PlacementDriverResponseMapper();
      assertEquals(5, mapper.toBrokerTopology(topology).getPartitionSize());
      final var response =
          (TopologyRecord)
              new BrokerTopologyQueryBehavior(state)
                  .createResponseValue(new TopologyQueryRecord().setClusterId("demo"));
      final var grpc = mapper.toBrokerTopology(response);
      assertEquals(5, grpc.getPartitionSize());
      assertEquals(5, toClusterTopology(grpc).partitionSize());
      store.getCurrentTransaction().commit();
    }
  }

  @Test
  void pendingJoinBootstrapsLearnerBeforeAckAndSurvivesRecovery() {
    final Path directory = tempDir.resolve("pending-join");
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(2, 1, 2));
      for (int broker = 0; broker < 3; broker++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + broker,
                "127.0.0.1",
                26501 + broker,
                26601 + broker,
                "test",
                30_000,
                Instant.EPOCH,
                List.of());
      }
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "recover-test", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-2"),
                      TopologyOperation.promoteLearner(1, "broker-2"),
                      TopologyOperation.leave(1, "broker-1"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  BrokerTopologyOperationRequests.freeze(state, plan)
                      .withExecution(
                          new ChangePlan.Execution(
                              1, 1, "", "", plan.changeId(), PlanBlockedReason.NONE, 0))));
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
      final var queued =
          state.getBrokerTopologyChangeState().changePlan("demo", plan.changeId()).orElseThrow();
      new PartitionChangeAdmittedApplier(state)
          .applyState(
              2,
              BrokerPartitionChangeRecordMapper.fromPlan(BrokerMigrationBudget.admit(queued, 1)));
      assertPendingJoin(state);
    }
    try (var storage = new PartitionStorage(directory)) {
      final var recovered = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertPendingJoin(recovered);
      final var plan =
          recovered.getBrokerTopologyChangeState().changePlan("demo", "recover-test").orElseThrow();
      recovered.getBrokerRegistrationState().markBrokerDraining("demo", "broker-2", Instant.EPOCH);
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(recovered)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
      recovered.getBrokerRegistrationState().markBrokerOnline("demo", "broker-2");
      recovered.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-2");
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(recovered)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
      recovered.getBrokerRegistrationState().includeInPlacement("demo", "broker-2");
      assertPendingJoin(recovered);
      recovered
          .getBrokerTopologyChangeState()
          .putChangePlan(plan.withStatus(BrokerPartitionChangeStatus.SUPERSEDING));
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(recovered)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
      recovered
          .getBrokerTopologyChangeState()
          .putChangePlan(plan.withStatus(BrokerPartitionChangeStatus.FAILED));
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(recovered)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
    }
  }

  private static void assertPendingJoin(final MutableProcessingState state) {
    final var partition =
        new BrokerTopologyCalculator(state)
            .calculateBrokerTopology("demo", Instant.EPOCH)
            .partitions()
            .getFirst();
    assertEquals(List.of("broker-2"), partition.learners());
    assertEquals(0, partition.priorities().get("broker-2"));
    assertEquals(List.of("broker-0", "broker-1"), partition.voters());
    final var committed =
        state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
    assertEquals(List.of(), committed.learners());
    assertEquals(List.of("broker-0", "broker-1"), committed.currentVoters());
    assertEquals(
        0,
        state
            .getBrokerTopologyChangeState()
            .changePlan("demo", "recover-test")
            .orElseThrow()
            .nextOperationIndex());
  }

  @Test
  void distinguishesUnknownClusterFromTemporarilyUnavailableTopology() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("topology"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final BrokerTopologyCalculator calculator = new BrokerTopologyCalculator(state);

      final BrokerTopologyCalculationException missing =
          assertThrows(
              BrokerTopologyCalculationException.class,
              () -> calculator.calculateBrokerTopology("missing", Instant.EPOCH));
      assertEquals(BrokerTopologyCalculationException.Status.NOT_FOUND, missing.status());
      assertEquals("CLUSTER_NOT_REGISTERED: missing", missing.getMessage());

      state
          .getBrokerClusterConfigState()
          .putClusterConfig("registered", new BrokerClusterConfig(1, 1, 1));
      final BrokerTopologyCalculationException unavailable =
          assertThrows(
              BrokerTopologyCalculationException.class,
              () -> calculator.calculateBrokerTopology("registered", Instant.EPOCH));
      assertEquals(BrokerTopologyCalculationException.Status.UNAVAILABLE, unavailable.status());
      assertEquals("NO_BROKER_REGISTERED: registered", unavailable.getMessage());
    }
  }

  @Test
  void preservesOfflineVoterEndpointsUntilMembershipRemovesThem() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("offline-voter"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(2, 1, 2));
      for (int broker = 0; broker < 3; broker++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + broker,
                "127.0.0.1",
                26501 + broker,
                26601 + broker,
                "test",
                30_000,
                Instant.EPOCH,
                List.of());
      }
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", Instant.EPOCH);
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-2", Instant.EPOCH);
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final BrokerTopologyCalculator calculator = new BrokerTopologyCalculator(state);

      final BrokerTopologySnapshot snapshot =
          calculator.calculateBrokerTopologySnapshot("demo", Instant.EPOCH);
      assertEquals(
          Map.of(
              "broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601),
              "broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26502, 26602)),
          snapshot.topology().brokers());
      assertEquals(
          List.of("broker-0", "broker-1"), snapshot.topology().partitions().getFirst().voters());
      final TopologyRecord response =
          (TopologyRecord)
              new BrokerTopologyQueryBehavior(state)
                  .createResponseValue(new TopologyQueryRecord().setClusterId("demo"));
      final var grpcResponse = new PlacementDriverResponseMapper().toBrokerTopology(response);
      assertEquals(
          List.of("broker-0", "broker-1"),
          grpcResponse.getBrokersList().stream()
              .map(broker -> broker.getBrokerId())
              .sorted()
              .toList());
      final var offlineVoter =
          grpcResponse.getBrokersList().stream()
              .filter(broker -> broker.getBrokerId().equals("broker-0"))
              .findFirst()
              .orElseThrow();
      assertEquals(26601, offlineVoter.getRaftPort());
      assertEquals("OFFLINE", offlineVoter.getStatus());

      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(1, List.of("broker-1"), "broker-1"),
                  PartitionTopology.Membership.initial(List.of("broker-1"), List.of(), "broker-1"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      assertEquals(
          Map.of("broker-1", new BrokerEndpoint("broker-1", "127.0.0.1", 26502, 26602)),
          calculator.calculateBrokerTopology("demo", Instant.EPOCH).brokers());
    }
  }

  @Test
  void preservesOfflineLearnerEndpointsUntilMembershipRemovesThemAndSurvivesRecovery() {
    final Path directory = tempDir.resolve("offline-learners");
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1));
      for (final int broker : List.of(0, 3, 4, 5)) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + broker,
                "127.0.0.1",
                26501 + broker,
                26601 + broker,
                "test",
                30_000,
                Instant.EPOCH,
                List.of());
        if (broker != 0) {
          state
              .getBrokerRegistrationState()
              .markBrokerOffline("demo", "broker-" + broker, Instant.EPOCH);
        }
      }
      putLearnerMembership(state, List.of("broker-3", "broker-4"));
      assertOfflineLearnerEndpoints(state, List.of("broker-3", "broker-4"));
    }
    try (var storage = new PartitionStorage(directory)) {
      final var recovered = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertOfflineLearnerEndpoints(recovered, List.of("broker-3", "broker-4"));
      putLearnerMembership(recovered, List.of("broker-3"));
      assertOfflineLearnerEndpoints(recovered, List.of("broker-3"));
      putLearnerMembership(recovered, List.of());
      assertOfflineLearnerEndpoints(recovered, List.of());
    }
  }

  private static void putLearnerMembership(
      final MutableProcessingState state, final List<String> learners) {
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            new PartitionTopology(
                new PartitionTopology.Identity("demo", 1),
                new PartitionTopology.Placement(1, List.of("broker-0"), "broker-0"),
                PartitionTopology.Membership.initial(List.of("broker-0"), learners, "broker-0"),
                PartitionStatus.REBALANCING,
                Map.of()));
  }

  private static void assertOfflineLearnerEndpoints(
      final MutableProcessingState state, final List<String> learners) {
    final var topology =
        new BrokerTopologyCalculator(state).calculateBrokerTopology("demo", Instant.EPOCH);
    final var partition = topology.partitions().getFirst();
    assertEquals(List.of("broker-0"), partition.voters());
    assertEquals(learners, partition.learners());
    assertEquals("", partition.leaderId());
    assertFalse(partition.serviceReady());
    final var expected = new LinkedHashMap<String, BrokerEndpoint>();
    expected.put("broker-0", new BrokerEndpoint("broker-0", "127.0.0.1", 26501, 26601));
    for (final String learner : learners) {
      final int broker = Integer.parseInt(learner.substring("broker-".length()));
      expected.put(
          learner, new BrokerEndpoint(learner, "127.0.0.1", 26501 + broker, 26601 + broker));
    }
    assertEquals(expected, topology.brokers());
    final var response =
        (TopologyRecord)
            new BrokerTopologyQueryBehavior(state)
                .createResponseValue(new TopologyQueryRecord().setClusterId("demo"));
    final var grpcResponse = new PlacementDriverResponseMapper().toBrokerTopology(response);
    assertEquals(expected, toClusterTopology(grpcResponse).brokers());
    assertEquals(
        expected.keySet().stream().sorted().toList(),
        grpcResponse.getBrokersList().stream()
            .map(broker -> broker.getBrokerId())
            .sorted()
            .toList());
    for (final String learner : learners) {
      final var broker =
          grpcResponse.getBrokersList().stream()
              .filter(registration -> registration.getBrokerId().equals(learner))
              .findFirst()
              .orElseThrow();
      assertEquals(expected.get(learner).raftPort(), broker.getRaftPort());
      assertEquals("OFFLINE", broker.getStatus());
    }
  }
}
