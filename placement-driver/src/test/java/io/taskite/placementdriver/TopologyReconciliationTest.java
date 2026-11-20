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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPlanDependencies;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyCoordinatorChecker;
import io.taskite.placementdriver.processing.broker.topology.TopologyCoordinateProcessor;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.BrokerDrainedApplier;
import io.taskite.placementdriver.state.appliers.BrokerMarkedOfflineApplier;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeAdmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionJoinedApplier;
import io.taskite.placementdriver.state.appliers.PartitionLeftApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class TopologyReconciliationTest {
  private static final Map<ReconciliationFixture, WorkflowScheduler> SCHEDULERS =
      new IdentityHashMap<>();

  private static void reconcile(final ReconciliationFixture actor) {
    actor.requestReconciliation();
    final var scheduling = SCHEDULERS.get(actor);
    for (int round = 0; round < 4; round++) {
      scheduling.clock.addAndGet(100);
      scheduling.runReady();
    }
  }

  @Test
  void emptyRecoveryAndRepeatedDirtyNotificationsStopWithoutPolling(@TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var submitted = new ArrayList<LoggedEvent>();
      final var actor = actor(state, submitted);
      final var scheduling = SCHEDULERS.get(actor);
      reconcile(actor);
      assertTrue(scheduling.tasks.isEmpty());
      actor.requestReconciliation("absent", 1);
      actor.requestReconciliation("absent", 1);
      actor.requestReconciliation("absent");
      assertEquals(1, scheduling.tasks.size());
      reconcile(actor);
      assertTrue(scheduling.tasks.isEmpty());
      assertTrue(submitted.isEmpty());
      actor.close();
    }
  }

  @Test
  void recoveryHonorsSingleCommandBudgetAndObservesUnappliedSubmission(
      @TempDir final Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      registerClusterAndBrokers(state);
      for (int partition = 1; partition <= 2; partition++) {
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", partition),
                    new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
                    PartitionTopology.Membership.initial(
                        List.of("broker-0"), List.of(), "broker-0"),
                    PartitionStatus.ACTIVE,
                    Map.of()));
      }
      final var submitted = new ArrayList<LoggedEvent>();
      final var actor = actor(state, submitted);
      final var scheduling = SCHEDULERS.get(actor);
      scheduling.capacity = 1;
      for (int round = 0; round < 8; round++) {
        scheduling.clock.addAndGet(100);
        scheduling.runReady();
      }
      assertEquals(2, submitted.size());
      scheduling.clock.addAndGet(100);
      scheduling.runReady();
      assertEquals(2, submitted.size());
      assertTrue(scheduling.tasks.stream().allMatch(task -> task.due >= 15_000));
      actor.requestReconciliation("demo", 1);
      actor.requestReconciliation("demo", 1);
      scheduling.clock.addAndGet(100);
      scheduling.runReady();
      assertEquals(2, submitted.size(), "dirty wakes must preserve the submission observation");
      actor.close();
    }
  }

  @Test
  void partialRosterDoesNotRepeatNoOpBootstrapTransfer(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(1)
                  .setReplicationFactor(1));
      new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
          .applyState(1, brokerWithPartition("broker-0", "LEADER"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      reconcile(actor);
      assertTrue(submitted.isEmpty(), "wait for the declared initial roster before a no-op finish");
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());
      actor.close();
    }
  }

  @Test
  void singletonBootstrapWaitsForInitialLeaderReport(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(1)
                  .setReplicationFactor(1));
      register(state, "broker-0");
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      reconcile(actor);
      assertTrue(submitted.isEmpty());
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());

      new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
          .applyState(1, brokerWithPartition("broker-0", "LEADER"));
      reconcile(actor);
      assertEquals(1, submitted.size());
      final var bootstrap = plan(submitted.getFirst());
      assertEquals(ChangePlan.Purpose.BOOTSTRAP, bootstrap.purpose());
      assertEquals(TopologyOperationType.TRANSFER_LEADER, bootstrap.nextOperation().type());
      assertEquals("broker-0", bootstrap.nextOperation().targetBroker());
      actor.close();
    }
  }

  @Test
  void initialFivePartitionsArePlannedTogetherWithBalancedPrimary(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(5)
                  .setReplicationFactor(3));
      // The first broker materializes every partition with itself as the initial voter.
      register(state, "broker-2");
      register(state, "broker-0");
      register(state, "broker-1");
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      reconcile(actor);
      assertEquals(5, submitted.size());
      assertTrue(
          submitted.stream()
              .map(TopologyReconciliationTest::plan)
              .allMatch(plan -> plan.purpose() == ChangePlan.Purpose.BOOTSTRAP));
      final List<String> primaries =
          submitted.stream()
              .map(TopologyReconciliationTest::plan)
              .map(
                  plan ->
                      plan.operations().stream()
                          .filter(op -> op.type() == TopologyOperationType.TRANSFER_LEADER)
                          .map(TopologyOperation::targetBroker)
                          .findFirst()
                          .orElse("broker-2"))
              .toList();
      assertEquals(2, primaries.stream().filter("broker-2"::equals).count());
      assertEquals(2, primaries.stream().filter("broker-0"::equals).count());
      assertEquals(1, primaries.stream().filter("broker-1"::equals).count());
      actor.close();
    }
  }

  @Test
  void bootstrapWithExtraLearnersStopsRebalancing(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(5)
                  .setReplicationFactor(3));
      for (int i = 0; i < 5; i++) {
        register(state, "broker-" + i);
      }
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      reconcile(actor);
      assertEquals(5, submitted.size());
      for (final var command : List.copyOf(submitted)) {
        final var bootstrap = plan(command);
        assertEquals(ChangePlan.Purpose.BOOTSTRAP, bootstrap.purpose());
        new PartitionChangeSubmittedApplier(state)
            .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(bootstrap));
        for (int i = 0; i < bootstrap.operations().size(); i++) {
          var current =
              state
                  .getBrokerTopologyChangeState()
                  .changePlan("demo", bootstrap.changeId())
                  .orElseThrow();
          if (current.status() == BrokerPartitionChangeStatus.QUEUED) {
            new PartitionChangeAdmittedApplier(state)
                .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(current));
            current =
                state
                    .getBrokerTopologyChangeState()
                    .changePlan("demo", bootstrap.changeId())
                    .orElseThrow();
          }
          PartitionOperationFixture.applyOperation(state, 1, current);
        }
      }
      for (int partitionId = 1; partitionId <= 5; partitionId++) {
        final var completed =
            state
                .getBrokerPartitionTopologyState()
                .partitionTopology("demo", partitionId)
                .orElseThrow();
        assertFalse(completed.initializing());
        assertEquals(PartitionStatus.ACTIVE, completed.status());
        assertEquals(3, completed.currentVoters().size());
        assertEquals(List.of("broker-3", "broker-4"), completed.learners());
        assertFalse(
            state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", partitionId));
      }
      submitted.clear();
      for (int i = 0; i < 5; i++) {
        reconcile(actor);
      }
      assertTrue(submitted.isEmpty(), "a completed bootstrap must not create more rebalance plans");
      actor.close();
    }
  }

  @Test
  void stableOnlineTopologyDoesNotCreateAutomaticRebalance(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("scale-up"))) {
      final MutableProcessingState state = state(storage);
      registerClusterAndBrokers(state);
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1"), List.of("broker-2"), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertTrue(submitted.isEmpty());
      actor.close();
    }
  }

  @Test
  void onlineSpareReceivesPersistentLearnerJoinPlan(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("extra-learner"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(3));
      register(state, "broker-0");
      register(state, "broker-1");
      register(state, "broker-2");
      register(state, "broker-3");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertEquals(1, plan.operations().size());
      assertEquals(TopologyOperationType.JOIN, plan.operations().getFirst().type());
      assertEquals("broker-3", plan.operations().getFirst().targetBroker());
      assertEquals(ChangePlan.Purpose.MIGRATION, plan.purpose());
      actor.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void restartedRemovedBrokerGetsFifteenAuthorizedLearners(
      final boolean reopen, @TempDir final Path tempDir) {
    final var directory = tempDir.resolve("rejoin");
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(
                  4, 15, 3, List.of("broker-0", "broker-1", "broker-2", "broker-4"), 1));
      for (int i = 0; i < 5; i++) {
        new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
            .applyState(i, broker("broker-" + i).setIncarnationId("startup-" + i));
      }
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
      state.getBrokerRegistrationState().removeBroker("demo", "broker-3");
      state
          .getBrokerClusterConfigState()
          .updateVoterRoster("demo", List.of("broker-0", "broker-1", "broker-2", "broker-4"));
      BrokerTopologyPlanner.plan(
              "demo",
              state.getBrokerClusterConfigState().clusterConfig("demo"),
              BrokerTopologyPlanner.eligibleVoterRegistrations(
                  state.getBrokerRegistrationState(), "demo"))
          .forEach(state.getBrokerPartitionTopologyState()::putPartitionTopology);
      if (!reopen) {
        assertRejoinPlans(state);
      }
    }
    if (reopen) {
      try (var storage = new PartitionStorage(directory)) {
        assertRejoinPlans(state(storage));
      }
    }
  }

  private static void assertRejoinPlans(final MutableProcessingState state) {
    new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
        .applyState(20, broker("broker-3").setIncarnationId("restart-3"));
    assertEquals(
        List.of("broker-0", "broker-1", "broker-2", "broker-4"),
        state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
    final List<LoggedEvent> submitted = new ArrayList<>();
    final var actor = actor(state, submitted);
    reconcile(actor);
    assertEquals(15, submitted.size());
    for (final var event : submitted) {
      final var frozen =
          BrokerTopologyOperationRequests.freeze(state, plan(event))
              .withExecution(
                  new ChangePlan.Execution(
                      1, 1, "", "", plan(event).changeId(), PlanBlockedReason.NONE, 0));
      assertEquals(ChangePlan.Purpose.MIGRATION, frozen.purpose());
      assertEquals(
          1,
          frozen.operations().stream()
              .filter(
                  op ->
                      op.type() == TopologyOperationType.JOIN
                          && op.targetBroker().equals("broker-3"))
              .count());
      assertFalse(
          frozen.operations().stream()
              .anyMatch(
                  op ->
                      op.targetBroker().equals("broker-3")
                          && op.type() != TopologyOperationType.JOIN));
      assertEquals(PlanBlockedReason.NONE, BrokerPlanDependencies.invalidation(state, frozen, 1));
      new PartitionChangeSubmittedApplier(state)
          .applyState(21, BrokerPartitionChangeRecordMapper.fromPlan(frozen));
      final var queued =
          state.getBrokerTopologyChangeState().changePlan("demo", frozen.changeId()).orElseThrow();
      new PartitionChangeAdmittedApplier(state)
          .applyState(
              22,
              BrokerPartitionChangeRecordMapper.fromPlan(BrokerMigrationBudget.admit(queued, 1)));
      // Publish only the admitted JOIN; broker-3 is the first extra learner in this fixture.
      final var partition =
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partition(frozen.partitionId())
              .orElseThrow();
      assertTrue(partition.learners().contains("broker-3"));
      assertEquals(0, partition.priorities().get("broker-3"));
      final var admitted =
          state.getBrokerTopologyChangeState().changePlan("demo", frozen.changeId()).orElseThrow();
      new PartitionJoinedApplier(state)
          .applyState(23, BrokerPartitionChangeRecordMapper.fromPlan(admitted));
      final var beforeStaleLeave =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", frozen.partitionId())
              .orElseThrow();
      final var oldLeave =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity(
                      "demo", "old-remove-" + frozen.partitionId(), frozen.partitionId()),
                  new ChangePlan.Versions(0, 1),
                  List.of(TopologyOperation.leave(frozen.partitionId(), "broker-3"))),
              new ChangePlan.Progress(1, BrokerPartitionChangeStatus.COMPLETED),
              new ChangePlan.Deadline(1, 2));
      state.getBrokerTopologyChangeState().putChangePlan(oldLeave);
      new PartitionLeftApplier(state)
          .applyState(
              24,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  oldLeave.withStatus(BrokerPartitionChangeStatus.IN_PROGRESS)));
      assertEquals(
          beforeStaleLeave,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", frozen.partitionId())
              .orElseThrow());
      assertTrue(beforeStaleLeave.learners().contains("broker-3"));
    }
    actor.close();
  }

  @Test
  void returnedLearnerDoesNotBecomeAutomaticRecoveryVoter(@TempDir final Path tempDir) {
    try (var storage = new PartitionStorage(tempDir.resolve("recovery-voter-exclusion"))) {
      final var state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(
                  4, 1, 3, List.of("broker-0", "broker-1", "broker-2", "broker-4"), 1));
      for (int i = 0; i < 5; i++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + i,
                "127.0.0.1",
                26501 + i,
                26511 + i,
                "test",
                30_000,
                Instant.EPOCH,
                List.of(),
                "startup-" + i);
      }
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
      state.getBrokerRegistrationState().removeBroker("demo", "broker-3");
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-3",
              "127.0.0.1",
              26504,
              26514,
              "test",
              30_000,
              Instant.EPOCH,
              List.of(),
              "restart-3");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of("broker-3"), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-1", Instant.EPOCH);
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      reconcile(actor);
      assertEquals(1, submitted.size());
      final var operations = plan(submitted.getFirst()).operations();
      assertEquals(
          List.of(
              TopologyOperationType.JOIN,
              TopologyOperationType.PROMOTE_LEARNER,
              TopologyOperationType.LEAVE),
          operations.stream().map(TopologyOperation::type).toList());
      assertEquals("broker-4", operations.getFirst().targetBroker());
      assertFalse(operations.stream().anyMatch(op -> op.targetBroker().equals("broker-3")));
      actor.close();
    }
  }

  @Test
  void brokerJoinCreatesLearnerFirstPlanAndPreservesLeader(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("broker-join"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(2)
                  .setReplicationFactor(3));
      new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
          .applyState(1, brokerWithPartition("broker-0", "LEADER", 2));
      register(state, "broker-1");
      register(state, "broker-2");
      final var mature =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 2).orElseThrow();
      state.getBrokerPartitionTopologyState().putPartitionTopology(mature.withInitializing(false));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertTrue(plan.changeId().startsWith("rebalance:"));
      assertEquals(
          List.of(
              TopologyOperationType.JOIN,
              TopologyOperationType.PROMOTE_LEARNER,
              TopologyOperationType.JOIN,
              TopologyOperationType.PROMOTE_LEARNER),
          plan.operations().stream()
              .map(operation -> operation.type())
              .filter(
                  type ->
                      type == TopologyOperationType.JOIN
                          || type == TopologyOperationType.PROMOTE_LEARNER)
              .toList());
      assertFalse(
          plan.operations().stream()
              .anyMatch(operation -> operation.type() == TopologyOperationType.TRANSFER_LEADER));
      actor.close();
    }
  }

  @Test
  void offlineVoterCreatesScopedRecoveryPlan(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("scale-down"))) {
      final MutableProcessingState state = state(storage);
      // RF=3 so that losing one voter still leaves quorum (2/3); recovery can commit a live
      // membership change. (RF=2 losing a voter has no quorum and must fast-fail instead — see
      // lostQuorumDoesNotCreateAutomaticRecoveryPlan.)
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(3));
      register(state, "broker-0");
      register(state, "broker-1");
      register(state, "broker-2");
      register(state, "broker-3");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of("broker-3"), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-1"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertTrue(plan.changeId().startsWith("recover:broker-1:1:"));
      assertEquals(TopologyOperationType.JOIN, plan.operations().get(0).type());
      assertEquals(TopologyOperationType.PROMOTE_LEARNER, plan.operations().get(1).type());
      assertEquals("broker-3", plan.operations().get(0).targetBroker());
      assertEquals("broker-3", plan.operations().get(1).targetBroker());
      // E2/E3: recovery is JOIN/PROMOTE -> LEAVE only, never priority/step-down.
      assertTrue(
          plan.operations().stream()
              .noneMatch(
                  operation ->
                      operation.type() == TopologyOperationType.RECONFIGURE_PRIORITY
                          || operation.type() == TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY));
      assertTrue(
          plan.operations().stream()
              .allMatch(operation -> operation.opId().startsWith(plan.changeId() + ":")));
      actor.close();
    }
  }

  @Test
  void drainingBrokerWaitsForAdminBatch(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("draining"))) {
      final MutableProcessingState state = state(storage);
      registerClusterAndBrokers(state);
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
      new BrokerDrainedApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-1"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertTrue(submitted.isEmpty());
      actor.close();
    }
  }

  @Test
  void lostQuorumDoesNotCreateAutomaticRecoveryPlan(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("quorum-recovery"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(3));
      new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
          .applyState(1, brokerWithPartition("broker-0", "FOLLOWER"));
      register(state, "broker-1");
      register(state, "broker-2");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-1"));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-2"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      // Q3/M5: a partition that has lost quorum must fast-fail to NO_QUORUM, not auto-reset via an
      // ADD. Recovery is only possible through the fenced coordinator (disabled by default), so no
      // automatic plan is emitted here.
      assertTrue(submitted.isEmpty(), "lost-quorum partition must not auto-recover");
      actor.close();
    }
  }

  @Test
  void unselectedVoterPoolLearnerIsRemoved(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("repair-learner"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(2));
      register(state, "broker-0");
      register(state, "broker-1");
      register(state, "broker-2");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
                  new PartitionTopology.Membership(
                      List.of("broker-0", "broker-1"),
                      List.of("broker-2"),
                      Map.of("broker-0", 100, "broker-1", 10, "broker-2", 0),
                      0),
                  PartitionStatus.ACTIVE,
                  Map.of(
                      "broker-2",
                      new ReplicaState(
                          new BrokerPartitionState.Status(
                              MemberRole.LEARNER, 0, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
                          new BrokerPartitionState.Replication(0, 5, false),
                          new BrokerPartitionState.Operation(
                              "", BrokerTopologyOperationStatus.NONE),
                          1,
                          BrokerPartitionElectionState.UNKNOWN))));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertEquals(1, plan.operations().size());
      assertEquals(TopologyOperationType.LEAVE, plan.operations().getFirst().type());
      assertEquals("broker-2", plan.operations().getFirst().targetBroker());
      actor.close();
    }
  }

  @Test
  void offlineLearnerOnlyBrokerIsRemoved(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("offline-learner"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(2));
      register(state, "broker-0");
      register(state, "broker-2");
      register(state, "broker-3");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-2", "broker-0"), "broker-2"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-2", "broker-0"), List.of("broker-3"), "broker-2"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-3"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertTrue(plan.changeId().startsWith("recover:broker-3:1:"));
      assertEquals(1, plan.operations().size());
      assertEquals(TopologyOperationType.LEAVE, plan.operations().getFirst().type());
      assertEquals("broker-3", plan.operations().getFirst().targetBroker());
      actor.close();
    }
  }

  @Test
  void automaticPlanOnOnePartitionDoesNotBlockAnotherPartition(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("no-head-of-line"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(2)
                  .setReplicationFactor(2));
      register(state, "broker-0");
      register(state, "broker-1");
      register(state, "broker-2");
      register(state, "broker-3");
      // Partition 1 already has an in-flight automatic plan (simulating a slow LEAVE).
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
      final ChangePlan stuck =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "recover:broker-x:1:stuck", 1),
                  new ChangePlan.Versions(0, 1),
                  List.of(TopologyOperation.leave(1, "broker-x"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1L, Long.MAX_VALUE));
      state.getBrokerTopologyChangeState().putChangePlan(stuck);
      state.getBrokerTopologyChangeState().markPending(stuck);
      // Partition 2 has an offline learner that must still be cleaned up.
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 2),
                  new PartitionTopology.Placement(2, List.of("broker-2", "broker-0"), "broker-2"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-2", "broker-0"), List.of("broker-3"), "broker-2"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-3"));
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ReconciliationFixture actor = actor(state, submitted);

      reconcile(actor);

      assertEquals(1, submitted.size());
      final var plan = plan(submitted.getFirst());
      assertEquals(2, plan.partitionId());
      assertEquals(TopologyOperationType.LEAVE, plan.operations().getFirst().type());
      assertEquals("broker-3", plan.operations().getFirst().targetBroker());
      actor.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void targetFourMembersPromotesReturnedLearnerAcrossEightPartitions(
      final boolean gracefulExit, @TempDir final Path tempDir) {
    try (var storage = new PartitionStorage(tempDir)) {
      final var state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(
                  4, 8, 3, List.of("broker-0", "broker-1", "broker-2", "broker-3"), 1));
      for (int id = 0; id < 6; id++) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                "broker-" + id,
                "127.0.0.1",
                26501,
                26502,
                "test",
                30_000,
                Instant.EPOCH,
                List.of(),
                "old-" + id);
      }
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-4");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-5");
      for (int id = 4; id < 6; id++) {
        state.getBrokerRegistrationState().removeBroker("demo", "broker-" + id);
        new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
            .applyState(1, broker("broker-" + id).setIncarnationId("new-" + id));
      }
      for (int partition = 1; partition <= 8; partition++) {
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", partition),
                    new PartitionTopology.Placement(
                        3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                    PartitionTopology.Membership.initial(
                        List.of("broker-0", "broker-1", "broker-2"),
                        List.of("broker-4", "broker-5"),
                        "broker-0"),
                    PartitionStatus.ACTIVE,
                    Map.of()));
      }
      final List<LoggedEvent> submitted = new ArrayList<>();
      final var actor = actor(state, submitted);
      if (gracefulExit) {
        new BrokerDrainedApplier(state, () -> Instant.EPOCH).applyState(1, broker("broker-3"));
        reconcile(actor);
        assertTrue(submitted.isEmpty(), "admin drain retains ownership of its migration batch");
        state.getBrokerRegistrationState().removeBroker("demo", "broker-3");
      } else {
        new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH)
            .applyState(1, broker("broker-3"));
      }
      reconcile(actor);
      assertEquals(8, submitted.size());
      final var plans = submitted.stream().map(TopologyReconciliationTest::plan).toList();
      assertEquals(
          6,
          plans.stream()
              .filter(
                  plan ->
                      plan.operations().stream()
                          .anyMatch(
                              operation ->
                                  operation.type() == TopologyOperationType.PROMOTE_LEARNER
                                      && operation.targetBroker().equals("broker-4")))
              .count());
      assertTrue(
          plans.stream()
              .allMatch(
                  plan ->
                      plan.operations().stream()
                          .noneMatch(
                              operation ->
                                  operation.targetBroker().equals("broker-5")
                                      && (operation.type() == TopologyOperationType.LEAVE
                                          || operation.type()
                                              == TopologyOperationType.PROMOTE_LEARNER))));
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2", "broker-4"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-5"));
      actor.close();
    }
  }

  private static ReconciliationFixture actor(
      final MutableProcessingState state, final List<LoggedEvent> submitted) {
    final var scheduling = new WorkflowScheduler(new AtomicLong(System.currentTimeMillis()));
    final var actor = new ReconciliationFixture(state, scheduling, submitted);
    SCHEDULERS.put(actor, scheduling);
    return actor;
  }

  /** Drives the production Processor/Scheduler; contains no placement or recovery decisions. */
  private static final class ReconciliationFixture implements AutoCloseable {
    private final MutableProcessingState state;
    private final BrokerTopologyCoordinatorChecker scheduler;

    private ReconciliationFixture(
        final MutableProcessingState state,
        final WorkflowScheduler scheduling,
        final List<LoggedEvent> submitted) {
      this.state = state;
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduling.writers);
      final var writer = mock(StateWriter.class);
      when(scheduling.writers.state()).thenReturn(writer);
      final var appliers = PlacementDriverEventAppliers.create(state);
      doAnswer(
              invocation -> {
                appliers.applyState(
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    1);
                return null;
              })
          .when(writer)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      final var processor =
          new TopologyCoordinateProcessor(
              context, () -> true, new BrokerClusterChangePlanningService(), Duration.ofMinutes(5));
      scheduler =
          new BrokerTopologyCoordinatorChecker(
              state.getBrokerTopologyCoordinatorState(), () -> true, scheduling.clock::get);
      scheduling.processor =
          command -> {
            if (command.metadata().getIntent() == TopologyIntent.COORDINATE) {
              @SuppressWarnings("unchecked")
              final TypedRecord<BrokerTopologyCoordinationRecord> record = mock(TypedRecord.class);
              when(record.getValue())
                  .thenReturn((BrokerTopologyCoordinationRecord) command.value());
              when(record.getTimestamp()).thenReturn(scheduling.clock.get());
              when(record.getValueType()).thenReturn(ValueType.TOPOLOGY);
              processor.processRecord(record);
              scheduler.onProcessed(record);
            } else {
              submitted.add(command);
            }
          };
      scheduler.onRecovered(scheduling);
    }

    private void requestReconciliation() {
      state.getBrokerTopologyCoordinatorState().startRecoveryScan();
      notifyScheduler();
    }

    private void requestReconciliation(final String clusterId) {
      state.getBrokerTopologyCoordinatorState().markCluster(clusterId);
      notifyScheduler();
    }

    private void requestReconciliation(final String clusterId, final int partitionId) {
      state.getBrokerTopologyCoordinatorState().markPartition(clusterId, partitionId);
      notifyScheduler();
    }

    private void notifyScheduler() {
      final TypedRecord<?> record = mock(TypedRecord.class);
      when(record.getValueType()).thenReturn(ValueType.TOPOLOGY);
      scheduler.onProcessed(record);
    }

    @Override
    public void close() {
      scheduler.onClose();
    }
  }

  private static ChangePlan plan(final LoggedEvent command) {
    return BrokerPartitionChangeRecordMapper.toPlan(
        (PartitionChangeRecord) command.value(), BrokerPartitionChangeStatus.PENDING);
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void registerClusterAndBrokers(final MutableProcessingState state) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId("demo")
                .setClusterSize(2)
                .setPartitionSize(1)
                .setReplicationFactor(2));
    register(state, "broker-0");
    register(state, "broker-1");
    register(state, "broker-2");
    assertEquals(
        BrokerStatus.ONLINE,
        state.getBrokerRegistrationState().registrations("demo").getFirst().status());
  }

  private static void register(final MutableProcessingState state, final String brokerId) {
    new BrokerRegisteredApplier(state, () -> Instant.EPOCH).applyState(1, broker(brokerId));
  }

  private static BrokerRegistrationRecord broker(final String brokerId) {
    return new BrokerRegistrationRecord()
        .setClusterId("demo")
        .setBrokerId(brokerId)
        .setHost("127.0.0.1")
        .setPort(26501)
        .setRaftPort(26601)
        .setVersion("test");
  }

  private static BrokerRegistrationRecord brokerWithPartition(
      final String brokerId, final String role) {
    return brokerWithPartition(brokerId, role, 1);
  }

  private static BrokerRegistrationRecord brokerWithPartition(
      final String brokerId, final String role, final int partitionId) {
    return broker(brokerId)
        .setPartitions(
            List.of(
                new BrokerRegistrationRecord.PartitionStateRecord()
                    .setPartitionId(partitionId)
                    .setRole(MemberRole.parse(role))
                    .setTerm(1)
                    .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
                    .setMatchIndex(1)
                    .setLag(0)));
  }
}
