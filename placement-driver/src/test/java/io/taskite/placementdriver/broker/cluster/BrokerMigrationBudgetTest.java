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
package io.taskite.placementdriver.broker.cluster;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.PartitionChangeAdmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerMigrationBudgetTest {
  @Test
  void bootstrapPartitionsAdmitTogetherAfterPersistenceAndDoNotConsumeMigrationBudget(
      @TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      for (int id = 1; id <= 2; id++) {
        final var topology =
            state.getBrokerPartitionTopologyState().partitionTopology("demo", id).orElseThrow();
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(topology.withInitializing(true));
      }
      final var running = bootstrap(1, BrokerPartitionChangeStatus.PENDING);
      state.getBrokerTopologyChangeState().putChangePlan(running);
      state.getBrokerTopologyChangeState().markPending(running);
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      assertEquals(
          ChangePlan.Purpose.BOOTSTRAP,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "change-1")
              .orElseThrow()
              .purpose());
      assertTrue(
          BrokerMigrationBudget.canAdmit(
              state,
              bootstrap(2, BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
      // A distinct online partition still uses the ordinary budget.
      assertTrue(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(3, "b2", BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
      final var migration = plan(3, "b2", BrokerPartitionChangeStatus.PENDING);
      state.getBrokerTopologyChangeState().putChangePlan(migration);
      state.getBrokerTopologyChangeState().markPending(migration);
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(4, "b3", BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
      assertTrue(
          BrokerMigrationBudget.canAdmit(
              state,
              bootstrap(2, BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
    }
  }

  @Test
  void bootstrapPurposeCannotBypassBudgetForExistingMaturePartition(@TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              bootstrap(2, BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
    }
  }

  private static ChangePlan bootstrap(int partition, BrokerPartitionChangeStatus status) {
    final var plan = plan(partition, "b2", status);
    return new ChangePlan(
        new ChangePlan.Definition(
            plan.definition().identity(),
            plan.definition().versions(),
            plan.operations(),
            ChangePlan.Purpose.BOOTSTRAP),
        plan.progress(),
        plan.deadline());
  }

  @Test
  void queuedPlanSurvivesRestartWithoutChangingServingStatusOrExecutionClock(
      @TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  plan(1, "b2", BrokerPartitionChangeStatus.PENDING)));
      final var queued =
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.QUEUED, queued.status());
      assertEquals(10, queued.startedAtEpochMillis());
      assertEquals(110, queued.deadlineEpochMillis());
      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .status());
      assertEquals(
          List.of(),
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var queued =
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow();
      assertTrue(BrokerMigrationBudget.canAdmit(state, queued, BrokerMigrationBudget.DEFAULT));
      final var admitted = BrokerMigrationBudget.admit(queued, 1000);
      new PartitionChangeAdmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(admitted));
      final var persisted =
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.PENDING, persisted.status());
      assertEquals(1000, persisted.startedAtEpochMillis());
      assertEquals(1100, persisted.deadlineEpochMillis());
      new PartitionChangeAdmittedApplier(state)
          .applyState(
              3,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  BrokerMigrationBudget.admit(queued, 2000)));
      assertEquals(
          persisted,
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow());
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(2, "b3", BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
    }
  }

  @Test
  void reservationsIncludeStoppedExecutionAndRespectIncomingBrokerLimit(@TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      final var running = plan(1, "b2", BrokerPartitionChangeStatus.SUPERSEDING);
      state.getBrokerTopologyChangeState().putChangePlan(running);
      state.getBrokerTopologyChangeState().markPending(running);
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(2, "b2", BrokerPartitionChangeStatus.QUEUED),
              new BrokerMigrationBudget.Policy(3, 1, 3)));
      assertTrue(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(2, "b3", BrokerPartitionChangeStatus.QUEUED),
              new BrokerMigrationBudget.Policy(3, 1, 3)));
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(2, "b2", BrokerPartitionChangeStatus.QUEUED),
              new BrokerMigrationBudget.Policy(3, 3, 1)));
    }
  }

  @Test
  void admissionPreservesDurationAndSaturatesDeadline() {
    final var queued = plan(1, "b2", BrokerPartitionChangeStatus.QUEUED);
    assertEquals(
        Long.MAX_VALUE,
        BrokerMigrationBudget.admit(queued, Long.MAX_VALUE - 50).deadlineEpochMillis());
    assertThrows(IllegalArgumentException.class, () -> new BrokerMigrationBudget.Policy(0, 1, 1));
  }

  @Test
  void fencedRecoveryBypassesMigrationReservationsButKeepsPartitionExclusion(
      @TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      final var running = plan(1, "b1", BrokerPartitionChangeStatus.SUPERSEDING);
      state.getBrokerTopologyChangeState().putChangePlan(running);
      state.getBrokerTopologyChangeState().markPending(running);
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var recovery =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "quorum-recovery", 2),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.resetPeers(2, "b1", 7))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.QUEUED),
              new ChangePlan.Deadline(10, 110));
      assertTrue(BrokerMigrationBudget.canAdmit(state, recovery, BrokerMigrationBudget.DEFAULT));
      assertFalse(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(2, "b1", BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
      final var owner = plan(2, "b2", BrokerPartitionChangeStatus.PENDING);
      state.getBrokerTopologyChangeState().putChangePlan(owner);
      state.getBrokerTopologyChangeState().markPending(owner);
      assertFalse(BrokerMigrationBudget.canAdmit(state, recovery, BrokerMigrationBudget.DEFAULT));
    }
  }

  @Test
  void tenHealthyPartitionsReserveNineAndExistingFailureBlocksFurtherHealthyMigration(
      @TempDir Path directory) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      initialize(state);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 10, 1));
      final var reports = new ArrayList<BrokerPartitionState>();
      for (int id = 1; id <= 10; id++) {
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", id),
                    new PartitionTopology.Placement(1, List.of("b1"), "b1"),
                    PartitionTopology.Membership.initial(List.of("b1"), List.of(), "b1"),
                    PartitionStatus.ACTIVE,
                    Map.of()));
        reports.add(
            new BrokerPartitionState(
                id,
                new BrokerPartitionState.Status(
                    MemberRole.LEADER, 1, BrokerPartitionLifecycle.ACTIVE, 100, 0),
                new BrokerPartitionState.Replication(10, 0, false),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                new BrokerPartitionElectionState(
                    RaftConfigurationReference.UNKNOWN,
                    new BrokerPartitionElectionState.Priorities(100, 100),
                    new BrokerPartitionElectionState.Observation(
                        RaftNodeState.STATE_LEADER, "b1", true, 1000))));
      }
      state
          .getBrokerRegistrationState()
          .renewBroker("demo", "b1", Instant.ofEpochMilli(1000), reports);
      final var candidate = plan(1, "b2", BrokerPartitionChangeStatus.QUEUED);
      assertTrue(BrokerMigrationBudget.canAdmit(state, candidate, BrokerMigrationBudget.DEFAULT));
      reports.removeLast();
      state
          .getBrokerRegistrationState()
          .renewBroker("demo", "b1", Instant.ofEpochMilli(1000), reports);
      assertFalse(BrokerMigrationBudget.canAdmit(state, candidate, BrokerMigrationBudget.DEFAULT));
      assertTrue(
          BrokerMigrationBudget.canAdmit(
              state,
              plan(10, "b2", BrokerPartitionChangeStatus.QUEUED),
              BrokerMigrationBudget.DEFAULT));
    }
  }

  private static MutableProcessingState state(PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void initialize(MutableProcessingState state) {
    state.getBrokerClusterConfigState().putClusterConfig("demo", new BrokerClusterConfig(1, 2, 1));
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo", "b1", "127.0.0.1", 26501, 26502, "test", 30000, Instant.EPOCH, List.of());
    for (int id = 1; id <= 2; id++) {
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", id),
                  new PartitionTopology.Placement(1, List.of("b1"), "b1"),
                  PartitionTopology.Membership.initial(List.of("b1"), List.of(), "b1"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
    }
  }

  private static ChangePlan plan(int partition, String target, BrokerPartitionChangeStatus status) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", "change-" + partition, partition),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.join(partition, target))),
        new ChangePlan.Progress(0, status),
        new ChangePlan.Deadline(10, 110));
  }
}
