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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.BrokerClusterConfiguredApplier;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionAcknowledgedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeAdmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeCancelledApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeStatusUpdatedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionJoinedApplier;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class PartitionChangeStateTest {
  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

  @TempDir Path tempDir;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bootstrapWithLearnersCompletesAcrossRestartAfterResourceAck(
      final boolean waitForResourceAck) {
    final var runtime = tempDir.resolve("bootstrap-learners-" + waitForResourceAck);
    final var distribution = deployment(93, "demo");
    try (var storage = new PartitionStorage(runtime)) {
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
        registerBroker(state, "broker-" + i);
      }
      final var voters = List.of("broker-0", "broker-1", "broker-2");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(3, voters, "broker-0"),
                  PartitionTopology.Membership.initial(voters, List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of(),
                  true));
      if (waitForResourceAck) {
        putDistribution(state, distribution);
        state.getCommandDistributionState().markPending(93, "demo", 1);
      }
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "bootstrap-learners", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-3"),
                      TopologyOperation.join(1, "broker-4"),
                      TopologyOperation.transferLeader(1, "broker-0")),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      for (int i = 0; i < plan.operations().size(); i++) {
        applyNext(state, plan.changeId());
      }
      if (waitForResourceAck) {
        assertTrue(
            state
                .getBrokerPartitionTopologyState()
                .partitionTopology("demo", 1)
                .orElseThrow()
                .initializing());
        assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
      }
    }
    try (var storage = new PartitionStorage(runtime)) {
      final var state = state(storage);
      if (waitForResourceAck) {
        new CommandDistributionAcknowledgedApplier(state)
            .applyState(93, distribution.setPartitionId(1));
      }
      final var completed =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertFalse(completed.initializing());
      assertEquals(PartitionStatus.ACTIVE, completed.status());
      assertEquals(List.of("broker-3", "broker-4"), completed.learners());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bootstrapCompletionFollowsExpandedRosterAcrossRestart(final boolean waitForResourceAck) {
    final var runtime = tempDir.resolve("expanded-roster-" + waitForResourceAck);
    final var distribution = deployment(92, "demo");
    try (var storage = new PartitionStorage(runtime)) {
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
      registerBroker(state, "broker-2");
      registerBroker(state, "broker-1");
      // A successor may preserve this intermediate primary while completing the third replica.
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 4),
                  new PartitionTopology.Placement(3, List.of("broker-1", "broker-2"), "broker-1"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-1", "broker-2"), List.of(), "broker-1"),
                  PartitionStatus.ACTIVE,
                  Map.of(),
                  true));
      registerBroker(state, "broker-0");
      assertEquals(
          List.of("broker-2", "broker-1", "broker-0"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      if (waitForResourceAck) {
        putDistribution(state, distribution);
        state.getCommandDistributionState().markPending(92, "demo", 4);
      }
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "intermediate-primary", 4),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(4, "broker-0"),
                      TopologyOperation.promoteLearner(4, "broker-0"),
                      TopologyOperation.reconfigurePriority(4, "broker-0", 10),
                      TopologyOperation.transferLeader(4, "broker-1")),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      for (int i = 0; i < plan.operations().size(); i++) {
        applyNext(state, plan.changeId());
      }
    }
    try (var storage = new PartitionStorage(runtime)) {
      final var state = state(storage);
      if (waitForResourceAck) {
        new CommandDistributionAcknowledgedApplier(state)
            .applyState(92, distribution.setPartitionId(4));
      }
      final var intermediate =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 4).orElseThrow();
      assertTrue(
          intermediate.initializing(), "the intermediate primary differs from the full roster");
      assertEquals(PartitionStatus.ACTIVE, intermediate.status());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 4));
      final var finalPlan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "final-primary", 4),
                  new ChangePlan.Versions(2, 3),
                  List.of(
                      TopologyOperation.reconfigurePriority(4, "broker-1", 10),
                      TopologyOperation.reconfigurePriority(4, "broker-2", 100),
                      TopologyOperation.transferLeader(4, "broker-2")),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(finalPlan));
      for (int i = 0; i < finalPlan.operations().size(); i++) {
        applyNext(state, finalPlan.changeId());
      }
      final var completed =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 4).orElseThrow();
      assertFalse(completed.initializing());
      assertEquals("broker-2", completed.desiredPrimary());
      assertEquals(100, completed.priorities().get("broker-2"));
    }
  }

  @Test
  void bootstrapKeepsLifecycleUntilDeclaredVoterRosterIsFilled() {
    try (var storage = new PartitionStorage(tempDir.resolve("partial-roster"))) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(1)
                  .setReplicationFactor(2));
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "partial-roster", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-1"),
                      TopologyOperation.promoteLearner(1, "broker-1"),
                      TopologyOperation.reconfigurePriority(1, "broker-1", 10),
                      TopologyOperation.transferLeader(1, "broker-0")),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      for (int i = 0; i < plan.operations().size(); i++) {
        applyNext(state, plan.changeId());
      }
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
    }
  }

  @Test
  void initialMembershipRemainsBootstrapUntilFinalTransferAndResourceAckAcrossRestart() {
    final var runtime = tempDir.resolve("bootstrap-lifecycle");
    final var distribution = deployment(91, "demo");
    try (var storage = new PartitionStorage(runtime)) {
      final var state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      state
          .getBrokerClusterConfigState()
          .updateVoterRoster("demo", List.of("broker-1", "broker-0"));
      putDistribution(state, distribution);
      state.getCommandDistributionState().markPending(91, "demo", 1);
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "initial-membership", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-1"),
                      TopologyOperation.promoteLearner(1, "broker-1"),
                      TopologyOperation.reconfigurePriority(1, "broker-0", 10),
                      TopologyOperation.transferLeader(1, "broker-1")),
                  ChangePlan.Purpose.BOOTSTRAP),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(10, 10000));
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      for (int i = 0; i < plan.operations().size(); i++) {
        assertTrue(
            state
                .getBrokerPartitionTopologyState()
                .partitionTopology("demo", 1)
                .orElseThrow()
                .initializing());
        applyNext(state, "initial-membership");
      }
      final var pending =
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "initial-membership")
              .orElseThrow();
      assertFalse(pending.hasMoreOperations());
      assertEquals(BrokerPartitionChangeStatus.IN_PROGRESS, pending.status());
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());
    }
    try (var storage = new PartitionStorage(runtime)) {
      final var state = state(storage);
      assertEquals(
          ChangePlan.Purpose.BOOTSTRAP,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "initial-membership")
              .orElseThrow()
              .purpose());
      assertTrue(
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .initializing());
      new CommandDistributionAcknowledgedApplier(state)
          .applyState(91, distribution.setPartitionId(1));
      final var completed =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertFalse(completed.initializing());
      assertEquals(PartitionStatus.ACTIVE, completed.status());
      assertEquals("broker-1", completed.desiredPrimary());
      assertEquals(List.of("broker-0", "broker-1"), completed.currentVoters());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
    }
  }

  @Test
  void delayedCancelPreservesSuccessorOwnershipAndTopology() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("delayed-cancel"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      registerBroker(state, "broker-2");
      final ChangePlan old = plan("old", TopologyOperation.join(1, "broker-2"));
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(old));
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(old.withStatus(BrokerPartitionChangeStatus.FAILED));
      final ChangePlan successor = plan("successor", TopologyOperation.leave(1, "broker-2"));
      state.getBrokerTopologyChangeState().putChangePlan(successor);
      state.getBrokerTopologyChangeState().markPending(successor);
      final var before = state.getBrokerPartitionTopologyState().partitionTopology("demo", 1);

      new PartitionChangeCancelledApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(old));

      assertEquals(
          List.of(successor), state.getBrokerTopologyChangeState().pendingChangePlans("demo"));
      assertEquals(before, state.getBrokerPartitionTopologyState().partitionTopology("demo", 1));
      assertEquals(
          BrokerPartitionChangeStatus.FAILED,
          state.getBrokerTopologyChangeState().changePlan("demo", "old").orElseThrow().status());
    }
  }

  @Test
  void duplicateAcknowledgementCannotApplyTheFollowingOperation() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("duplicate-ack"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      registerBroker(state, "broker-2");
      final ChangePlan submitted =
          plan(
              "duplicate",
              TopologyOperation.join(1, "broker-2"),
              TopologyOperation.promoteLearner(1, "broker-2"));
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(submitted));
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(
              submitted.withExecution(
                  new ChangePlan.Execution(
                      0,
                      0,
                      "",
                      "",
                      "duplicate",
                      PlanBlockedReason.BROKER_UNAVAILABLE,
                      T0.toEpochMilli())));
      final var event = BrokerPartitionChangeRecordMapper.fromPlan(submitted);
      final var applier = new PartitionJoinedApplier(state);
      applier.applyState(2, event);
      final var advanced =
          state.getBrokerTopologyChangeState().changePlan("demo", "duplicate").orElseThrow();
      assertEquals(PlanBlockedReason.NONE, advanced.execution().blockedReason());
      assertEquals(0, advanced.execution().blockedSinceEpochMillis());
      assertEquals("duplicate", advanced.rootChangeId());
      final var before = state.getBrokerPartitionTopologyState().partitionTopology("demo", 1);

      applier.applyState(2, event);

      assertEquals(
          1,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "duplicate")
              .orElseThrow()
              .nextOperationIndex());
      assertEquals(before, state.getBrokerPartitionTopologyState().partitionTopology("demo", 1));
      assertFalse(
          state
              .getBrokerTopologyChangeState()
              .isOperationApplied("demo", submitted.operations().get(1).opId()));
    }
  }

  @Test
  void failedPlanAcknowledgementCannotChangeMembership() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("terminal-ack"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      registerBroker(state, "broker-2");
      final ChangePlan submitted = plan("failed", TopologyOperation.join(1, "broker-2"));
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(submitted));
      new PartitionChangeCancelledApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(submitted));
      final var before = state.getBrokerPartitionTopologyState().partitionTopology("demo", 1);

      new PartitionJoinedApplier(state)
          .applyState(3, BrokerPartitionChangeRecordMapper.fromPlan(submitted));

      assertEquals(
          BrokerPartitionChangeStatus.FAILED,
          state.getBrokerTopologyChangeState().changePlan("demo", "failed").orElseThrow().status());
      assertEquals(before, state.getBrokerPartitionTopologyState().partitionTopology("demo", 1));
      assertFalse(
          state
              .getBrokerTopologyChangeState()
              .isOperationApplied("demo", submitted.nextOperation().opId()));
    }
  }

  private static ChangePlan plan(final String id, final TopologyOperation... operations) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", id, 1),
            new ChangePlan.Versions(1, 2),
            List.of(operations)),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        ChangePlan.Deadline.NONE);
  }

  @Test
  void persistsAndAdvancesJoinPromoteLeavePlanWithoutRoundRobinOverwrite() {
    final Path runtime = tempDir.resolve("runtime");
    final ChangePlan submitted =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "change-1", 1),
                new ChangePlan.Versions(10, 11),
                List.of(
                    TopologyOperation.join(1, "broker-2"),
                    TopologyOperation.promoteLearner(1, "broker-2"),
                    TopologyOperation.leave(1, "broker-1"))),
            new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
            ChangePlan.Deadline.NONE);

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      registerBroker(state, "broker-2");
      registerCluster(state);

      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(submitted));

      assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
      assertEquals(
          submitted.withStatus(BrokerPartitionChangeStatus.QUEUED),
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow());
      final var queued =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(PartitionStatus.ACTIVE, queued.status());
      assertEquals(List.of("broker-2"), queued.learners());
      admit(state, "change-1");
      final var started =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(PartitionStatus.REBALANCING, started.status());
      assertEquals(List.of("broker-0", "broker-2"), started.desiredVoters());
      assertEquals(List.of("broker-0", "broker-1"), started.currentVoters());
      assertEquals(List.of("broker-2"), started.learners());

      applyNext(state);
      assertEquals(
          1,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "change-1")
              .orElseThrow()
              .nextOperationIndex());
      assertTrue(
          state
              .getBrokerTopologyChangeState()
              .isOperationApplied("demo", submitted.operations().get(0).opId()));
      assertEquals(
          List.of("broker-2"),
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .learners());

      applyNext(state);
      final var promoted =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(List.of("broker-0", "broker-1", "broker-2"), promoted.currentVoters());
      assertFalse(promoted.learners().contains("broker-2"));

      applyNext(state);
      final ChangePlan completed =
          state.getBrokerTopologyChangeState().changePlan("demo", "change-1").orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.COMPLETED, completed.status());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));
      final var topology =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(PartitionStatus.ACTIVE, topology.status());
      assertEquals(List.of("broker-0", "broker-2"), topology.currentVoters());

      registerBroker(state, "broker-3");
      assertEquals(
          List.of("broker-0", "broker-2"),
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .currentVoters());
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState recovered = state(storage);
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          recovered
              .getBrokerTopologyChangeState()
              .changePlan("demo", "change-1")
              .orElseThrow()
              .status());
      assertEquals(
          List.of("broker-0", "broker-2"),
          recovered
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .currentVoters());
    }
  }

  @Test
  void changePlanCodecRoundTripsEveryOperation() {
    final ChangePlan plan =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "change-codec", 3),
                new ChangePlan.Versions(20, 21),
                List.of(
                    TopologyOperation.add(3, "broker-0"),
                    TopologyOperation.join(3, "broker-1"),
                    TopologyOperation.promoteLearner(3, "broker-1"),
                    TopologyOperation.leave(3, "broker-0"))),
            new ChangePlan.Progress(2, BrokerPartitionChangeStatus.IN_PROGRESS),
            ChangePlan.Deadline.NONE);

    assertEquals(plan, ChangePlanRecordCodec.fromBytes(ChangePlanRecordCodec.toBytes(plan)));
    final PartitionChangeRecord eventRecord = BrokerPartitionChangeRecordMapper.fromPlan(plan);
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[eventRecord.getLength()]);
    eventRecord.write(buffer, 0);
    final PartitionChangeRecord decodedEvent = new PartitionChangeRecord();
    decodedEvent.wrap(buffer, 0, buffer.capacity());
    assertEquals(plan, BrokerPartitionChangeRecordMapper.toPlan(decodedEvent, plan.status()));
  }

  @Test
  void addPlanExpandsPartitionCountAndCreatesTheNewPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("add-partition-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      final ChangePlan addPlan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "add-partition-2", 2),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.add(2, "broker-0"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);

      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan));

      assertEquals(2, state.getBrokerClusterConfigState().clusterConfig("demo").partitionSize());
      final var topology =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 2).orElseThrow();
      assertEquals(PartitionStatus.CREATING, topology.status());
      assertEquals(List.of(), topology.currentVoters());
      assertEquals(List.of("broker-0"), topology.desiredVoters());

      applyNext(state, "add-partition-2");
      assertEquals(
          List.of("broker-0"),
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .currentVoters());
      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void addPlanMarksOnlyLiveClusterDeploymentsPendingForNewPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("add-partition-redistribution-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(11, "demo"));
      putDistribution(state, deployment(12, "demo"));
      putDistribution(state, deletion(13, "demo", 12));
      putDistribution(state, deployment(21, "other"));

      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan("add-partition-2")));

      assertTrue(state.getCommandDistributionState().isPending(11, "demo", 2));
      assertFalse(state.getCommandDistributionState().isPending(12, "demo", 2));
      assertFalse(state.getCommandDistributionState().isPending(13, "demo", 2));
      assertFalse(state.getCommandDistributionState().isPending(21, "other", 2));
    }
  }

  @Test
  void mirrorTargetExpansionMarksOnlyTargetClusterPending() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("mirror-target-expansion"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerCluster(state, "backup");
      putDistribution(state, deployment(51, "demo"));
      state.getCommandDistributionState().putTarget(51, "backup", 1);

      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  addPlan("backup-add-partition", "backup", 2)));

      assertTrue(state.getCommandDistributionState().isPending(51, "backup", 2));
      assertFalse(state.getCommandDistributionState().isPending(51, "demo", 2));

      state.getCommandDistributionState().acknowledge(51, "backup", 2);

      assertTrue(state.getCommandDistributionState().isTargetComplete(51, "backup"));
      assertTrue(state.getCommandDistributionState().isPlanComplete(51));
    }
  }

  @Test
  void mirrorCreatedAfterDeploymentDoesNotBackfillOnTargetExpansion() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("mirror-no-history-backfill"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerCluster(state, "backup");
      putDistribution(state, deployment(52, "demo"));
      state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(1)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));

      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  addPlan("backup-no-backfill", "backup", 2)));

      assertFalse(state.getCommandDistributionState().isTarget(52, "backup"));
      assertFalse(state.getCommandDistributionState().isPending(52, "backup", 2));
    }
  }

  @Test
  void deletedMirroredResourceIsNotRedistributedToNewTargetPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("deleted-mirror-target-expansion"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerCluster(state, "backup");
      putDistribution(state, deployment(53, "demo"));
      state.getCommandDistributionState().putTarget(53, "backup", 1);
      putDistribution(state, deletion(54, "demo", 53));
      state.getCommandDistributionState().putTarget(54, "backup", 1);

      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  addPlan("backup-deleted-resource", "backup", 2)));

      assertFalse(state.getCommandDistributionState().isPending(53, "backup", 2));
    }
  }

  @Test
  void configuredExpansionMarksExistingDeploymentsPendingForNewPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("configured-expansion-redistribution-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(14, "demo"));
      final ChangePlan plan = addPlan("configured-expansion");

      new BrokerClusterConfiguredApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(2)
                  .setReplicationFactor(2)
                  .setSourcePartitionSize(1)
                  .setEncodedChangePlans(
                      List.of(
                          Base64.getEncoder()
                              .encodeToString(ChangePlanRecordCodec.toBytes(plan)))));

      assertEquals(2, state.getBrokerClusterConfigState().clusterConfig("demo").partitionSize());
      assertTrue(state.getCommandDistributionState().isPending(14, "demo", 2));
      applyNext(state, "configured-expansion");
      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void configuredExpansionMarksEveryNewPartitionPending() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("configured-multi-partition-expansion"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(15, "demo"));

      new BrokerClusterConfiguredApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(3)
                  .setReplicationFactor(2)
                  .setSourcePartitionSize(1)
                  .setEncodedChangePlans(
                      encodedPlans(
                          addPlan("configured-expansion-2", 2),
                          addPlan("configured-expansion-3", 3))));

      assertEquals(3, state.getBrokerClusterConfigState().clusterConfig("demo").partitionSize());
      assertFalse(state.getCommandDistributionState().isPending(15, "demo", 1));
      assertTrue(state.getCommandDistributionState().isPending(15, "demo", 2));
      assertTrue(state.getCommandDistributionState().isPending(15, "demo", 3));
    }
  }

  @Test
  void configuredExpansionFallsBackToPersistedPartitionSizeForLegacyEvent() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("configured-legacy-expansion"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(16, "demo"));

      new BrokerClusterConfiguredApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(2)
                  .setReplicationFactor(2)
                  .setEncodedChangePlans(encodedPlans(addPlan("configured-legacy-expansion"))));

      assertTrue(state.getCommandDistributionState().isPending(16, "demo", 2));
    }
  }

  @Test
  void configuredExpansionPendingSurvivesRestart() {
    final Path runtime = tempDir.resolve("configured-expansion-restart");
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(17, "demo"));
      new BrokerClusterConfiguredApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(2)
                  .setReplicationFactor(2)
                  .setSourcePartitionSize(1)
                  .setEncodedChangePlans(encodedPlans(addPlan("configured-expansion-restart"))));
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      assertTrue(state(storage).getCommandDistributionState().isPending(17, "demo", 2));
    }
  }

  @Test
  void jobSubscriptionDistributionInProgressSurvivesRestartAndCompletesAfterAcknowledgement() {
    final Path runtime = tempDir.resolve("resource-distribution-restart");
    final CommandDistributionRecord<JobSubscriptionRecord> distribution = deployment(20, "demo");
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, distribution);
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(addPlan("resource-distribution-restart")));

      applyNext(state, "resource-distribution-restart");

      assertEquals(
          BrokerPartitionChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "resource-distribution-restart")
              .orElseThrow()
              .status());
      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "resource-distribution-restart")
              .orElseThrow()
              .status());

      new CommandDistributionAcknowledgedApplier(state)
          .applyState(20, distribution.setPartitionId(2));

      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "resource-distribution-restart")
              .orElseThrow()
              .status());
      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void deletionFromOtherClusterDoesNotSuppressRedistribution() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("cross-cluster-deletion"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(18, "demo"));
      putDistribution(state, deletion(19, "other", 18));

      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan("cross-cluster-deletion")));

      assertTrue(state.getCommandDistributionState().isPending(18, "demo", 2));
    }
  }

  @Test
  void repeatedAddPlanDoesNotChangeRedistributionPendingState() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("add-partition-replay-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(22, "demo"));
      final ChangePlan plan = addPlan("add-partition-replay");
      final PartitionChangeRecord event = BrokerPartitionChangeRecordMapper.fromPlan(plan);

      new PartitionChangeSubmittedApplier(state).applyState(1, event);
      assertTrue(state.getCommandDistributionState().isPending(22, "demo", 2));

      new PartitionChangeSubmittedApplier(state).applyState(1, event);
      assertEquals(2, state.getBrokerClusterConfigState().clusterConfig("demo").partitionSize());
      assertTrue(state.getCommandDistributionState().isPending(22, "demo", 2));
    }
  }

  @Test
  void nonAddPlanDoesNotRedistributeAndIsNotActivationGated() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("non-add-plan-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      final var mature =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      state.getBrokerPartitionTopologyState().putPartitionTopology(mature.withInitializing(false));
      putDistribution(state, deployment(23, "demo"));
      final ChangePlan plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "non-add-plan", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.promoteLearner(1, "broker-1"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);

      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      assertEquals(1, state.getBrokerClusterConfigState().clusterConfig("demo").partitionSize());
      assertFalse(state.getCommandDistributionState().isPending(23, "demo", 2));
      state.getCommandDistributionState().markPending(23, "demo", 1);

      applyNext(state, "non-add-plan");

      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .status());
      assertTrue(state.getCommandDistributionState().isPending(23, "demo", 1));
    }
  }

  @Test
  void memberCompletionKeepsCreatingWhileDeploymentIsPending() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("member-completion-gate-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      putDistribution(state, deployment(31, "demo"));
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan("member-completion-gate")));

      applyNext(state, "member-completion-gate");

      assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      final ChangePlan distributing =
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "member-completion-gate")
              .orElseThrow();
      assertFalse(distributing.hasMoreOperations());
      assertEquals(BrokerPartitionChangeStatus.IN_PROGRESS, distributing.status());
      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
    }
  }

  @Test
  void memberCompletionActivatesAfterRedistributionCompletedFirst() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("redistribution-first-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      final CommandDistributionRecord<JobSubscriptionRecord> distribution = deployment(32, "demo");
      putDistribution(state, distribution);
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan("redistribution-first")));

      new CommandDistributionAcknowledgedApplier(state)
          .applyState(32, distribution.setPartitionId(2));

      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
      applyNext(state, "redistribution-first");
      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "redistribution-first")
              .orElseThrow()
              .status());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
    }
  }

  @Test
  void finalDistributionAcknowledgementActivatesCompletedPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("distribution-completion-gate-runtime"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      final CommandDistributionRecord<JobSubscriptionRecord> distribution = deployment(41, "demo");
      putDistribution(state, distribution);
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(clusterChangeOperation("distribution-completion-gate"));
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1,
              BrokerPartitionChangeRecordMapper.fromPlan(addPlan("distribution-completion-gate")));
      applyNext(state, "distribution-completion-gate");
      assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "distribution-completion-gate")
              .orElseThrow()
              .status());
      assertEquals(
          BrokerClusterChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "distribution-completion-gate")
              .orElseThrow()
              .status());
      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());

      new CommandDistributionAcknowledgedApplier(state)
          .applyState(41, distribution.setPartitionId(2));

      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "distribution-completion-gate")
              .orElseThrow()
              .status());
      assertEquals(
          BrokerClusterChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "distribution-completion-gate")
              .orElseThrow()
              .status());
    }
  }

  @Test
  void onlyFinalResourceAcknowledgementActivatesCompletedPartition() {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("multi-resource-completion-gate"))) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      final CommandDistributionRecord<JobSubscriptionRecord> first = deployment(42, "demo");
      final CommandDistributionRecord<JobSubscriptionRecord> second = deployment(43, "demo");
      putDistribution(state, first);
      putDistribution(state, second);
      new PartitionChangeSubmittedApplier(state)
          .applyState(
              1, BrokerPartitionChangeRecordMapper.fromPlan(addPlan("multi-resource-gate")));
      applyNext(state, "multi-resource-gate");

      new CommandDistributionAcknowledgedApplier(state).applyState(42, first.setPartitionId(2));

      assertEquals(
          PartitionStatus.CREATING,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
      assertTrue(state.getCommandDistributionState().isPending(43, "demo", 2));
      assertTrue(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "multi-resource-gate")
              .orElseThrow()
              .status());

      new CommandDistributionAcknowledgedApplier(state).applyState(43, second.setPartitionId(2));
      new CommandDistributionAcknowledgedApplier(state).applyState(43, second);

      assertEquals(
          PartitionStatus.ACTIVE,
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 2)
              .orElseThrow()
              .status());
      assertFalse(state.getCommandDistributionState().isPending(42, "demo", 2));
      assertFalse(state.getCommandDistributionState().isPending(43, "demo", 2));
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 2));
      assertEquals(
          BrokerPartitionChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "multi-resource-gate")
              .orElseThrow()
              .status());
    }
  }

  @Test
  void waitingPhaseIsPersistedAndSurvivesRestart() {
    final Path runtime = tempDir.resolve("waiting-status-runtime");
    final ChangePlan plan =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "waiting-ready", 1),
                new ChangePlan.Versions(1, 2),
                List.of(TopologyOperation.promoteLearner(1, "broker-1"))),
            new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
            ChangePlan.Deadline.NONE);
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerCluster(state);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      admit(state, "waiting-ready");
      new PartitionChangeStatusUpdatedApplier(state)
          .applyState(
              2,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  plan.withStatus(BrokerPartitionChangeStatus.WAITING_READY)));

      assertEquals(
          BrokerPartitionChangeStatus.WAITING_READY,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "waiting-ready")
              .orElseThrow()
              .status());
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      assertEquals(
          BrokerPartitionChangeStatus.WAITING_READY,
          state(storage)
              .getBrokerTopologyChangeState()
              .changePlan("demo", "waiting-ready")
              .orElseThrow()
              .status());
    }
  }

  @Test
  void cancelKeepsAppliedOperationsAndDropsRemaining() {
    // Requirement (c): cancelling a plan mid-flight must keep already-applied operations and must
    // NOT roll back to the pre-plan membership; the remaining operations are simply dropped.
    final Path runtime = tempDir.resolve("cancel-runtime");
    final ChangePlan submitted =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "change-cancel", 1),
                new ChangePlan.Versions(10, 11),
                List.of(
                    TopologyOperation.join(1, "broker-2"),
                    TopologyOperation.promoteLearner(1, "broker-2"),
                    TopologyOperation.leave(1, "broker-1"))),
            new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
            ChangePlan.Deadline.NONE);

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final MutableProcessingState state = state(storage);
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      registerBroker(state, "broker-2");
      registerCluster(state);

      new PartitionChangeSubmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(submitted));
      // Apply JOIN then PROMOTE_LEARNER: broker-2 becomes a voter. The LEAVE(broker-1) is still
      // pending.
      applyNext(state, "change-cancel");
      applyNext(state, "change-cancel");
      final var beforeCancel =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertTrue(beforeCancel.currentVoters().contains("broker-2"), "PROMOTE must have applied");
      assertTrue(beforeCancel.currentVoters().contains("broker-1"), "LEAVE not applied yet");

      new PartitionChangeCancelledApplier(state)
          .applyState(
              3,
              BrokerPartitionChangeRecordMapper.fromPlan(
                  state
                      .getBrokerTopologyChangeState()
                      .changePlan("demo", "change-cancel")
                      .orElseThrow()));

      // The plan is terminal (FAILED) and no longer blocks the partition.
      final ChangePlan cancelled =
          state.getBrokerTopologyChangeState().changePlan("demo", "change-cancel").orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.FAILED, cancelled.status());
      assertFalse(state.getBrokerTopologyChangeState().hasPendingPlanForPartition("demo", 1));

      // Applied op (broker-2 promoted) is KEPT; dropped op (LEAVE broker-1) is NOT rolled forward,
      // and the pre-plan voter broker-1 is NOT rolled back out either — membership is frozen at the
      // currently applied set and the partition returns to ACTIVE.
      final var afterCancel =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      assertEquals(PartitionStatus.ACTIVE, afterCancel.status());
      assertTrue(afterCancel.currentVoters().contains("broker-2"), "applied op must be kept");
      assertTrue(afterCancel.currentVoters().contains("broker-1"), "no rollback of prior voter");
      assertEquals(afterCancel.currentVoters(), afterCancel.desiredVoters());
    }
  }

  private static void applyNext(final MutableProcessingState state) {
    applyNext(state, "change-1");
  }

  private static void applyNext(final MutableProcessingState state, final String changeId) {
    admit(state, changeId);
    final ChangePlan plan =
        state.getBrokerTopologyChangeState().changePlan("demo", changeId).orElseThrow();
    PartitionOperationFixture.applyOperation(state, 1, plan);
    assertEquals(
        plan.nextOperationIndex() + 1,
        state
            .getBrokerTopologyChangeState()
            .changePlan("demo", changeId)
            .orElseThrow()
            .nextOperationIndex());
  }

  private static void admit(final MutableProcessingState state, final String changeId) {
    final ChangePlan plan =
        state.getBrokerTopologyChangeState().changePlan("demo", changeId).orElseThrow();
    if (plan.status() == BrokerPartitionChangeStatus.QUEUED) {
      new PartitionChangeAdmittedApplier(state)
          .applyState(1, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      assertEquals(
          BrokerPartitionChangeStatus.PENDING,
          state.getBrokerTopologyChangeState().changePlan("demo", changeId).orElseThrow().status());
    }
  }

  private static ChangePlan addPlan(final String changeId) {
    return addPlan(changeId, 2);
  }

  private static ChangePlan addPlan(final String changeId, final int partitionId) {
    return addPlan(changeId, "demo", partitionId);
  }

  private static ChangePlan addPlan(
      final String changeId, final String clusterId, final int partitionId) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(clusterId, changeId, partitionId),
            new ChangePlan.Versions(1, 2),
            List.of(TopologyOperation.add(partitionId, "broker-0"))),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        ChangePlan.Deadline.NONE);
  }

  private static ClusterChangeOperation clusterChangeOperation(final String changeId) {
    return new ClusterChangeOperation(
        "demo",
        changeId,
        1,
        2,
        2,
        2,
        List.of("broker-0"),
        List.of(),
        List.of("broker-0"),
        List.of(),
        List.of(changeId),
        BrokerClusterChangeStatus.PENDING,
        1,
        2);
  }

  private static List<String> encodedPlans(final ChangePlan... plans) {
    return Arrays.stream(plans)
        .map(ChangePlanRecordCodec::toBytes)
        .map(Base64.getEncoder()::encodeToString)
        .toList();
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> deployment(
      final long distributionKey, final String clusterId) {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(distributionKey)
        .setClusterId(clusterId)
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId(clusterId)
                .setTenantId("tenant")
                .setName("subscription-" + distributionKey)
                .setJobType("process")
                .setVariables("<xml />"));
  }

  private static CommandDistributionRecord<JobSubscriptionDeletionRecord> deletion(
      final long distributionKey, final String clusterId, final long subscriptionKey) {
    return new CommandDistributionRecord<JobSubscriptionDeletionRecord>()
        .setDistributionKey(distributionKey)
        .setClusterId(clusterId)
        .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
        .setIntent(JobSubscriptionIntent.DELETE)
        .setValue(
            new JobSubscriptionDeletionRecord()
                .setClusterId(clusterId)
                .setTenantId("tenant")
                .setSubscriptionKey(subscriptionKey));
  }

  private static void putDistribution(
      final MutableProcessingState state, final CommandDistributionRecord<?> distribution) {
    state.getCommandDistributionState().putDistribution(distribution);
    state
        .getCommandDistributionState()
        .putTarget(
            distribution.getDistributionKey(),
            distribution.getClusterId(),
            Math.max(
                1,
                state
                    .getBrokerClusterConfigState()
                    .clusterConfig(distribution.getClusterId())
                    .partitionSize()));
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void registerCluster(final MutableProcessingState state) {
    registerCluster(state, "demo");
  }

  private static void registerCluster(final MutableProcessingState state, final String clusterId) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId(clusterId)
                .setClusterSize(2)
                .setPartitionSize(1)
                .setReplicationFactor(2));
  }

  private static void registerBroker(final MutableProcessingState state, final String brokerId) {
    if (!state.getBrokerClusterState().clusterIds().contains("demo")) {
      state
          .getBrokerClusterState()
          .remember("demo", state.getBrokerClusterState().latestClusterKey() + 1);
    }
    new BrokerRegisteredApplier(state, () -> T0)
        .applyState(
            1,
            new BrokerRegistrationRecord()
                .setClusterId("demo")
                .setBrokerId(brokerId)
                .setHost("127.0.0.1")
                .setPort(26501)
                .setRaftPort(26601)
                .setVersion("test"));
  }
}
