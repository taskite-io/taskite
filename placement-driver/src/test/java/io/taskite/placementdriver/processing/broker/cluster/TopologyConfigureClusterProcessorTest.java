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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerOperationConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.BrokerClusterConfiguredApplier;
import io.taskite.placementdriver.state.appliers.PartitionJoinedApplier;
import io.taskite.placementdriver.state.appliers.PartitionPeersResetApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.PostCommitActionWriter;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedCommandWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import io.taskite.topology.RecoveryFence;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class TopologyConfigureClusterProcessorTest {
  @TempDir Path tempDir;

  @Test
  void explicitRebalanceMovesConcentratedLeadersToRosterPrimaries() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("concentrated"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var voters = List.of("broker-0", "broker-1", "broker-2");
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(3, 5, 3, voters, BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
      state.getBrokerClusterState().remember("demo", 1);
      voters.forEach(broker -> register(state, broker));
      final var membership = PartitionTopology.Membership.initial(voters, List.of(), "broker-2");
      final String digest =
          new BrokerOperationConfiguration(
                  voters.stream()
                      .map(
                          broker ->
                              new BrokerPartitionMember(
                                  broker, true, membership.priorities().get(broker)))
                      .toList(),
                  List.of(),
                  12,
                  new RecoveryFence(0, 0, ""))
              .sourceDigest("demo");
      final Map<String, List<BrokerPartitionState>> reports = new LinkedHashMap<>();
      voters.forEach(broker -> reports.put(broker, new ArrayList<>()));
      for (int id = 1; id <= 5; id++) {
        final Map<String, ReplicaState> replicas = new LinkedHashMap<>();
        for (final var broker : voters) {
          final String role = broker.equals("broker-2") ? "LEADER" : "FOLLOWER";
          final var election =
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(12, digest),
                  new BrokerPartitionElectionState.Priorities(
                      membership.priorities().get(broker), membership.priorities().get(broker)),
                  new BrokerPartitionElectionState.Observation(
                      RaftNodeState.parse("STATE_" + role), "broker-2", true, 1000));
          final var report =
              new BrokerPartitionState(
                  id,
                  new BrokerPartitionState.Status(
                      MemberRole.parse(role),
                      7,
                      BrokerPartitionLifecycle.ACTIVE,
                      membership.priorities().get(broker),
                      0),
                  new BrokerPartitionState.Replication(12, 0, true),
                  new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                  election);
          replicas.put(broker, ReplicaState.observed(report, 1000));
          reports.get(broker).add(report);
        }
        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", id),
                    new PartitionTopology.Placement(3, voters, "broker-2"),
                    membership,
                    PartitionStatus.ACTIVE,
                    replicas));
      }
      voters.forEach(
          broker ->
              state
                  .getBrokerRegistrationState()
                  .renewBroker("demo", broker, Instant.ofEpochMilli(1000), reports.get(broker)));
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(record(rebalance("balance-leaders")));
      assertFalse(writers.hasRejection(), writers.rejectionReason);
      final var event = (ClusterConfigRecord) writers.value;
      assertEquals(4, event.getEncodedChangePlans().size());
      final var targets =
          event.getEncodedChangePlans().stream()
              .map(encoded -> ChangePlanRecordCodec.fromBytes(Base64.getDecoder().decode(encoded)))
              .flatMap(plan -> plan.operations().stream())
              .filter(op -> op.type() == TopologyOperationType.TRANSFER_LEADER)
              .map(op -> op.targetBroker())
              .toList();
      assertEquals(2, targets.stream().filter("broker-0"::equals).count());
      assertEquals(2, targets.stream().filter("broker-1"::equals).count());
    }
  }

  @Test
  void quorumRecoveryCreatesDeterministicFencedPlan() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("recovery"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 41, () -> 7);
      prepareLostQuorum(state);
      final CapturingWriters writers = new CapturingWriters();
      final TopologyConfigureClusterProcessor processor =
          new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService());
      final ClusterConfigRecord command =
          new ClusterConfigRecord()
              .setClusterId("demo")
              .setAdminOperation(TopologyConfigureClusterProcessor.QUORUM_RECOVERY)
              .setChangeId("aggregate-change")
              .setRequestedAtEpochMillis(1234)
              .setRequestedPartitionId(1)
              .setConfirmation("true")
              .setFencingProof("ticket-42")
              .setSurvivorBrokerId("broker-0");

      processor.processRecord(record(command));

      assertFalse(writers.hasRejection(), writers.rejectionReason);
      assertEquals(TopologyIntent.CLUSTER_CONFIGURED, writers.intent);
      final ClusterConfigRecord event = (ClusterConfigRecord) writers.value;
      assertEquals(ClusterConfigurationOutcome.RECOVERY_PLANNED, event.getOutcome());
      assertEquals("aggregate-change", event.getChangeId());
      assertEquals(1234, event.getRequestedAtEpochMillis());
      assertEquals("broker-0", event.getSurvivorBrokerId());
      assertEquals(1, event.getEncodedChangePlans().size());
      final ChangePlan plan =
          ChangePlanRecordCodec.fromBytes(
              Base64.getDecoder().decode(event.getEncodedChangePlans().getFirst()));
      assertEquals(1, plan.partitionId());
      assertEquals(1, plan.operations().size());
      assertEquals(TopologyOperationType.RESET_PEERS, plan.operations().getFirst().type());
      assertEquals("broker-0", plan.operations().getFirst().targetBroker());
      assertEquals(42, plan.operations().getFirst().priority());
    }
  }

  @Test
  void quorumRecoveryPreemptsPendingTopologyChanges() {
    try (var storage = new PartitionStorage(tempDir.resolve("recovery-preemption"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareLostQuorum(state);
      final var previous = pendingPlan(1, "old-plan", TopologyOperation.join(1, "broker-1"));
      state.getBrokerTopologyChangeState().putChangePlan(previous);
      state.getBrokerTopologyChangeState().markPending(previous);
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(pendingAggregate("ongoing", List.of("old-plan")));
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(record(recovery()));
      assertFalse(writers.hasRejection(), writers.rejectionReason);
      final var event = (ClusterConfigRecord) writers.value;
      assertEquals(ClusterConfigurationOutcome.RECOVERY_PLANNED, event.getOutcome());
      final var plan =
          ChangePlanRecordCodec.fromBytes(
              Base64.getDecoder().decode(event.getEncodedChangePlans().getFirst()));
      assertEquals(previous.changeId(), plan.execution().predecessor());
      assertEquals(previous.execution().generation() + 1, plan.execution().generation());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "'', ticket-42, broker-0, CONFIRMATION_REQUIRED",
    "true, '', broker-0, FENCING_PROOF_REQUIRED",
    "true, ticket-42, broker-1, NO_SURVIVOR"
  })
  void recoveryPreemptionRetainsSafetyChecks(
      String confirmation, String proof, String survivor, String rejection) {
    try (var storage = new PartitionStorage(tempDir.resolve("recovery-guard-" + rejection))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareLostQuorum(state);
      final var previous =
          BrokerTopologyOperationRequests.freeze(
              state, pendingPlan(1, "old-plan", TopologyOperation.join(1, "broker-1")));
      state.getBrokerTopologyChangeState().putChangePlan(previous);
      state.getBrokerTopologyChangeState().markPending(previous);
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(
              record(
                  recovery()
                      .setConfirmation(confirmation)
                      .setFencingProof(proof)
                      .setSurvivorBrokerId(survivor)));
      assertTrue(writers.rejectionReason.startsWith(rejection + ":"));
      assertTrue(writers.rejectionReason.contains("recover quorum in cluster 'demo'"));
      if ("NO_SURVIVOR".equals(rejection)) {
        assertTrue(writers.rejectionReason.contains("partition '1'"));
        assertTrue(writers.rejectionReason.contains("survivor broker '" + survivor + "'"));
      } else {
        assertTrue(writers.rejectionReason.contains("change 'restore'"));
      }
      assertEquals(
          previous, state.getBrokerTopologyChangeState().pendingChangePlans("demo").getFirst());
    }
  }

  @Test
  void duplicateRecoveryCannotReplacePendingRecovery() {
    try (var storage = new PartitionStorage(tempDir.resolve("duplicate-recovery"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareLostQuorum(state);
      final var previous =
          pendingPlan(1, "pending-recovery", TopologyOperation.resetPeers(1, "broker-0", 7));
      state.getBrokerTopologyChangeState().putChangePlan(previous);
      state.getBrokerTopologyChangeState().markPending(previous);
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(record(recovery()));
      assertEquals(
          "QUORUM_RECOVERY_IN_PROGRESS: Expected to recover quorum in cluster 'demo' for partition '1' and change 'restore', but recovery change 'pending-recovery' is already pending",
          writers.rejectionReason);
    }
  }

  @Test
  void recoveryAtomicallyReplacesOnlyTargetOwnerAndPreservesAggregateAfterRestart() {
    final var directory = tempDir.resolve("recovery-ownership");
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareLostQuorum(state);
      final var previous =
          BrokerTopologyOperationRequests.freeze(
                  state, pendingPlan(1, "old-plan", TopologyOperation.join(1, "broker-1")))
              .withExecution(
                  new ChangePlan.Execution(
                      1, 0, "", "", "old-plan", PlanBlockedReason.LEADER_UNAVAILABLE, 700));
      final var unrelated = pendingPlan(2, "other-plan", TopologyOperation.join(2, "broker-2"));
      for (final var plan : List.of(previous, unrelated)) {
        state.getBrokerTopologyChangeState().putChangePlan(plan);
        state.getBrokerTopologyChangeState().markPending(plan);
      }
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(
              pendingAggregate("ongoing", List.of("old-plan", "other-plan")));
      final var recovery =
          pendingPlan(
              1, "admin:quorum-recovery:restore:1", TopologyOperation.resetPeers(1, "broker-0", 7));
      final var event =
          recovery()
              .setClusterSize(3)
              .setPartitionSize(1)
              .setReplicationFactor(3)
              .setVoterRoster(List.of("broker-0", "broker-1", "broker-2"))
              .setEncodedChangePlans(
                  List.of(
                      Base64.getEncoder().encodeToString(ChangePlanRecordCodec.toBytes(recovery))));
      new BrokerClusterConfiguredApplier(state).applyState(1, event);
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var previous =
          state.getBrokerTopologyChangeState().changePlan("demo", "old-plan").orElseThrow();
      final var recovery =
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "admin:quorum-recovery:restore:1")
              .orElseThrow();
      assertEquals(BrokerPartitionChangeStatus.SUPERSEDED, previous.status());
      assertEquals(PlanBlockedReason.LEADER_UNAVAILABLE, previous.execution().blockedReason());
      assertEquals(700, previous.execution().blockedSinceEpochMillis());
      assertEquals(recovery.changeId(), previous.execution().successor());
      assertEquals(previous.changeId(), recovery.execution().predecessor());
      assertEquals(previous.rootChangeId(), recovery.rootChangeId());
      assertTrue(recovery.execution().generation() > previous.execution().generation());
      assertEquals(
          recovery,
          state.getBrokerTopologyChangeState().currentDescendant("demo", "old-plan").orElseThrow());
      assertTrue(state.getBrokerTopologyChangeState().ownsPendingPlan(recovery));
      assertEquals(2, state.getBrokerTopologyChangeState().pendingChangePlans("demo").size());
      assertEquals(
          BrokerPartitionChangeStatus.IN_PROGRESS,
          state
              .getBrokerTopologyChangeState()
              .changePlan("demo", "other-plan")
              .orElseThrow()
              .status());
      assertEquals(
          "ongoing",
          state
              .getBrokerTopologyChangeState()
              .activeClusterChange("demo")
              .orElseThrow()
              .changeId());
      new PartitionJoinedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(previous));
      assertTrue(state.getBrokerTopologyChangeState().ownsPendingPlan(recovery));
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2"),
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .currentVoters());
      // Completing recovery must refresh its own history without removing the original aggregate.
      state
          .getBrokerTopologyChangeState()
          .putChangePlan(recovery.withStatus(BrokerPartitionChangeStatus.IN_PROGRESS));
      new PartitionPeersResetApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(recovery));
      assertEquals(
          BrokerClusterChangeStatus.COMPLETED,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "restore")
              .orElseThrow()
              .status());
      assertEquals(
          "ongoing",
          state
              .getBrokerTopologyChangeState()
              .activeClusterChange("demo")
              .orElseThrow()
              .changeId());
      assertEquals(
          BrokerClusterChangeStatus.WAITING_READY,
          state
              .getBrokerTopologyChangeState()
              .clusterChangeOperation("demo", "ongoing")
              .orElseThrow()
              .status());
    }
  }

  private static ClusterConfigRecord recovery() {
    return new ClusterConfigRecord()
        .setClusterId("demo")
        .setAdminOperation(TopologyConfigureClusterProcessor.QUORUM_RECOVERY)
        .setChangeId("restore")
        .setRequestedAtEpochMillis(1234)
        .setRequestedPartitionId(1)
        .setConfirmation("true")
        .setFencingProof("ticket-42")
        .setSurvivorBrokerId("broker-0");
  }

  private static ChangePlan pendingPlan(int partition, String id, TopologyOperation operation) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", id, partition),
            new ChangePlan.Versions(1, 2),
            List.of(operation)),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.IN_PROGRESS),
        new ChangePlan.Deadline(100, 200));
  }

  @Test
  void repeatedNoopRebalanceRemainsQueryableAndDoesNotBlockNextRequest() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("noop-rebalance"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareStableCluster(state);
      final CapturingWriters writers = new CapturingWriters();
      final TopologyConfigureClusterProcessor processor =
          new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService());

      processor.processRecord(record(rebalance("first-noop")));
      assertFalse(writers.hasRejection(), writers.rejectionReason);
      final ClusterConfigRecord first = (ClusterConfigRecord) writers.value;
      assertEquals(ClusterConfigurationOutcome.NO_CHANGES, first.getOutcome());
      assertEquals(List.of(), first.getEncodedChangePlans());
      new BrokerClusterConfiguredApplier(state).applyState(1, first);

      final BrokerClusterChangeStatusRecord result =
          (BrokerClusterChangeStatusRecord)
              new BrokerClusterChangeQueryBehavior(state)
                  .createResponseValue(
                      new TopologyQueryRecord().setClusterId("demo").setChangeId("first-noop"));
      assertEquals(ChangeStatus.COMPLETED, result.getStatus());
      assertEquals(List.of(), result.getPartitionChanges());

      processor.processRecord(record(rebalance("second-noop")));
      assertFalse(writers.hasRejection(), writers.rejectionReason);
      assertEquals(TopologyIntent.CLUSTER_CONFIGURED, writers.intent);
      assertEquals("second-noop", ((ClusterConfigRecord) writers.value).getChangeId());
      assertEquals(
          ClusterConfigurationOutcome.NO_CHANGES,
          ((ClusterConfigRecord) writers.value).getOutcome());
    }
  }

  @Test
  void rebalanceAcceptsLegacyPendingEmptyManifestButRejectsMissingChildPlan() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("legacy-rebalance"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      prepareStableCluster(state);
      final CapturingWriters writers = new CapturingWriters();
      final TopologyConfigureClusterProcessor processor =
          new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService());
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(pendingAggregate("legacy-noop", List.of()));

      processor.processRecord(record(rebalance("next-change")));
      assertFalse(writers.hasRejection(), writers.rejectionReason);
      assertEquals("next-change", ((ClusterConfigRecord) writers.value).getChangeId());

      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(
              pendingAggregate("real-change", List.of("admin:rebalance:real-change:1")));
      processor.processRecord(record(rebalance("blocked-change")));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);
      assertEquals(
          "TOPOLOGY_CHANGE_IN_PROGRESS:real-change\nExpected to apply admin operation 'REBALANCE' for change 'blocked-change' in cluster 'demo', but the listed topology changes are still pending",
          writers.rejectionReason);
    }
  }

  @Test
  void countPatchReadmitsReturnedBrokerAndPersistsPlansWithoutClearingOtherExclusions() {
    final Path directory = tempDir.resolve("returned-count-patch");
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = countResizeState(storage, "new-startup", "ONLINE");
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(record(countPatch()));

      assertFalse(writers.hasRejection(), writers.rejectionReason);
      final var event = (ClusterConfigRecord) writers.value;
      assertEquals(TopologyIntent.CLUSTER_CONFIGURED, writers.intent);
      assertEquals(4, event.getClusterSize());
      assertEquals(List.of("broker-0", "broker-1", "broker-2", "broker-3"), event.getVoterRoster());
      assertEquals(5, event.getEncodedChangePlans().size());
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      event.getEncodedChangePlans().stream()
          .map(encoded -> ChangePlanRecordCodec.fromBytes(Base64.getDecoder().decode(encoded)))
          .flatMap(plan -> plan.operations().stream())
          .forEach(op -> assertTrue(op.configuration() != null, "Operation must be frozen"));
      new BrokerClusterConfiguredApplier(state).applyState(1, event);
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-4"));
    }
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2", "broker-3"),
          state
              .getBrokerClusterConfigState()
              .findClusterConfig("demo")
              .orElseThrow()
              .voterRoster());
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-4"));
      assertEquals(5, state.getBrokerTopologyChangeState().pendingChangePlans("demo").size());
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'':ONLINE", "removed:ONLINE", "new:OFFLINE", "new:DRAINING"},
      delimiter = ':')
  void countPatchRejectsFencedOrUnavailableReturningBroker(
      final String incarnation, final String status) {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("count-patch-guard-" + incarnation + status))) {
      final var state = countResizeState(storage, incarnation, status);
      final var writers = new CapturingWriters();
      new TopologyConfigureClusterProcessor(
              new TestContext(state, writers), new BrokerClusterChangePlanningService())
          .processRecord(record(countPatch()));
      assertEquals(RejectionType.INVALID_STATE, writers.rejectionType);
      assertEquals(
          "BROKER_COUNT_UNREACHABLE: Expected to configure cluster 'demo' for change 'count-resize' with broker count '4', but the target roster contains '3' brokers",
          writers.rejectionReason);
      assertEquals(
          3,
          state
              .getBrokerClusterConfigState()
              .findClusterConfig("demo")
              .orElseThrow()
              .clusterSize());
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
    }
  }

  private static MutableProcessingState countResizeState(
      final PartitionStorage storage, final String incarnation, final String status) {
    final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo",
            new BrokerClusterConfig(
                3,
                5,
                3,
                List.of("broker-0", "broker-1", "broker-2"),
                BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
    state.getBrokerClusterState().remember("demo", 1);
    for (int i = 0; i < 5; i++) {
      register(state, "broker-" + i);
    }
    state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-4");
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            "broker-3",
            "127.0.0.1",
            26501,
            26502,
            "test",
            30_000,
            Instant.EPOCH,
            List.of(),
            "removed");
    state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            "broker-3",
            "127.0.0.1",
            26501,
            26502,
            "test",
            30_000,
            Instant.EPOCH,
            List.of(),
            incarnation);
    if ("OFFLINE".equals(status)) {
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-3", Instant.EPOCH);
    } else if ("DRAINING".equals(status)) {
      state.getBrokerRegistrationState().markBrokerDraining("demo", "broker-3", Instant.EPOCH);
    } else {
      state.getBrokerRegistrationState().markBrokerOnline("demo", "broker-3");
    }
    return state;
  }

  private static ClusterConfigRecord countPatch() {
    return new ClusterConfigRecord()
        .setClusterId("demo")
        .setAdminOperation(TopologyConfigureClusterProcessor.CONFIGURATION_PATCH)
        .setChangeId("count-resize")
        .setRequestedAtEpochMillis(1000)
        .setRequestedBrokerCount(4);
  }

  private static ClusterConfigRecord rebalance(final String changeId) {
    return new ClusterConfigRecord()
        .setClusterId("demo")
        .setAdminOperation(TopologyConfigureClusterProcessor.REBALANCE)
        .setChangeId(changeId)
        .setRequestedAtEpochMillis(1234);
  }

  private static ClusterChangeOperation pendingAggregate(
      final String changeId, final List<String> planIds) {
    return new ClusterChangeOperation(
        "demo",
        changeId,
        1,
        1,
        1,
        1,
        List.of("broker-0"),
        List.of(),
        List.of("broker-0"),
        List.of(),
        planIds,
        BrokerClusterChangeStatus.PENDING,
        100,
        200);
  }

  private static void prepareStableCluster(final MutableProcessingState state) {
    state.getBrokerClusterState().remember("demo", 1);
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo",
            new BrokerClusterConfig(
                1, 1, 1, List.of("broker-0"), BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
    register(state, "broker-0");
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            new PartitionTopology(
                new PartitionTopology.Identity("demo", 1),
                new PartitionTopology.Placement(1, List.of("broker-0"), "broker-0"),
                PartitionTopology.Membership.initial(List.of("broker-0"), List.of(), "broker-0"),
                PartitionStatus.ACTIVE,
                Map.of()));
  }

  private static void prepareLostQuorum(final MutableProcessingState state) {
    state.getBrokerClusterState().remember("demo", 1);
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo",
            new BrokerClusterConfig(3, 1, 3, List.of("broker-0", "broker-1", "broker-2"), 1));
    state
        .getBrokerPartitionTopologyState()
        .putPartitionTopology(
            new PartitionTopology(
                new PartitionTopology.Identity("demo", 1),
                new PartitionTopology.Placement(
                    3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                new PartitionTopology.Membership(
                    List.of("broker-0", "broker-1", "broker-2"), List.of(), Map.of(), 0),
                PartitionStatus.ACTIVE,
                Map.of()));
    register(state, "broker-0");
    register(state, "broker-1");
    register(state, "broker-2");
    state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-1", Instant.EPOCH);
    state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-2", Instant.EPOCH);
  }

  private static void register(final MutableProcessingState state, final String brokerId) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo", brokerId, "127.0.0.1", 26501, 26502, "test", 30_000, Instant.EPOCH, List.of());
  }

  private static TypedRecord<ClusterConfigRecord> record(final ClusterConfigRecord value) {
    return new TypedRecord<>() {
      @Override
      public long getKey() {
        return 1;
      }

      @Override
      public ClusterConfigRecord getValue() {
        return value;
      }

      @Override
      public String getRequestStreamId() {
        return "stream";
      }

      @Override
      public String getRequestId() {
        return "request";
      }

      @Override
      public int getLength() {
        return value.getLength();
      }
    };
  }

  private record TestContext(MutableProcessingState state, CapturingWriters writers)
      implements TypedRecordProcessorContext {
    @Override
    public int getPartitionId() {
      return 1;
    }

    @Override
    public KeyGenerator getKeyGenerator() {
      return () -> 1;
    }

    @Override
    public Writers getWriters() {
      return writers;
    }

    @Override
    public Object getProcessingState() {
      return state;
    }

    @Override
    public EventAppliers getEventAppliers() {
      return null;
    }
  }

  private static final class CapturingWriters implements Writers {
    private Intent intent;
    private UnifiedRecordValue value;
    private RejectionType rejectionType;
    private String rejectionReason;

    boolean hasRejection() {
      return rejectionType != null;
    }

    @Override
    public TypedCommandWriter command() {
      return (key, commandIntent, commandValue) -> {};
    }

    @Override
    public StateWriter state() {
      return new StateWriter() {
        @Override
        public void appendFollowUpEvent(
            final long key, final Intent eventIntent, final UnifiedRecordValue eventValue) {
          appendFollowUpEvent(key, eventIntent, eventValue, 1);
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final int recordVersion) {
          intent = eventIntent;
          value = eventValue;
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue) {
          appendFollowUpEvent(key, valueType, eventIntent, eventValue, 1);
        }

        @Override
        public void appendFollowUpEvent(
            final long key,
            final ValueType valueType,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final int recordVersion) {
          intent = eventIntent;
          value = eventValue;
        }
      };
    }

    @Override
    public TypedRejectionWriter rejection() {
      return (type, reason) -> {
        rejectionType = type;
        rejectionReason = reason;
      };
    }

    @Override
    public TypedResponseWriter response() {
      return new TypedResponseWriter() {
        @Override
        public void writeEventOnCommand(
            final long key,
            final Intent eventIntent,
            final UnifiedRecordValue eventValue,
            final TypedRecord<?> sourceCommand) {}

        @Override
        public void writeRejectionOnCommand(
            final TypedRecord<?> sourceCommand, final RejectionType type, final String reason) {}
      };
    }

    @Override
    public PostCommitActionWriter postCommit() {
      return action -> {};
    }
  }
}
