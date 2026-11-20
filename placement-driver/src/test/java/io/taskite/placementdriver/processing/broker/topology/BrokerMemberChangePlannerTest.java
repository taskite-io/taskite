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
package io.taskite.placementdriver.processing.broker.topology;

import static io.taskite.protocol.record.TopologyOperationType.PROMOTE_LEARNER;
import static io.taskite.protocol.record.TopologyOperationType.get;
import static org.junit.jupiter.api.Assertions.*;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerOperationConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.member.MemberChangeRejection;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.MemberChangePlannedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOutcome;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.*;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

final class BrokerMemberChangePlannerTest {
  @TempDir Path tempDir;

  @Test
  void reportsRealPendingPartitionChangeIds() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("pending-change"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      final ChangePlan pending =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "rebalance:broker-4:1:change-1", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.join(1, "broker-4"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(pending);
      state.getBrokerTopologyChangeState().markPending(pending);
      final MemberChangeRecord request =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("member-change")
              .setRequestedAtEpochMillis(1)
              .setRequestedAdditions(List.of("broker-4"));

      final MemberChangeRejection rejection =
          assertThrows(
              MemberChangeRejection.class,
              () ->
                  new BrokerMemberChangePlanner(new BrokerClusterChangePlanningService())
                      .calculate(request, state));

      assertEquals(
          "TOPOLOGY_CHANGE_IN_PROGRESS:rebalance:broker-4:1:change-1\nExpected to plan Broker member change 'member-change' in cluster 'demo', but the listed topology changes are still pending",
          rejection.getMessage());
    }
  }

  @Test
  void rejectsExplicitAdditionWhenCountDoesNotMatch() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("count-mismatch"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo", new BrokerClusterConfig(2, 1, 1, List.of("broker-0", "broker-1"), 1));
      state.getBrokerClusterState().remember("demo", 1);
      register(state, "broker-0");
      register(state, "broker-1");
      register(state, "broker-2");
      final MemberChangeRecord request =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("change-1")
              .setRequestedAtEpochMillis(1)
              .setRequestedAdditions(List.of("broker-2"))
              .setRequestedCount(2);

      final MemberChangeRejection rejection =
          assertThrows(
              MemberChangeRejection.class,
              () ->
                  new BrokerMemberChangePlanner(new BrokerClusterChangePlanningService())
                      .calculate(request, state));

      assertEquals(
          "BROKER_MEMBER_COUNT_MISMATCH: Expected member change 'change-1' in cluster 'demo' to produce requested broker count '2', but additions and removals produce '3' brokers",
          rejection.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void freezesAffectedPartitionScaleUpAndRetainsConfigurationAfterReopen(final boolean explicit) {
    final Path directory = tempDir.resolve("scale-up-" + explicit);
    final List<ChangePlan> expected = new ArrayList<>();
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = observedCluster(storage, 3);
      final var request = request(4);
      if (explicit) {
        request.setRequestedAdditions(List.of("broker-3"));
      }
      final var result = planner().calculate(request, state);
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2", "broker-3"),
          result.memberChange().getTargetVoters());
      assertEquals(5, result.partitionChanges().size());
      assertEquals(MemberChangeStatus.PENDING, result.memberChange().getStatus());
      assertTrue(
          result.partitionChanges().stream()
              .flatMap(child -> child.getOperations().stream())
              .anyMatch(operation -> get(operation.type()) == PROMOTE_LEARNER));
      for (final var child : result.partitionChanges()) {
        final var plan = assertFrozen(child);
        assertEquals("admin:member-change:change-1:" + child.getPartitionId(), plan.changeId());
        assertEquals(1000, plan.startedAtEpochMillis());
        assertEquals(301000, plan.deadlineEpochMillis());
        expected.add(plan.withStatus(BrokerPartitionChangeStatus.QUEUED));
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[child.getLength()]);
        child.write(buffer, 0);
        final var decoded = new PartitionChangeRecord();
        decoded.wrap(buffer, 0, buffer.capacity());
        assertEquals(
            plan,
            BrokerPartitionChangeRecordMapper.toPlan(decoded, BrokerPartitionChangeStatus.PENDING));
        new PartitionChangeSubmittedApplier(state).applyState(child.getPartitionId(), decoded);
      }
    }
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var recovered = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var pending = recovered.getBrokerTopologyChangeState().pendingChangePlans("demo");
      assertEquals(5, pending.size());
      for (final var plan : pending) {
        assertEquals(
            expected.stream()
                .filter(original -> original.changeId().equals(plan.changeId()))
                .findFirst()
                .orElseThrow(),
            plan);
        assertFrozen(BrokerPartitionChangeRecordMapper.fromPlan(plan));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void addingOneBrokerRetainsOtherStandbyOnAllFiveFrozenTargets(final boolean prewarmed) {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("standby-retained-" + prewarmed))) {
      final var state = observedCluster(storage, 3, prewarmed);
      final var result =
          planner().calculate(request(4).setRequestedAdditions(List.of("broker-3")), state);
      assertEquals(5, result.partitionChanges().size());
      for (final var child : result.partitionChanges()) {
        final var plan = assertFrozen(child);
        assertTrue(
            plan.operations().stream()
                .noneMatch(
                    op ->
                        op.type() == TopologyOperationType.LEAVE
                            && op.targetBroker().equals("broker-4")));
        assertTrue(
            plan.operations().getLast().configuration().targetMembers().stream()
                .anyMatch(
                    member ->
                        member.brokerId().equals("broker-4")
                            && !member.voter()
                            && member.priority() == 0));
      }
    }
  }

  @Test
  void freezesExplicitRemoval() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("remove"))) {
      final var result =
          planner()
              .calculate(
                  request(3).setRequestedRemovals(List.of("broker-3")),
                  observedCluster(storage, 4));
      assertEquals(List.of("broker-3"), result.memberChange().getRemovedMembers());
      assertFalse(result.partitionChanges().isEmpty());
      result.partitionChanges().forEach(BrokerMemberChangePlannerTest::assertFrozen);
    }
  }

  @Test
  void rejectsIncompleteSourceOnLaterPartitionWithoutChangingState() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("incomplete"))) {
      final var state = observedCluster(storage, 3);
      final var source =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 4).orElseThrow();
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  source.identity(),
                  source.placement(),
                  source.membership(),
                  source.status(),
                  Map.of()));
      final var error =
          assertThrows(MemberChangeRejection.class, () -> planner().calculate(request(4), state));
      assertEquals(RejectionType.INVALID_STATE, error.rejectionType());
      assertTrue(
          error.getMessage().contains("INCOMPLETE_SOURCE_CONFIGURATION"), error.getMessage());
      assertTrue(state.getBrokerTopologyChangeState().pendingChangePlans("demo").isEmpty());
      assertTrue(state.getMemberChangeState().active(MemberChangeScope.BROKER, "demo").isEmpty());
      assertEquals(
          3,
          state
              .getBrokerClusterConfigState()
              .findClusterConfig("demo")
              .orElseThrow()
              .clusterSize());
    }
  }

  @Test
  void freezesBootstrapWhenPartitionTopologyIsMissing() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("bootstrap"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig(
              "demo",
              new BrokerClusterConfig(
                  1, 1, 1, List.of("broker-0"), BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
      state.getBrokerClusterState().remember("demo", 1);
      register(state, "broker-0");
      register(state, "broker-1");
      final var result = planner().calculate(request(2), state);
      final var plan = assertFrozen(result.partitionChanges().getFirst());
      assertEquals(TopologyOperationType.ADD, plan.nextOperation().type());
      assertTrue(plan.nextOperation().configuration().sourceMembers().isEmpty());
      assertEquals(
          "broker-0", plan.nextOperation().configuration().targetMembers().getFirst().brokerId());
    }
  }

  @Test
  void noopCompletesWithoutPartitionEvents() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("noop"))) {
      final var result = planner().calculate(request(3), observedCluster(storage, 3, true));
      assertEquals(MemberChangeStatus.COMPLETED, result.memberChange().getStatus());
      assertEquals(MemberChangeOutcome.NO_CHANGES, result.memberChange().getOutcome());
      assertTrue(result.partitionChanges().isEmpty());
    }
  }

  @Test
  void countResizeSkipsExcludedSpareWhileExplicitReaddReadmitsIt() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("excluded-spare"))) {
      final var state = observedCluster(storage, 3);
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
      final var resized = planner().calculate(request(4), state);
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2", "broker-4"),
          resized.memberChange().getTargetVoters());
      for (final var child : resized.partitionChanges()) {
        assertTrue(
            assertFrozen(child).operations().getLast().configuration().targetMembers().stream()
                .noneMatch(member -> member.brokerId().equals("broker-3")));
      }
      final var explicit =
          planner().calculate(request(4).setRequestedAdditions(List.of("broker-3")), state);
      assertTrue(explicit.memberChange().getTargetVoters().contains("broker-3"));
      new MemberChangePlannedApplier(state).applyState(1, explicit.memberChange());
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
    }
  }

  @Test
  void countResizeReadmitsReturnedLearnerAndPersistsOnlySelectedExclusionRemoval() {
    final Path directory = tempDir.resolve("returned-count-resize");
    final List<ChangePlan> expected = new ArrayList<>();
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = observedCluster(storage, 3, true);
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-4");
      returnWithIncarnation(state, "broker-3", "new-startup");
      returnWithIncarnation(state, "broker-4", "new-standby");
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      assertFalse(
          state.getBrokerRegistrationState().isLearnerExcluded("demo", "broker-3", "new-startup"));

      final var result = planner().calculate(request(4), state);
      assertEquals(
          List.of("broker-0", "broker-1", "broker-2", "broker-3"),
          result.memberChange().getTargetVoters());
      assertEquals(4, result.memberChange().getTargetCount());
      assertEquals(5, result.partitionChanges().size());
      assertTrue(
          result.partitionChanges().stream()
              .map(BrokerMemberChangePlannerTest::assertFrozen)
              .flatMap(plan -> plan.operations().stream())
              .anyMatch(
                  op ->
                      op.type() == TopologyOperationType.PROMOTE_LEARNER
                          && op.targetBroker().equals("broker-3")));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      new MemberChangePlannedApplier(state).applyState(1, result.memberChange());
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-3"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-4"));
      for (final var child : result.partitionChanges()) {
        final var plan = assertFrozen(child);
        assertTrue(
            plan.operations().stream()
                .noneMatch(
                    op ->
                        op.type() == TopologyOperationType.LEAVE
                            && op.targetBroker().equals("broker-4")));
        assertTrue(
            plan.operations().getLast().configuration().targetMembers().stream()
                .anyMatch(
                    member ->
                        member.brokerId().equals("broker-4")
                            && !member.voter()
                            && member.priority() == 0));
        expected.add(plan.withStatus(BrokerPartitionChangeStatus.QUEUED));
        new PartitionChangeSubmittedApplier(state).applyState(child.getPartitionId(), child);
      }
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
      assertEquals(expected, state.getBrokerTopologyChangeState().pendingChangePlans("demo"));
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'':ONLINE", "removed:ONLINE", "new:OFFLINE", "new:DRAINING"},
      delimiter = ':')
  void countResizeRejectsFencedOrUnavailableReturningBroker(
      final String incarnation, final String status) {
    try (PartitionStorage storage =
        new PartitionStorage(tempDir.resolve("count-guard-" + incarnation + status))) {
      final var state = observedCluster(storage, 3, true);
      returnWithIncarnation(state, "broker-3", "removed");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-3");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-4");
      returnWithIncarnation(state, "broker-3", incarnation);
      if ("OFFLINE".equals(status)) {
        state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-3", Instant.EPOCH);
      } else if ("DRAINING".equals(status)) {
        state.getBrokerRegistrationState().markBrokerDraining("demo", "broker-3", Instant.EPOCH);
      } else {
        state.getBrokerRegistrationState().markBrokerOnline("demo", "broker-3");
      }
      final var error =
          assertThrows(MemberChangeRejection.class, () -> planner().calculate(request(4), state));
      assertEquals(
          "BROKER_COUNT_UNREACHABLE: Expected member change 'change-1' in cluster 'demo' to produce positive broker count '4', but the target roster contains '3' brokers",
          error.getMessage());
      assertEquals(RejectionType.INVALID_STATE, error.rejectionType());
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

  private static void returnWithIncarnation(
      final MutableProcessingState state, final String brokerId, final String incarnation) {
    final var previous =
        state.getBrokerRegistrationState().registrations("demo").stream()
            .filter(registration -> registration.brokerId().equals(brokerId))
            .findFirst()
            .orElseThrow();
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            brokerId,
            previous.host(),
            previous.port(),
            previous.raftPort(),
            "test",
            30_000,
            Instant.ofEpochMilli(1000),
            previous.partitions(),
            incarnation);
  }

  private static ChangePlan assertFrozen(final PartitionChangeRecord child) {
    final var plan =
        BrokerPartitionChangeRecordMapper.toPlan(child, BrokerPartitionChangeStatus.PENDING);
    assertFalse(plan.operations().isEmpty());
    BrokerOperationConfiguration previous = null;
    for (final var operation : plan.operations()) {
      assertTrue(operation.opId().startsWith(plan.changeId() + ":"));
      final var configuration = operation.configuration();
      assertNotNull(configuration, "UNFROZEN_OPERATION: " + operation.opId());
      if (previous != null) {
        assertEquals(
            previous.targetDigest(plan.clusterId()), configuration.sourceDigest(plan.clusterId()));
        assertEquals(previous.targetMembers(), configuration.sourceMembers());
        assertEquals(previous.targetRecoveryEpoch(), configuration.expectedRecoveryEpoch());
      }
      previous = configuration;
    }
    return plan;
  }

  private static BrokerMemberChangePlanner planner() {
    return new BrokerMemberChangePlanner(new BrokerClusterChangePlanningService());
  }

  private static MemberChangeRecord request(final int count) {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.BROKER)
        .setResourceId("demo")
        .setChangeId("change-1")
        .setRequestedAtEpochMillis(1000)
        .setRequestedCount(count);
  }

  private static MutableProcessingState observedCluster(
      final PartitionStorage storage, final int count) {
    return observedCluster(storage, count, false);
  }

  private static MutableProcessingState observedCluster(
      final PartitionStorage storage, final int count, final boolean prewarmed) {
    final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
    final var roster = IntStream.range(0, count).mapToObj(i -> "broker-" + i).toList();
    final var all = IntStream.range(0, 5).mapToObj(i -> "broker-" + i).toList();
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo",
            new BrokerClusterConfig(count, 5, 3, roster, BrokerClusterConfig.MANAGED_ROSTER_EPOCH));
    state.getBrokerClusterState().remember("demo", 1);
    final Map<String, List<BrokerRegistrationRecord.PartitionStateRecord>> reports =
        new LinkedHashMap<>();
    all.forEach(id -> reports.put(id, new ArrayList<>()));
    final List<PartitionTopology> topologies = new ArrayList<>();
    for (int partitionId = 1; partitionId <= 5; partitionId++) {
      final int index = partitionId - 1;
      final var voters =
          IntStream.range(0, 3).mapToObj(i -> roster.get((index + i) % count)).toList();
      final List<String> learners =
          prewarmed ? all.stream().filter(id -> !roster.contains(id)).toList() : List.of();
      final var membership =
          PartitionTopology.Membership.initial(voters, learners, voters.getFirst());
      final List<BrokerPartitionMember> members = new ArrayList<>();
      voters.forEach(
          id -> members.add(new BrokerPartitionMember(id, true, membership.priorities().get(id))));
      learners.forEach(id -> members.add(new BrokerPartitionMember(id, false, 0)));
      final String digest =
          new BrokerOperationConfiguration(members, List.of(), 12, new RecoveryFence(0, 0, ""))
              .sourceDigest("demo");
      final Map<String, ReplicaState> replicas = new LinkedHashMap<>();
      for (final var member : members) {
        final String role =
            member.brokerId().equals(voters.getFirst())
                ? "LEADER"
                : member.voter() ? "FOLLOWER" : "LEARNER";
        final var election =
            new BrokerPartitionElectionState(
                new RaftConfigurationReference(12, digest),
                new BrokerPartitionElectionState.Priorities(100, 100),
                new BrokerPartitionElectionState.Observation(
                    RaftNodeState.parse("STATE_" + (role.equals("LEARNER") ? "FOLLOWER" : role)),
                    voters.getFirst(),
                    true,
                    1000));
        final var report =
            new BrokerPartitionState(
                partitionId,
                new BrokerPartitionState.Status(
                    MemberRole.parse(role),
                    7,
                    BrokerPartitionLifecycle.ACTIVE,
                    member.priority(),
                    0),
                new BrokerPartitionState.Replication(12, 0, true),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                election);
        replicas.put(member.brokerId(), ReplicaState.observed(report, 1000));
        reports
            .get(member.brokerId())
            .add(
                new BrokerRegistrationRecord.PartitionStateRecord()
                    .setPartitionId(partitionId)
                    .setRole(MemberRole.parse(role))
                    .setTerm(7)
                    .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
                    .setPriority(member.priority())
                    .setMatchIndex(12)
                    .setReadyForPromotion(true)
                    .setLastOpStatus(BrokerTopologyOperationStatus.NONE)
                    .setElection(election));
      }
      topologies.add(
          new PartitionTopology(
              new PartitionTopology.Identity("demo", partitionId),
              new PartitionTopology.Placement(3, voters, voters.getFirst()),
              membership,
              PartitionStatus.ACTIVE,
              replicas));
    }
    for (final var id : all) {
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(
              1,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId(id)
                  .setHost("127.0.0.1")
                  .setPort(raftPort(id) - 1)
                  .setRaftPort(raftPort(id))
                  .setPartitions(reports.get(id)));
    }
    topologies.forEach(state.getBrokerPartitionTopologyState()::putPartitionTopology);
    return state;
  }

  private static int raftPort(final String id) {
    return 26502 + Integer.parseInt(id.substring("broker-".length())) * 100;
  }

  private static void register(final MutableProcessingState state, final String brokerId) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            brokerId,
            "127.0.0.1",
            raftPort(brokerId) - 1,
            raftPort(brokerId),
            "test",
            30_000,
            Instant.EPOCH,
            List.of());
  }
}
