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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeDispatchResult;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeTransport;
import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerOperationSafety;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.request.BrokerApplyTopologyOperationRequest;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.broker.topology.ReplicaState;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.topology.PartitionExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.BrokerMarkedOfflineApplier;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.BrokerRenewedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeAdmittedApplier;
import io.taskite.placementdriver.state.appliers.PartitionChangeSubmittedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationDigest;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerOperationExecutionTest {
  private static ExecutionFixture actor(
      final MutableProcessingState state,
      final BrokerClusterChangeTransport transport,
      final Consumer<LoggedEvent> commands) {
    return new ExecutionFixture(state, transport, commands);
  }

  private static void dispatch(final ExecutionFixture fixture) {
    fixture.scheduler.onProcessed(mock(TypedRecord.class));
    fixture.scheduling.clock.addAndGet(100);
    fixture.scheduling.runReady();
  }

  private static final class ExecutionFixture {
    final WorkflowScheduler scheduling =
        new WorkflowScheduler(new AtomicLong(System.currentTimeMillis()));
    final PartitionExecuteProcessor processor;
    final TopologyMembershipChangeExecutor executor;
    final TopologyMembershipChangeChecker scheduler;
    long position = System.currentTimeMillis();

    ExecutionFixture(
        final MutableProcessingState state,
        final BrokerClusterChangeTransport transport,
        final Consumer<LoggedEvent> commands) {
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduling.writers);
      final var stateWriter = mock(StateWriter.class);
      when(scheduling.writers.state()).thenReturn(stateWriter);
      doAnswer(
              call -> {
                PlacementDriverEventAppliers.create(state)
                    .applyState(call.getArgument(0), call.getArgument(1), call.getArgument(2), 1);
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      executor =
          new TopologyMembershipChangeExecutor(
              context,
              () -> true,
              () -> transport,
              null,
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      processor = new PartitionExecuteProcessor(context, executor);
      scheduler = new TopologyMembershipChangeChecker(state, () -> true, scheduling.clock::get);
      // Supply current registration evidence, as a real Broker heartbeat would.
      final var topology = state.getBrokerPartitionTopologyState().partitionTopology("demo", 1);
      if (topology.isPresent() && !topology.get().currentVoters().isEmpty()) {
        final var report =
            new BrokerPartitionState(
                1,
                new BrokerPartitionState.Status(
                    MemberRole.LEADER,
                    1,
                    BrokerPartitionLifecycle.ACTIVE,
                    100,
                    topology.get().recoveryEpoch()),
                new BrokerPartitionState.Replication(0, 0, false),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                new BrokerPartitionElectionState(
                    RaftConfigurationReference.UNKNOWN,
                    new BrokerPartitionElectionState.Priorities(100, 100),
                    new BrokerPartitionElectionState.Observation(
                        RaftNodeState.STATE_LEADER, "broker-0", true, scheduling.clock.get())));
        state
            .getBrokerRegistrationState()
            .renewBroker(
                "demo", "broker-0", Instant.ofEpochMilli(scheduling.clock.get()), List.of(report));
      }
      scheduling.processor =
          event -> {
            final var intent = event.metadata().getIntent();
            if (intent == BrokerPartitionChangeIntent.EXECUTE) {
              processor.processRecord(record((PartitionChangeRecord) event.value()));
            } else if (intent == BrokerPartitionChangeIntent.EXECUTION_RESULT) {
              processor.results().processRecord(record((PartitionExecutionRecord) event.value()));
            } else commands.accept(event);
          };
      executor.onRecovered(scheduling);
      scheduler.onRecovered(scheduling);
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getPosition()).thenReturn(++position);
      when(record.getTimestamp()).thenReturn(scheduling.clock.get());
      return record;
    }

    void close() {
      executor.onClose();
      scheduler.onClose();
    }
  }

  @Test
  void queuedPlansDrainWithOneCommandBudgetAndStopWhenPendingIsEmpty(@TempDir final Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(2)
                  .setReplicationFactor(1));
      registerBroker(state, "broker-0");
      for (int partition = 1; partition <= 2; partition++) {
        final var plan =
            new ChangePlan(
                new ChangePlan.Definition(
                    new ChangePlan.Identity("demo", "change-" + partition, partition),
                    new ChangePlan.Versions(1, 2),
                    List.of(TopologyOperation.add(partition, "broker-0"))),
                new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
                new ChangePlan.Deadline(1, Long.MAX_VALUE));
        new PartitionChangeSubmittedApplier(state)
            .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      }
      final var commands = new ArrayList<LoggedEvent>();
      final var actor = actor(state, new NoopTransport(), commands::add);
      final var scheduling = actor.scheduling;
      scheduling.capacity = 1;
      dispatch(actor);
      assertEquals(1, commands.size());
      dispatch(actor);
      assertEquals(2, commands.size());
      assertTrue(
          commands.stream()
              .allMatch(
                  command -> command.metadata().getIntent() == BrokerPartitionChangeIntent.ADMIT));
      state.getBrokerTopologyChangeState().removePending("demo", 1);
      state.getBrokerTopologyChangeState().removePending("demo", 2);
      dispatch(actor);
      dispatch(actor);
      assertTrue(scheduling.tasks.isEmpty());
      actor.close();
    }
  }

  @Test
  void emptyLearnerIsReadyWhenItMatchesTheVoterHighWatermark() {
    final PartitionTopology topology = topologyWithReplicaIndexes(0, 0, 0);

    assertTrue(BrokerOperationSafety.replicaReadyForPromotion(topology, "broker-1"));
  }

  @Test
  void laggingLearnerIsNotReadyForPromotion() {
    final PartitionTopology topology = topologyWithReplicaIndexes(5, 4, 0);

    assertFalse(BrokerOperationSafety.replicaReadyForPromotion(topology, "broker-1"));
  }

  @Test
  void locallyIdleLearnerIsNotReadyWithoutLeaderConfirmation() {
    final PartitionTopology topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
            new PartitionTopology.Membership(
                List.of("broker-0"),
                List.of("broker-1"),
                Map.of("broker-0", 100, "broker-1", 0),
                0),
            PartitionStatus.REBALANCING,
            Map.of(
                "broker-0",
                new ReplicaState(
                    new BrokerPartitionState.Status(
                        MemberRole.LEADER, 0, BrokerPartitionLifecycle.ACTIVE, 100, 0),
                    new BrokerPartitionState.Replication(0, 0, false),
                    new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                    1,
                    BrokerPartitionElectionState.UNKNOWN),
                "broker-1",
                new ReplicaState(
                    new BrokerPartitionState.Status(
                        MemberRole.LEARNER, 0, BrokerPartitionLifecycle.ACTIVE, 0, 0),
                    new BrokerPartitionState.Replication(0, 0, false),
                    new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                    1,
                    BrokerPartitionElectionState.UNKNOWN)));

    assertFalse(BrokerOperationSafety.replicaReadyForPromotion(topology, "broker-1"));
  }

  @Test
  void leaveRequiresCurrentAndTargetQuorum() {
    final PartitionTopology topology = leaveTopology();

    assertFalse(
        BrokerOperationSafety.leavePreservesQuorum(topology, "broker-2", Set.of("broker-0")));
    assertTrue(
        BrokerOperationSafety.leavePreservesQuorum(
            topology, "broker-2", Set.of("broker-0", "broker-1")));
  }

  @Test
  void queuedPlanRequestsAdmissionWithoutBrokerRpcEvenAfterOriginalDeadline(@TempDir Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(1)
                  .setReplicationFactor(1));
      registerBroker(state, "broker-0");
      final var plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "queued", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.join(1, "broker-1"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, 2));
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(plan));
      final var transport = new CapturingAppliedTransport();
      final List<LoggedEvent> commands = new ArrayList<>();
      final var actor = actor(state, transport, commands::add);
      dispatch(actor);
      assertEquals(
          List.of("ADMIT"), commands.stream().map(c -> c.metadata().getIntent().name()).toList());
      assertEquals(null, transport.request);
      actor.close();
    }
  }

  @Test
  void persistsWaitingReadyPhaseBeforePromotion(@TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(1)
                  .setReplicationFactor(1));
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      final ChangePlan plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "promote-broker-1", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.promoteLearner(1, "broker-1"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(authorizedPlan(state, plan)));
      admit(state, plan.changeId());

      final List<LoggedEvent> submitted = new ArrayList<>();
      final ExecutionFixture actor = actor(state, new NoopTransport(), submitted::add);

      dispatch(actor);

      assertEquals(1, submitted.size());
      assertEquals("UPDATE_STATUS", submitted.get(0).metadata().getIntent().name());
    }
  }

  @Test
  void joinTransportSuccessRequiresAppliedConfigurationBeforeAcknowledgement(
      @TempDir final Path tempDir) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("join"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(1)
                  .setPartitionSize(1)
                  .setReplicationFactor(1));
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      final ChangePlan plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "join-broker-1", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(TopologyOperation.join(1, "broker-1"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(authorizedPlan(state, plan)));
      admit(state, plan.changeId());

      final List<LoggedEvent> submitted = new ArrayList<>();
      final var transport = new AppliedTransport();
      final ExecutionFixture actor = actor(state, transport, submitted::add);

      dispatch(actor);

      assertTrue(submitted.isEmpty());
      dispatch(actor);
      assertEquals(1, transport.calls);
      actor.scheduling.clock.addAndGet(15_000);
      dispatch(actor);
      assertEquals(2, transport.calls, "only the durable RPC deadline permits another attempt");
      transport.pending.complete(
          new BrokerClusterChangeDispatchResult(transport.request.opId(), true));
      final var pending =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      final String digest =
          RaftConfigurationDigest.ofBroker(
              "demo",
              List.of(
                  new BrokerPartitionMember("broker-0", true, 100),
                  new BrokerPartitionMember("broker-1", false, 0)));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity(pending.clusterId(), pending.partitionId()),
                  new PartitionTopology.Placement(
                      pending.replicationFactor(),
                      pending.desiredVoters(),
                      pending.desiredPrimary()),
                  new PartitionTopology.Membership(
                      pending.currentVoters(),
                      pending.learners(),
                      pending.priorities(),
                      pending.recoveryEpoch()),
                  pending.status(),
                  Map.of(
                      "broker-0",
                      new ReplicaState(
                          new BrokerPartitionState.Status(
                              MemberRole.LEADER, 0, BrokerPartitionLifecycle.ACTIVE, 100, 0),
                          new BrokerPartitionState.Replication(5, 0, false),
                          new BrokerPartitionState.Operation(
                              plan.nextOperation().opId(), BrokerTopologyOperationStatus.APPLIED),
                          1,
                          new BrokerPartitionElectionState(
                              new RaftConfigurationReference(5, digest),
                              new BrokerPartitionElectionState.Priorities(100, 100),
                              new BrokerPartitionElectionState.Observation(
                                  RaftNodeState.STATE_LEADER, "broker-0", true, 1))))));
      renewLeaderReport(state);
      actor.scheduling.runReady();
      actor.scheduling.clock.addAndGet(1000);
      dispatch(actor);
      assertEquals(1, submitted.size());
      assertEquals("JOIN", submitted.getFirst().metadata().getIntent().name());
    }
  }

  @Test
  void recoveryJoinRetainsOfflineVoterIdentityAndItsCurrentRegistration(@TempDir final Path tempDir)
      throws Exception {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("offline-voter"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
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
      registerBroker(state, "broker-3", 26_504, 30_633);
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(2, List.of("broker-0", "broker-3"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-3"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH)
          .applyState(
              2, new BrokerRegistrationRecord().setClusterId("demo").setBrokerId("broker-3"));
      final ChangePlan plan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "recover:broker-3:1:test", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(
                      TopologyOperation.join(1, "broker-1"),
                      TopologyOperation.promoteLearner(1, "broker-1"),
                      TopologyOperation.leave(1, "broker-3"))),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      new PartitionChangeSubmittedApplier(state)
          .applyState(3, BrokerPartitionChangeRecordMapper.fromPlan(authorizedPlan(state, plan)));
      admit(state, plan.changeId());
      final CapturingAppliedTransport transport = new CapturingAppliedTransport();
      final List<LoggedEvent> submitted = new ArrayList<>();
      final ExecutionFixture actor = actor(state, transport, submitted::add);

      dispatch(actor);

      assertNotNull(transport.request);
      assertEquals(
          new BrokerPartitionMember("broker-3", true, 10),
          transport.request.members().stream()
              .filter(member -> "broker-3".equals(member.brokerId()))
              .findFirst()
              .orElseThrow());
      assertEquals(
          30_633,
          state
              .getBrokerRegistrationState()
              .registration(new BrokerRegistryKey("demo", "broker-3"))
              .orElseThrow()
              .raftPort());
      assertTrue(submitted.isEmpty(), "RPC success alone must not acknowledge JOIN");
      actor.close();
    }
  }

  private static void registerBroker(final MutableProcessingState state, final String brokerId) {
    registerBroker(
        state,
        brokerId,
        "broker-0".equals(brokerId) ? 26_501 : 26_502,
        "broker-0".equals(brokerId) ? 26_601 : 26_602);
  }

  @Test
  void frozenRequestSurvivesEndpointChangesRestartAndLostAcknowledgement(
      @TempDir final Path directory) throws Exception {
    final Path runtime = directory.resolve("frozen");
    final CapturingAppliedTransport first = new CapturingAppliedTransport();
    final ChangePlan plan =
        new ChangePlan(
            new ChangePlan.Definition(
                new ChangePlan.Identity("demo", "frozen-join", 1),
                new ChangePlan.Versions(1, 2),
                List.of(
                    TopologyOperation.join(1, "broker-1"),
                    TopologyOperation.promoteLearner(1, "broker-1"))),
            new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
            new ChangePlan.Deadline(1, Long.MAX_VALUE));
    ChangePlan frozen;
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(2)
                  .setPartitionSize(1)
                  .setReplicationFactor(2));
      registerBroker(state, "broker-0");
      registerBroker(state, "broker-1");
      frozen = authorizedPlan(state, plan);
      assertEquals(
          frozen.operations().get(0).configuration().targetMembers(),
          frozen.operations().get(1).configuration().sourceMembers());
      new PartitionChangeSubmittedApplier(state)
          .applyState(2, BrokerPartitionChangeRecordMapper.fromPlan(frozen));
      admit(state, frozen.changeId());
      frozen =
          state.getBrokerTopologyChangeState().changePlan("demo", frozen.changeId()).orElseThrow();
      final var actor = actor(state, first, command -> {});
      dispatch(actor);
      assertNotNull(first.request);
      actor.close();
    }
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertEquals(
          frozen,
          state.getBrokerTopologyChangeState().changePlan("demo", "frozen-join").orElseThrow());
      registerBroker(state, "broker-1", 28001, 28002);
      final var retried = new CapturingAppliedTransport();
      final List<LoggedEvent> acknowledgements = new ArrayList<>();
      final var actor = actor(state, retried, acknowledgements::add);
      actor.scheduling.clock.addAndGet(16_000);
      dispatch(actor);
      assertArrayEquals(first.request.serializeRequest(), retried.request.serializeRequest());
      assertTrue(acknowledgements.isEmpty());
      final var pending =
          state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      final var report =
          new ReplicaState(
              new BrokerPartitionState.Status(
                  MemberRole.LEADER, 0, BrokerPartitionLifecycle.ACTIVE, 100, 0),
              new BrokerPartitionState.Replication(7, 0, false),
              new BrokerPartitionState.Operation(
                  frozen.nextOperation().opId(), BrokerTopologyOperationStatus.APPLIED),
              1,
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(7, first.request.targetConfigurationDigest()),
                  new BrokerPartitionElectionState.Priorities(100, 100),
                  new BrokerPartitionElectionState.Observation(
                      RaftNodeState.STATE_LEADER, "broker-0", true, System.currentTimeMillis())));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity(pending.clusterId(), pending.partitionId()),
                  new PartitionTopology.Placement(
                      pending.replicationFactor(),
                      pending.desiredVoters(),
                      pending.desiredPrimary()),
                  new PartitionTopology.Membership(
                      pending.currentVoters(),
                      pending.learners(),
                      pending.priorities(),
                      pending.recoveryEpoch()),
                  pending.status(),
                  Map.of("broker-0", report)));
      renewLeaderReport(state);
      actor.scheduling.clock.addAndGet(1000);
      dispatch(actor);
      dispatch(actor);
      assertEquals(1, acknowledgements.size(), "queue admission must wait for durable progress");
      assertTrue(
          acknowledgements.stream()
              .allMatch(command -> "JOIN".equals(command.metadata().getIntent().name())));
      actor.close();
    }
  }

  private static void registerBroker(
      final MutableProcessingState state,
      final String brokerId,
      final int port,
      final int raftPort) {
    new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
        .applyState(
            1,
            new BrokerRegistrationRecord()
                .setClusterId("demo")
                .setBrokerId(brokerId)
                .setHost("127.0.0.1")
                .setPort(port)
                .setRaftPort(raftPort)
                .setVersion("test"));
  }

  private static ChangePlan authorizedPlan(
      final MutableProcessingState state, final ChangePlan plan) {
    // These dispatch tests begin after the committed authority prerequisite.
    return BrokerTopologyOperationRequests.freeze(state, plan)
        .withExecution(
            new ChangePlan.Execution(1, 1, "", "", plan.changeId(), PlanBlockedReason.NONE, 0));
  }

  private static void admit(final MutableProcessingState state, final String changeId) {
    final var queued =
        state.getBrokerTopologyChangeState().changePlan("demo", changeId).orElseThrow();
    new PartitionChangeAdmittedApplier(state)
        .applyState(
            3, BrokerPartitionChangeRecordMapper.fromPlan(BrokerMigrationBudget.admit(queued, 1)));
  }

  private static void renewLeaderReport(final MutableProcessingState state) {
    final var report =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology("demo", 1)
            .orElseThrow()
            .replicaStates()
            .get("broker-0");
    // A materialized projection alone is insufficient: publish the current process report
    // through the same Applier that normalizes PD receipt time and registration evidence.
    new BrokerRenewedApplier(state, () -> Instant.ofEpochMilli(1000))
        .applyState(
            3,
            new BrokerRegistrationRecord()
                .setClusterId("demo")
                .setBrokerId("broker-0")
                .setPartitions(
                    List.of(
                        new BrokerRegistrationRecord.PartitionStateRecord()
                            .setPartitionId(1)
                            .setRole(report.role())
                            .setTerm(1)
                            .setLifecycleState(
                                BrokerPartitionLifecycle.valueOf(report.lifecycle().name()))
                            .setPriority(report.priority())
                            .setRecoveryEpoch(report.recoveryEpoch())
                            .setMatchIndex(report.matchIndex())
                            .setLag(report.lag())
                            .setLastOpId(report.lastOpId())
                            .setLastOpStatus(report.lastOpStatus())
                            .setElection(report.election()))));
  }

  private static PartitionTopology topologyWithReplicaIndexes(
      final long voterMatchIndex, final long learnerMatchIndex, final long learnerLag) {
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", 1),
        new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
        new PartitionTopology.Membership(
            List.of("broker-0"), List.of("broker-1"), Map.of("broker-0", 100, "broker-1", 0), 0),
        PartitionStatus.REBALANCING,
        Map.of(
            "broker-0",
            new ReplicaState(
                new BrokerPartitionState.Status(
                    MemberRole.LEADER, 0, BrokerPartitionLifecycle.ACTIVE, 100, 0),
                new BrokerPartitionState.Replication(voterMatchIndex, 0, false),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                1,
                BrokerPartitionElectionState.UNKNOWN),
            "broker-1",
            new ReplicaState(
                new BrokerPartitionState.Status(
                    MemberRole.LEARNER, 0, BrokerPartitionLifecycle.CATCHING_UP, 0, 0),
                new BrokerPartitionState.Replication(
                    learnerMatchIndex, learnerLag, learnerLag == 0),
                new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                1,
                BrokerPartitionElectionState.UNKNOWN)));
  }

  private static PartitionTopology leaveTopology() {
    final ReplicaState active =
        new ReplicaState(
            new BrokerPartitionState.Status(
                MemberRole.FOLLOWER, 0, BrokerPartitionLifecycle.ACTIVE, 10, 0),
            new BrokerPartitionState.Replication(5, 0, false),
            new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
            1,
            BrokerPartitionElectionState.UNKNOWN);
    return new PartitionTopology(
        new PartitionTopology.Identity("demo", 1),
        new PartitionTopology.Placement(2, List.of("broker-0", "broker-1"), "broker-0"),
        new PartitionTopology.Membership(
            List.of("broker-0", "broker-1", "broker-2"),
            List.of(),
            Map.of("broker-0", 100, "broker-1", 10, "broker-2", 10),
            0),
        PartitionStatus.REBALANCING,
        Map.of("broker-0", active, "broker-1", active, "broker-2", active));
  }

  private static final class NoopTransport implements BrokerClusterChangeTransport {
    @Override
    public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
        final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
      return CompletableActorFuture.completedExceptionally(
          new UnsupportedOperationException("not used"));
    }
  }

  private static final class AppliedTransport implements BrokerClusterChangeTransport {
    private int calls;
    private BrokerApplyTopologyOperationRequest request;
    private final CompletableActorFuture<BrokerClusterChangeDispatchResult> pending =
        new CompletableActorFuture<>();

    @Override
    public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
        final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
      this.request = request;
      return ++calls == 1
          ? CompletableActorFuture.completed(
              new BrokerClusterChangeDispatchResult(request.opId(), true))
          : pending;
    }
  }

  private static final class CapturingAppliedTransport implements BrokerClusterChangeTransport {
    private BrokerApplyTopologyOperationRequest request;

    @Override
    public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
        final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
      this.request = request;
      return CompletableActorFuture.completed(
          new BrokerClusterChangeDispatchResult(request.opId(), true));
    }
  }
}
