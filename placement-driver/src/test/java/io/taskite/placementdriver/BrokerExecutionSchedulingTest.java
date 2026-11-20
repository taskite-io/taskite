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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeDispatchResult;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeTransport;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.request.BrokerApplyTopologyOperationRequest;
import io.taskite.placementdriver.broker.request.BrokerInstallPartitionAuthorityRequest;
import io.taskite.placementdriver.broker.request.BrokerReadPartitionConfigurationRequest;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.processing.topology.PartitionExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord.Phase;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BrokerExecutionSchedulingTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "admit",
        "offline",
        "deadline",
        "finished",
        "promote",
        "leave",
        "transfer",
        "waiting-join",
        "applied",
        "rpc-success",
        "authority",
        "successor",
        "no-quorum"
      })
  void preservesBusinessDecisions(final String scenario) throws Exception {
    script(scenario);
  }

  private List<String> script(final String scenario) throws Exception {
    try (final var f = new Fixture(directory.resolve(scenario))) {
      f.seed();
      final TopologyOperation operation =
          switch (scenario) {
            case "promote" -> TopologyOperation.promoteLearner(1, "d");
            case "leave" -> TopologyOperation.leave(1, "c");
            case "transfer" -> TopologyOperation.transferLeader(1, "b");
            default -> TopologyOperation.join(1, "d");
          };
      ChangePlan plan = f.plan(operation);
      if (scenario.equals("admit")) plan = plan.withStatus(BrokerPartitionChangeStatus.QUEUED);
      if (scenario.equals("waiting-join"))
        plan = plan.withStatus(BrokerPartitionChangeStatus.WAITING_READY);
      if (scenario.equals("authority") || scenario.equals("no-quorum"))
        plan = plan.withExecution(ChangePlan.Execution.LEGACY);
      if (scenario.equals("successor")) {
        plan =
            plan.withStatus(BrokerPartitionChangeStatus.SUPERSEDING)
                .withExecution(
                    new ChangePlan.Execution(
                        1, 1, "", "next", "change", PlanBlockedReason.TARGET_UNAVAILABLE, 1));
      }
      if (scenario.equals("deadline") || scenario.equals("finished")) {
        plan =
            new ChangePlan(
                plan.definition(),
                new ChangePlan.Progress(
                    scenario.equals("finished") ? plan.operations().size() : 0, plan.status()),
                new ChangePlan.Deadline(1, 2),
                plan.execution());
      }
      f.put(plan);
      if (scenario.equals("offline"))
        f.state.getBrokerRegistrationState().markBrokerOffline("demo", "d", Instant.EPOCH);
      if (scenario.equals("no-quorum")) {
        f.state.getBrokerRegistrationState().markBrokerOffline("demo", "b", Instant.EPOCH);
        f.state.getBrokerRegistrationState().markBrokerOffline("demo", "c", Instant.EPOCH);
      }
      if (scenario.equals("applied"))
        f.state
            .getBrokerTopologyChangeState()
            .markOperationApplied("demo", plan.nextOperation().opId());
      f.commit();
      f.tick(100);
      f.tick(100);
      final var result = List.copyOf(f.business);
      if (List.of("rpc-success", "waiting-join").contains(scenario)) {
        assertTrue(result.isEmpty());
        if (scenario.equals("rpc-success")) assertTrue(f.transport.calls >= 1);
        else assertEquals(0, f.transport.calls);
      } else {
        assertFalse(result.isEmpty(), scenario);
        final String expected =
            switch (scenario) {
              case "admit" -> "ADMIT:NONE:QUEUED:0";
              case "offline" -> "SUPERSEDE:TARGET_UNAVAILABLE:PENDING:0";
              case "deadline" -> "SUPERSEDE:PLAN_DEADLINE_EXCEEDED:PENDING:0";
              case "finished" -> "BLOCK:JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT:PENDING:1";
              case "promote", "leave" -> "UPDATE_STATUS:NONE:WAITING_READY:0";
              case "transfer" -> "UPDATE_STATUS:NONE:WAITING_LEADER_CONVERGE:0";
              case "applied" -> "JOIN:NONE:PENDING:0";
              case "authority" -> "AUTHORIZE:NONE:PENDING:0";
              case "no-quorum" -> "SUPERSEDE:TARGET_UNAVAILABLE:PENDING:0";
              case "successor" -> "REPLACE:";
              default -> throw new AssertionError(scenario);
            };
        assertTrue(result.getFirst().startsWith(expected), result.toString());
      }
      return result;
    }
  }

  @Test
  void rpcSuccessWaitsForVerifiedReportAndWrongDigestDoesNotAck() throws Exception {
    try (final var f = new Fixture(directory.resolve("effect"))) {
      f.seed();
      final var plan = f.plan(TopologyOperation.join(1, "d"));
      f.put(plan);
      f.tick(100);
      assertTrue(f.business.isEmpty());
      f.report("wrong", plan.nextOperation().opId());
      f.commit();
      f.tick(1000);
      assertTrue(f.business.isEmpty());
      f.report(
          plan.nextOperation().configuration().targetDigest("demo"), plan.nextOperation().opId());
      f.commit();
      f.tick(1000);
      assertEquals(List.of("JOIN:NONE:PENDING:0"), f.business);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"matching", "wrong-digest", "wrong-operation", "missing"})
  void successfulApplyVerifiesCommittedReportAtNextCheck(final String scenario) throws Exception {
    try (final var f = new Fixture(directory.resolve("prompt-effect-" + scenario))) {
      f.seed();
      final var plan = f.plan(TopologyOperation.join(1, "d"));
      f.put(plan);
      f.tick(100);
      assertEquals(1, f.transport.calls);
      assertTrue(f.business.isEmpty());

      if (!scenario.equals("missing")) {
        f.report(
            scenario.equals("wrong-digest")
                ? "wrong"
                : plan.nextOperation().configuration().targetDigest("demo"),
            scenario.equals("wrong-operation") ? "other-operation" : plan.nextOperation().opId());
        f.commit();
      }
      f.tick(100);

      assertEquals(
          scenario.equals("matching") ? List.of("JOIN:NONE:PENDING:0") : List.of(), f.business);
      assertEquals(1, f.transport.calls);
    }
  }

  @Test
  void durableAttemptSurvivesRestartAndRejectedCallbackRetriesAtDeadline() throws Exception {
    final var path = directory.resolve("restart");
    long attempt;
    try (final var f = new Fixture(path)) {
      f.seed();
      f.transport.hold = true;
      f.put(f.plan(TopologyOperation.join(1, "d")));
      f.tick(100);
      assertEquals(1, f.transport.calls);
      attempt =
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow().attempt();
    }
    try (final var f = new Fixture(path)) {
      f.tick(100);
      assertEquals(0, f.transport.calls);
      f.scheduler.admission =
          event -> event.metadata().getIntent() != BrokerPartitionChangeIntent.EXECUTION_RESULT;
      f.tick(16_000);
      assertEquals(1, f.transport.calls);
      assertTrue(
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow().attempt()
              > attempt);
      f.scheduler.admission = ignored -> true;
      f.tick(16_000);
      assertEquals(2, f.transport.calls);
    }
  }

  @Test
  void staleCallbackAndFollowerCannotAuthorizeAnotherOwner() throws Exception {
    try (final var f = new Fixture(directory.resolve("stale"))) {
      f.seed();
      f.transport.hold = true;
      final var plan = f.plan(TopologyOperation.join(1, "d"));
      f.put(plan);
      f.tick(100);
      final var request =
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow();
      f.put(
          plan.withExecution(
              new ChangePlan.Execution(2, 2, "", "", "change", PlanBlockedReason.NONE, 0)));
      f.result(new PartitionExecutionRecord().setSchedule(request).setSuccess(true));
      assertEquals(
          0,
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow().attempt());
      f.leader.set(false);
      f.tick(1000);
      assertEquals(1, f.transport.calls);
      f.executor.onClose();
      f.transport.pending.complete(
          new BrokerClusterChangeDispatchResult(plan.nextOperation().opId(), true));
      f.scheduler.runReady();
      assertTrue(f.business.isEmpty());
    }
  }

  @Test
  void authoritySourceAndDueIndexRollbackAndReopenTogether() throws Exception {
    final var path = directory.resolve("install");
    try (final var f = new Fixture(path)) {
      f.seed();
      f.transport.holdInstall = true;
      f.put(f.plan(TopologyOperation.join(1, "d")).withExecution(ChangePlan.Execution.LEGACY));
      f.tick(100);
      final var entry =
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow();
      assertEquals(Phase.INSTALL, entry.phase());
      assertEquals("a", entry.source().leaderId());
      f.state.getBrokerTopologyChangeState().removePending("demo", 1);
      assertEquals(-1, f.state.getTopologyMembershipChangeState().nextDueAt());
      f.store.getCurrentTransaction().rollback();
      f.store.getCurrentTransaction();
      assertEquals(entry.dueAt(), f.state.getTopologyMembershipChangeState().nextDueAt());
    }
    try (final var f = new Fixture(path)) {
      assertEquals(
          Phase.INSTALL,
          f.state.getTopologyMembershipChangeState().partition("demo", 1).orElseThrow().phase());
      f.tick(16_000);
      assertEquals(1, f.transport.reads);
      assertTrue(f.business.getFirst().startsWith("AUTHORIZE"));
    }
  }

  @Test
  void repeatedRestartAndWriterRejectionKeepPendingPlan() throws Exception {
    final var path = directory.resolve("switch");
    try (final var f = new Fixture(path)) {
      f.seed();
      f.put(f.plan(TopologyOperation.join(1, "d")));
      f.tick(100);
      assertEquals(1, f.transport.calls);
    }
    try (final var f = new Fixture(path)) {
      f.scheduler.capacity = 0;
      f.tick(100);
      assertEquals(0, f.transport.calls);
      f.scheduler.capacity = 1;
      f.tick(16_000);
      assertEquals(1, f.transport.calls);
    }
    try (final var f = new Fixture(path)) {
      f.tick(32_000);
      assertEquals(1, f.transport.calls);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"ready", "offline", "excluded", "missing", "capacity"})
  void stoppedBootstrapReplaysFrozenAddThenReadsAuthorityWithoutOldAck(final String scenario)
      throws Exception {
    try (final var f = new Fixture(directory.resolve("bootstrap" + scenario))) {
      f.seed();
      f.state
          .getBrokerRegistrationState()
          .renewBroker("demo", "a", Instant.ofEpochMilli(10_000), List.of());
      f.state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(3, List.of("a", "b", "c"), "a"),
                  PartitionTopology.Membership.initial(List.of(), List.of(), "a"),
                  PartitionStatus.CREATING,
                  Map.of(),
                  true));
      final var plan =
          f.plan(TopologyOperation.add(1, "a"))
              .withStatus(BrokerPartitionChangeStatus.SUPERSEDING)
              .withExecution(
                  new ChangePlan.Execution(
                      0, 0, "", "next", "change", PlanBlockedReason.PLAN_DEADLINE_EXCEEDED, 1));
      f.put(plan);
      if (scenario.equals("offline"))
        f.state.getBrokerRegistrationState().markBrokerOffline("demo", "a", Instant.EPOCH);
      if (scenario.equals("excluded"))
        f.state.getBrokerRegistrationState().excludeFromPlacement("demo", "a");
      if (scenario.equals("missing"))
        f.state.getBrokerRegistrationState().removeBroker("demo", "a");
      if (scenario.equals("capacity")) {
        for (final var id : List.of("b", "c", "d"))
          f.state.getBrokerRegistrationState().markBrokerOffline("demo", id, Instant.EPOCH);
      }
      f.commit();
      f.tick(100);
      assertEquals(scenario.equals("ready") ? 1 : 0, f.transport.calls);
      assertEquals(scenario.equals("ready") ? 1 : 0, f.transport.reads);
      assertTrue(f.business.getFirst().startsWith(scenario.equals("ready") ? "REPLACE" : "BLOCK"));
      assertTrue(f.business.stream().noneMatch(item -> item.startsWith("ACK")));
    }
  }

  @Test
  void boundedWriterAndDurableRpcQuotaDeferRemainingPartitions() throws Exception {
    try (final var f = new Fixture(directory.resolve("quota"))) {
      f.seed();
      f.transport.hold = true;
      final var template = f.plan(TopologyOperation.join(1, "d"));
      final var topology =
          f.state.getBrokerPartitionTopologyState().partitionTopology("demo", 1).orElseThrow();
      for (int partition = 1; partition <= 260; partition++) {
        if (partition > 1) {
          f.state
              .getBrokerPartitionTopologyState()
              .putPartitionTopology(
                  new PartitionTopology(
                      new PartitionTopology.Identity("demo", partition),
                      topology.placement(),
                      topology.membership(),
                      topology.status(),
                      Map.of(),
                      false));
          final var leader = new BrokerPartitionState(partition, MemberRole.LEADER, 1);
          // Explicit observation for every partition, retaining the verified management leader.
          final var election =
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(
                      5, RaftConfigurationDigest.ofBroker("demo", f.transport.members)),
                  new BrokerPartitionElectionState.Priorities(100, 100),
                  new BrokerPartitionElectionState.Observation(
                      RaftNodeState.STATE_LEADER, "a", true, 10_000));
          final var reports =
              new ArrayList<>(
                  f.state.getBrokerRegistrationState().registrations("demo").stream()
                      .filter(broker -> broker.brokerId().equals("a"))
                      .findFirst()
                      .orElseThrow()
                      .partitions());
          reports.add(
              new BrokerPartitionState(
                  partition, leader.status(), leader.replication(), leader.operation(), election));
          f.state
              .getBrokerRegistrationState()
              .renewBroker("demo", "a", Instant.ofEpochMilli(10_000), reports);
        }
        final var plan =
            new ChangePlan(
                new ChangePlan.Definition(
                    new ChangePlan.Identity("demo", "change-" + partition, partition),
                    new ChangePlan.Versions(1, 2),
                    template.operations()),
                template.progress(),
                template.deadline(),
                template.execution());
        f.state.getBrokerTopologyChangeState().putChangePlan(plan);
        f.state.getBrokerTopologyChangeState().markPending(plan);
      }
      f.commit();
      f.scheduler.capacity = 100;
      for (int tick = 0; tick < 4; tick++) f.tick(100);
      assertEquals(256, f.transport.calls);
      assertEquals(256, f.state.getTopologyMembershipChangeState().reserved(f.clock.get(), 256));
      assertEquals(
          Phase.READY,
          f.state.getTopologyMembershipChangeState().partition("demo", 260).orElseThrow().phase());
      f.state.getBrokerTopologyChangeState().removePending("demo", 1);
      f.commit();
      f.tick(1000);
      for (int tick = 0; tick < 3; tick++) f.tick(100);
      assertEquals(257, f.transport.calls);
    }
  }

  private static final class Fixture implements AutoCloseable {
    final PartitionStorage storage;
    final StateStoreProcessingStore store;
    final MutableProcessingState state;
    final AtomicLong clock = new AtomicLong(10_000);
    final AtomicBoolean leader = new AtomicBoolean(true);
    final WorkflowScheduler scheduler = new WorkflowScheduler(clock);
    final List<String> business = new ArrayList<>();
    final Transport transport = new Transport();
    final PartitionExecuteProcessor b;
    final TopologyMembershipChangeExecutor executor;
    final TopologyMembershipChangeChecker bScheduler;
    long position;

    Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      // A real log allocates globally increasing positions across restarts.
      position =
          state
              .getTopologyMembershipChangeState()
              .partition("demo", 1)
              .map(entry -> entry.attempt() + 100)
              .orElse(100L);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduler.writers);
      final var stateWriter = mock(StateWriter.class);
      when(scheduler.writers.state()).thenReturn(stateWriter);
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
              leader::get,
              () -> transport,
              null,
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      b = new PartitionExecuteProcessor(context, executor);
      bScheduler = new TopologyMembershipChangeChecker(state, leader::get, clock::get);
      scheduler.processor =
          event -> {
            if (event.metadata().getIntent() == BrokerPartitionChangeIntent.EXECUTE) {
              b.processRecord(record((PartitionChangeRecord) event.value()));
            } else if (event.metadata().getIntent()
                == BrokerPartitionChangeIntent.EXECUTION_RESULT) {
              result((PartitionExecutionRecord) event.value());
            } else {
              final var plan = (PartitionChangeRecord) event.value();
              business.add(
                  event.metadata().getIntent().name()
                      + ":"
                      + plan.getBlockedReason()
                      + ":"
                      + plan.getStatus()
                      + ":"
                      + plan.getNextOperationIndex());
            }
            commit();
          };
      commit();
      executor.onRecovered(scheduler);
      bScheduler.onRecovered(scheduler);
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getTimestamp()).thenReturn(clock.get());
      when(record.getPosition()).thenReturn(++position);
      return record;
    }

    void result(final PartitionExecutionRecord value) {
      b.results().processRecord(record(value));
      commit();
    }

    void seed() {
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(4, 1, 3));
      for (final var id : List.of("a", "b", "c", "d")) {
        state
            .getBrokerRegistrationState()
            .registerBroker(
                "demo",
                id,
                "127.0.0.1",
                26501,
                26502,
                "test",
                30_000,
                Instant.ofEpochMilli(1),
                List.of(),
                id);
      }
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(3, List.of("a", "b", "c"), "a"),
                  PartitionTopology.Membership.initial(List.of("a", "b", "c"), List.of(), "a"),
                  PartitionStatus.ACTIVE,
                  Map.of(),
                  false));
      report(RaftConfigurationDigest.ofBroker("demo", transport.members), "");
      commit();
    }

    void report(final String digest, final String operation) {
      final var report =
          new BrokerPartitionState(
              1,
              new BrokerPartitionState.Status(
                  MemberRole.LEADER, 1, BrokerPartitionLifecycle.ACTIVE, 100, 0),
              new BrokerPartitionState.Replication(10, 0, false),
              new BrokerPartitionState.Operation(
                  operation,
                  operation.isEmpty()
                      ? BrokerTopologyOperationStatus.NONE
                      : BrokerTopologyOperationStatus.APPLIED),
              new BrokerPartitionElectionState(
                  new RaftConfigurationReference(5, digest),
                  new BrokerPartitionElectionState.Priorities(100, 100),
                  new BrokerPartitionElectionState.Observation(
                      RaftNodeState.STATE_LEADER, "a", true, clock.get())));
      state
          .getBrokerRegistrationState()
          .renewBroker("demo", "a", Instant.ofEpochMilli(clock.get()), List.of(report));
    }

    ChangePlan plan(final TopologyOperation operation) {
      final var raw =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", "change", 1),
                  new ChangePlan.Versions(1, 2),
                  List.of(operation)),
              new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
              new ChangePlan.Deadline(1, Long.MAX_VALUE));
      return BrokerTopologyOperationRequests.freeze(state, raw)
          .withExecution(
              new ChangePlan.Execution(1, 1, "", "", "change", PlanBlockedReason.NONE, 0));
    }

    void put(final ChangePlan plan) {
      state.getBrokerTopologyChangeState().putChangePlan(plan);
      state.getBrokerTopologyChangeState().markPending(plan);
      commit();
      final TypedRecord<?> record = mock(TypedRecord.class);
      bScheduler.onProcessed(record);
    }

    void tick(final long millis) {
      clock.addAndGet(millis);
      scheduler.runReady();
    }

    void commit() {
      try {
        store.getCurrentTransaction().commit();
        store.getCurrentTransaction();
      } catch (final Exception error) {
        throw new AssertionError(error);
      }
    }

    @Override
    public void close() throws Exception {
      executor.onClose();
      bScheduler.onClose();
      commit();
      storage.close();
    }
  }

  private static final class Transport implements BrokerClusterChangeTransport {
    final List<BrokerPartitionMember> members =
        List.of(
            new BrokerPartitionMember("a", true, 100),
            new BrokerPartitionMember("b", true, 10),
            new BrokerPartitionMember("c", true, 10));
    int calls;
    int reads;
    boolean hold;
    boolean holdInstall;
    CompletableActorFuture<BrokerClusterChangeDispatchResult> pending;

    @Override
    public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
        final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
      calls++;
      pending = new CompletableActorFuture<>();
      if (!hold) pending.complete(new BrokerClusterChangeDispatchResult(request.opId(), true));
      return pending;
    }

    BrokerPartitionConfigurationRecord configuration(final long generation, final String owner) {
      return new BrokerPartitionConfigurationRecord(
          "demo",
          1,
          "a",
          1,
          5,
          RaftConfigurationDigest.ofBroker("demo", members),
          members,
          0,
          generation,
          owner,
          generation);
    }

    @Override
    public ActorFuture<BrokerPartitionConfigurationRecord> readConfiguration(
        final BrokerEndpoint endpoint, final BrokerReadPartitionConfigurationRequest request) {
      reads++;
      return CompletableActorFuture.completed(configuration(1, "old"));
    }

    @Override
    public ActorFuture<BrokerPartitionConfigurationRecord> installAuthority(
        final BrokerEndpoint endpoint, final BrokerInstallPartitionAuthorityRequest request) {
      return holdInstall
          ? new CompletableActorFuture<>()
          : CompletableActorFuture.completed(
              configuration(request.generation(), request.ownerChangeId()));
    }
  }
}
