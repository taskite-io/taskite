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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.processing.member.MemberAdvanceProcessor;
import io.taskite.placementdriver.processing.member.PdMemberChangeTransport;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverMemberChangePlanner;
import io.taskite.placementdriver.processing.placementdriver.TopologyUpdateProcessor;
import io.taskite.placementdriver.processing.topology.MemberExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeBehavior;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeProcessors;
import io.taskite.placementdriver.state.appliers.MemberChangePlannedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord.Phase;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord.Schedule;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.topology.MemberRole;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PdMemberChangeSchedulingTest {
  private static final String RESOURCE = PlacementDriverMemberChangePlanner.RESOURCE_ID;
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "add",
        "join",
        "promote",
        "barrier",
        "lag",
        "leave",
        "remove",
        "transfer",
        "already-add",
        "already-promote",
        "already-remove",
        "already-transfer"
      })
  void preservesDecisionsAndNativeRequests(final String scenario) throws Exception {
    script(scenario);
  }

  private List<String> script(final String scenario) throws Exception {
    try (final var f = new Fixture(directory.resolve(scenario))) {
      f.seed();
      final var type =
          switch (scenario) {
            case "promote", "barrier", "lag", "already-promote" ->
                MemberChangeOperationType.MEMBER_PROMOTE_VOTER;
            case "leave" -> MemberChangeOperationType.MEMBER_LEAVE;
            case "remove", "already-remove" -> MemberChangeOperationType.MEMBER_REMOVE;
            case "transfer", "already-transfer" -> MemberChangeOperationType.TRANSFER_LEADER;
            case "join" -> MemberChangeOperationType.MEMBER_JOIN;
            default -> MemberChangeOperationType.MEMBER_ADD_LEARNER;
          };
      if (scenario.equals("barrier")) f.report(5, 10);
      if (scenario.equals("lag")) f.report(10, 40);
      if (List.of("add", "join", "already-remove").contains(scenario))
        f.transport.targetRole = null;
      if (scenario.equals("already-promote")) f.transport.targetRole = RaftRole.FOLLOWER;
      if (scenario.equals("already-transfer")) f.transport.leader = "b";
      f.put(f.plan(type));
      f.tick(100);
      f.tick(1000);
      f.tick(1000);
      assertFalse(f.business.isEmpty(), scenario);
      if (scenario.equals("barrier") || scenario.equals("lag")) {
        assertEquals(List.of("ADVANCE:WAITING_CATCH_UP:0"), f.business);
        assertEquals(0, f.transport.calls);
      } else if (scenario.startsWith("already-")) {
        assertEquals(
            List.of(TopologyMembershipChangeBehavior.command(type).name() + ":COMPLETED:1"),
            f.business);
        assertEquals(0, f.transport.calls);
      } else {
        assertEquals(1, f.transport.calls);
        assertTrue(
            f.business.stream()
                .anyMatch(
                    item ->
                        item.startsWith(scenario.equals("transfer") ? "TRANSFER:b" : "UPDATE:")),
            f.business.toString());
      }
      return List.copyOf(f.business);
    }
  }

  @Test
  void samplesSurviveRestartAndStatusOnlyAdvanceButRequireOneSecond() throws Exception {
    final var path = directory.resolve("samples");
    try (final var f = new Fixture(path)) {
      f.seed();
      f.put(
          f.plan(MemberChangeOperationType.MEMBER_PROMOTE_VOTER)
              .setStatus(MemberChangeStatus.PENDING));
      f.tick(100);
      assertEquals(1, f.entry().readySamples());
      assertEquals(MemberChangeStatus.WAITING_CATCH_UP, f.current().getStatus());
      assertEquals(0, f.transport.calls);
    }
    try (final var f = new Fixture(path)) {
      f.tick(500);
      assertEquals(1, f.entry().readySamples());
      assertEquals(0, f.transport.calls);
      f.tick(1000);
      f.tick(100);
      assertEquals(1, f.transport.calls);
      assertEquals(2, f.entry().readySamples());
      assertEquals(MemberChangeStatus.IN_PROGRESS, f.current().getStatus());
    }
  }

  @Test
  void readinessLossAndOperationOrEpochChangeResetSamples() throws Exception {
    try (final var f = new Fixture(directory.resolve("reset"))) {
      f.seed();
      final var plan = f.plan(MemberChangeOperationType.MEMBER_PROMOTE_VOTER);
      f.put(plan);
      f.tick(100);
      f.report(9, 10);
      f.tick(1000);
      assertEquals(0, f.entry().readySamples());
      f.report(10, 10);
      f.tick(1000);
      assertEquals(1, f.entry().readySamples());
      f.put(f.current().copy().setTargetEpoch(3));
      assertEquals(0, f.entry().readySamples());
      f.tick(100);
      assertEquals(1, f.entry().readySamples());
      final var changed = f.current().copy();
      changed.getOperations().getFirst().setOperationId("different");
      f.put(changed);
      assertEquals(0, f.entry().readySamples());
      assertEquals(0, f.transport.calls);
    }
  }

  @Test
  void requestSurvivesRestartAndLostResultRetriesWithNewAttempt() throws Exception {
    final var path = directory.resolve("rpc");
    long attempt;
    try (final var f = new Fixture(path)) {
      f.seed();
      f.transport.targetRole = null;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      attempt = f.entry().attempt();
      assertEquals(Phase.RPC, f.entry().phase());
    }
    try (final var f = new Fixture(path)) {
      f.transport.targetRole = null;
      f.tick(1000);
      assertEquals(0, f.transport.calls);
      f.scheduler.admission =
          event -> event.metadata().getIntent() != MemberChangeIntent.EXECUTION_RESULT;
      f.transport.hold = false;
      f.tick(16_000);
      assertEquals(1, f.transport.calls);
      assertTrue(f.entry().attempt() > attempt);
      assertEquals(Phase.RPC, f.entry().phase());
      f.scheduler.admission = ignored -> true;
      f.tick(16_000);
      assertEquals(2, f.transport.calls);
      assertEquals(Phase.READY, f.entry().phase());
    }
  }

  @Test
  void rpcSuccessRequiresActualRolesAndKeepsRegisteredEndpoints() throws Exception {
    try (final var f = new Fixture(directory.resolve("effect"))) {
      f.seed();
      f.transport.targetRole = null;
      f.transport.hold = false;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      assertEquals(0, f.current().getNextOperationIndex());
      assertFalse(f.current().isTerminal());
      f.report(10, 10);
      f.transport.targetRole = RaftRole.LEARNER;
      f.tick(1000);
      assertTrue(
          f.state
              .getMemberChangeState()
              .active(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .isEmpty());
      assertTrue(
          f.state
              .getTopologyMembershipChangeState()
              .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .isEmpty());
      final var member =
          f.state.getPlacementDriverTopologyState().topology().orElseThrow().getMembers().stream()
              .filter(item -> item.getMemberId().equals("b"))
              .findFirst()
              .orElseThrow();
      // Re-register after the unobserved RPC result; CONFIGURATION must preserve its endpoint.
      assertEquals("127.0.0.1", member.getHost());
      assertEquals(26502, member.getRaftPort());
      assertEquals(10, member.getLastApplied());
    }
  }

  @Test
  void missingEndpointAndWriterRejectionDoNotLoseWork() throws Exception {
    try (final var f = new Fixture(directory.resolve("endpoint"))) {
      f.seed();
      f.transport.targetRole = null;
      final var topology = f.state.getPlacementDriverTopologyState().topology().orElseThrow();
      topology.getMembers().get(1).setRaftPort(0);
      f.state.getPlacementDriverTopologyState().updateTopology(topology);
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.scheduler.capacity = 0;
      f.tick(100);
      assertEquals(0, f.transport.calls);
      f.scheduler.capacity = 1;
      f.tick(100);
      assertEquals(0, f.transport.calls);
      assertEquals(Phase.READY, f.entry().phase());
      f.report(10, 10);
      f.tick(1000);
      assertEquals(1, f.transport.calls);
    }
  }

  @Test
  void lostAdvanceRetriesAfterDurableDeadline() throws Exception {
    final var path = directory.resolve("advance");
    try (final var f = new Fixture(path)) {
      f.seed();
      f.dropAdvance = true;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      assertEquals(Phase.WAIT_ADVANCE, f.entry().phase());
      assertEquals(1, f.business.size());
    }
    try (final var f = new Fixture(path)) {
      f.tick(1000);
      assertFalse(f.current().isTerminal());
      f.tick(16_000);
      assertTrue(
          f.state
              .getTopologyMembershipChangeState()
              .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .isEmpty());
      assertEquals(List.of("JOIN:COMPLETED:1"), f.business);
      assertEquals(0, f.transport.calls);
    }
  }

  @Test
  void oldResultFollowerAndClosedLifecycleCannotAdvanceAnotherEpoch() throws Exception {
    try (final var f = new Fixture(directory.resolve("stale"))) {
      f.seed();
      f.transport.targetRole = null;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      final var request = f.entry();
      f.put(f.current().copy().setTargetEpoch(3));
      f.b
          .results()
          .processRecord(
              f.record(new MemberExecutionRecord().setSchedule(request).setSuccess(true)));
      assertEquals(0, f.entry().attempt());
      f.leader.set(false);
      f.tick(1000);
      assertEquals(1, f.transport.calls);
      f.executor.onClose();
      f.leader.set(true);
      f.transport.pending.complete(null);
      f.scheduler.runReady();
      assertEquals(0, f.entry().attempt());
      assertTrue(f.business.stream().noneMatch(item -> item.startsWith("ADVANCE")));
    }
  }

  @Test
  void applierRollbackAndRepeatedRestartRetainPlan() throws Exception {
    final var path = directory.resolve("switch");
    try (final var f = new Fixture(path)) {
      f.seed();
      f.transport.targetRole = null;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      assertEquals(1, f.transport.calls);
      f.state.getMemberChangeState().clearActive(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE);
      assertTrue(
          f.state
              .getTopologyMembershipChangeState()
              .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .isEmpty());
      f.store.getCurrentTransaction().rollback();
      f.store.getCurrentTransaction();
      assertNotNull(f.entry());
      assertFalse(f.current().isTerminal());
    }
    try (final var f = new Fixture(path)) {
      f.transport.targetRole = null;
      f.tick(16_000);
      assertEquals(1, f.transport.calls);
      assertEquals(Phase.RPC, f.entry().phase());
    }
    try (final var f = new Fixture(path)) {
      f.transport.targetRole = null;
      f.tick(32_000);
      assertEquals(1, f.transport.calls);
      assertEquals("change", f.current().getChangeId());
    }
  }

  @Test
  void emptyPlanCompletesAndIdleSchedulerStopsUntilNewPlan() throws Exception {
    try (final var f = new Fixture(directory.resolve("idle"))) {
      f.seed();
      f.tick(100);
      assertTrue(f.scheduler.tasks.isEmpty());
      final var plan = f.plan(MemberChangeOperationType.MEMBER_JOIN).setOperations(List.of());
      f.put(plan);
      f.tick(100);
      f.tick(100);
      assertTrue(
          f.state
              .getTopologyMembershipChangeState()
              .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .isEmpty());
      assertTrue(f.scheduler.tasks.isEmpty());
      assertEquals(List.of("ADVANCE:COMPLETED:0"), f.business);
      f.transport.targetRole = null;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER).setChangeId("next"));
      f.tick(100);
      assertEquals(1, f.transport.calls);
      assertEquals("next", f.entry().operation().changeId());
    }
  }

  @Test
  void oldAttemptResultCannotReleaseRetriedRequestForSameOperation() throws Exception {
    try (final var f = new Fixture(directory.resolve("old-attempt"))) {
      f.seed();
      f.transport.targetRole = null;
      f.put(f.plan(MemberChangeOperationType.MEMBER_ADD_LEARNER));
      f.tick(100);
      final var old = f.entry();
      f.tick(16_000);
      final var retried = f.entry();
      assertEquals(2, f.transport.calls);
      assertTrue(retried.attempt() > old.attempt());
      f.b
          .results()
          .processRecord(f.record(new MemberExecutionRecord().setSchedule(old).setSuccess(true)));
      assertEquals(retried, f.entry());
      assertEquals(0, f.current().getNextOperationIndex());
    }
  }

  private static final class Fixture implements AutoCloseable {
    final PartitionStorage storage;
    final StateStoreProcessingStore store;
    final MutableProcessingState state;
    final AtomicLong clock = new AtomicLong(10_000);
    final AtomicBoolean leader = new AtomicBoolean(true);
    final WorkflowScheduler scheduler = new WorkflowScheduler(clock);
    final Transport transport = new Transport();
    final List<String> business = new ArrayList<>();
    final MemberExecuteProcessor b;
    final TopologyMembershipChangeExecutor executor;
    final TopologyMembershipChangeChecker checker;
    boolean dropAdvance;
    long position;

    Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      position =
          state
              .getTopologyMembershipChangeState()
              .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
              .map(entry -> entry.attempt() + 100)
              .orElse(100L);
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getWriters()).thenReturn(scheduler.writers);
      when(scheduler.writers.response()).thenReturn(mock(TypedResponseWriter.class));
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
              null,
              () -> transport,
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      b = new MemberExecuteProcessor(context, executor);
      checker = new TopologyMembershipChangeChecker(state, leader::get, clock::get);
      final var advance = new MemberAdvanceProcessor(context);
      final var operations = TypedRecordProcessors.processors();
      TopologyMembershipChangeProcessors.register(operations, context);
      final var topology = new TopologyUpdateProcessor(context);
      scheduler.processor =
          event -> {
            final var intent = event.metadata().getIntent();
            if (intent == MemberChangeIntent.EXECUTE) {
              b.processRecord(record((MemberChangeRecord) event.value()));
            } else if (intent == MemberChangeIntent.EXECUTION_RESULT) {
              b.results().processRecord(record((MemberExecutionRecord) event.value()));
            } else if (intent == MemberChangeIntent.ADVANCE) {
              final var change = (MemberChangeRecord) event.value();
              business.add("ADVANCE:" + change.getStatus() + ":" + change.getNextOperationIndex());
              if (!dropAdvance) advance.processRecord(record(change));
            } else if (intent == TopologyIntent.UPDATE) {
              topology.processRecord(record((MemberTopologyRecord) event.value()));
            } else {
              final var change = (MemberChangeRecord) event.value();
              business.add(
                  intent.name() + ":" + change.getStatus() + ":" + change.getNextOperationIndex());
              if (!dropAdvance) {
                @SuppressWarnings("unchecked")
                final var processor =
                    (TypedRecordProcessor<MemberChangeRecord>)
                        operations.get(ValueType.MEMBER_CHANGE, intent, 1);
                processor.processRecord(record(change));
              }
            }
            commit();
            checker.onProcessed(record((UnifiedRecordValue) event.value()));
          };
      transport.business = business;
      commit();
      executor.onRecovered(scheduler);
      checker.onRecovered(scheduler);
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getTimestamp()).thenReturn(clock.get());
      when(record.getPosition()).thenReturn(++position);
      return record;
    }

    MemberChangeRecord plan(final MemberChangeOperationType type) {
      return new MemberChangeRecord()
          .setScope(MemberChangeScope.PLACEMENT_DRIVER)
          .setResourceId(RESOURCE)
          .setChangeId("change")
          .setSourceEpoch(1)
          .setTargetEpoch(2)
          .setStatus(MemberChangeStatus.IN_PROGRESS)
          .setOperations(
              List.of(
                  new MemberChangeOperation()
                      .setOperationId("operation")
                      .setType(type)
                      .setMemberId("b")
                      .setBarrierIndex(10)
                      .setStatus(MemberChangeOperationStatus.IN_PROGRESS)));
    }

    void seed() {
      report(10, 10);
    }

    void report(final long targetApplied, final long leaderApplied) {
      state
          .getPlacementDriverTopologyState()
          .updateTopology(
              new MemberTopologyRecord()
                  .setLeaderId("a")
                  .setLocalMemberId("a")
                  .setMembers(
                      List.of(
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("a")
                              .setHost("127.0.0.1")
                              .setRaftPort(26501)
                              .setRole(MemberRole.LEADER)
                              .setLastApplied(leaderApplied),
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("b")
                              .setHost("127.0.0.1")
                              .setRaftPort(26502)
                              .setRole(MemberRole.LEARNER)
                              .setLastApplied(targetApplied))));
      commit();
    }

    void put(final MemberChangeRecord change) {
      new MemberChangePlannedApplier(state).applyState(1, change);
      commit();
      checker.onProcessed(record(change));
    }

    MemberChangeRecord current() {
      return state
          .getMemberChangeState()
          .active(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
          .orElseThrow();
    }

    Schedule entry() {
      return state
          .getTopologyMembershipChangeState()
          .member(MemberChangeScope.PLACEMENT_DRIVER, RESOURCE)
          .orElseThrow();
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
      checker.onClose();
      commit();
      storage.close();
    }
  }

  private static final class Transport implements PdMemberChangeTransport {
    RaftRole targetRole = RaftRole.LEARNER;
    String leader = "a";
    List<String> business;
    int calls;
    boolean hold = true;
    CompletableActorFuture<Void> pending;

    @Override
    public List<RaftMember> members() {
      return targetRole == null
          ? List.of(new RaftMember("a", RaftRole.LEADER))
          : List.of(new RaftMember("a", RaftRole.LEADER), new RaftMember("b", targetRole));
    }

    @Override
    public String leaderId() {
      return leader;
    }

    @Override
    public long term() {
      return 1;
    }

    @Override
    public long lastApplied() {
      return 10;
    }

    @Override
    public ActorFuture<Void> update(final List<RaftPeer> peers) {
      business.add("UPDATE:" + peers);
      return send();
    }

    @Override
    public ActorFuture<Void> transfer(final String id) {
      business.add("TRANSFER:" + id);
      return send();
    }

    private ActorFuture<Void> send() {
      calls++;
      pending = new CompletableActorFuture<>();
      if (!hold) pending.complete(null);
      return pending;
    }
  }
}
