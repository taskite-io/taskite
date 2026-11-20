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
package io.taskite.placementdriver.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.WorkflowScheduler;
import io.taskite.placementdriver.processing.member.MemberAdvanceProcessor;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverMemberChangePlanner;
import io.taskite.placementdriver.processing.topology.MemberExecuteProcessor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeChecker;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeExecutor;
import io.taskite.placementdriver.processing.topology.TopologyMembershipChangeProcessors;
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
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedResponseWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PlacementDriverMemberExecutionTest {
  @Test
  void idleRecoveryStopsAndCancelsOldCallbacks(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.scheduler.runReady();
      assertTrue(fixture.scheduler.tasks.isEmpty());
      final var old = fixture.scheduler.history.getFirst();
      fixture.executor.onRecovered(fixture.scheduler);
      fixture.checker.onRecovered(fixture.scheduler);
      old.action.accept(
          (key, type, intent, value) -> {
            throw new AssertionError("old recovery callback must be fenced");
          });
      fixture.executor.onClose();
      fixture.checker.onClose();
      assertTrue(fixture.scheduler.tasks.isEmpty());
    }
  }

  @Test
  void frequentWakesDoNotCountAsTwoPromotionSamples(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.plan(MemberChangeOperationType.MEMBER_PROMOTE_VOTER);
      fixture.scheduler.runReady();
      for (int count = 0; count < 9; count++) {
        fixture.tick(100);
      }
      verify(fixture.raft, never()).updateRaftMembership(any());
      fixture.tick(1000);
      fixture.tick(100);
      verify(fixture.raft).updateRaftMembership(any());
    }
  }

  @Test
  void lagAndBarrierMustBothBeSatisfied(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.plan(MemberChangeOperationType.MEMBER_PROMOTE_VOTER);
      fixture.topology(120, 99);
      fixture.scheduler.runReady();
      fixture.tick(1000);
      verify(fixture.raft, never()).updateRaftMembership(any());
      fixture.topology(120, 104);
      fixture.tick(1000);
      verify(fixture.raft, never()).updateRaftMembership(any());
      fixture.tick(1000);
      fixture.tick(100);
      verify(fixture.raft).updateRaftMembership(any());
    }
  }

  @Test
  void advanceWaitsForCommittedProgress(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.applyProgress = false;
      fixture.plan(MemberChangeOperationType.MEMBER_JOIN);
      fixture.scheduler.runReady();
      assertEquals(1, fixture.commands.size());
      assertEquals(MemberChangeIntent.JOIN, fixture.commands.getFirst().metadata().getIntent());
      fixture.tick(1000);
      assertEquals(1, fixture.commands.size());
      fixture.tick(14_000);
      assertEquals(2, fixture.commands.size());
    }
  }

  @Test
  void raftOperationStartsAfterExecutionCommit(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.plan(MemberChangeOperationType.TRANSFER_LEADER);
      fixture.scheduler.processor =
          event -> {
            assertEquals(MemberChangeIntent.EXECUTE, event.metadata().getIntent());
            fixture.actor.processRecord(fixture.record((MemberChangeRecord) event.value()));
            verify(fixture.raft, never()).transferLeadership(any());
            fixture.commit();
          };
      fixture.scheduler.runReady();
      verify(fixture.raft).transferLeadership("meta-1");
    }
  }

  @Test
  void oldRaftCompletionCannotClearRecoveredAttempt(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.plan(MemberChangeOperationType.TRANSFER_LEADER);
      fixture.scheduler.runReady();
      final var old = fixture.operation;
      fixture.operation = new CompletableActorFuture<>();
      when(fixture.raft.transferLeadership(any())).thenReturn(fixture.operation);
      fixture.executor.onRecovered(fixture.scheduler);
      fixture.checker.onRecovered(fixture.scheduler);
      fixture.scheduler.runReady();
      old.complete(null);
      fixture.tick(16_000);
      verify(fixture.raft, times(2)).transferLeadership("meta-1");
      assertEquals(0, fixture.topologyChanges.get());
    }
  }

  @Test
  void exhaustedBudgetRetainsActivePlan(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.scheduler.capacity = 0;
      fixture.plan(MemberChangeOperationType.MEMBER_JOIN);
      fixture.scheduler.runReady();
      assertTrue(fixture.commands.isEmpty());
      fixture.scheduler.capacity = 1;
      fixture.tick(100);
      assertEquals(1, fixture.commands.size());
      fixture.tick(100);
      assertTrue(fixture.scheduler.tasks.isEmpty());
    }
  }

  @Test
  void missingEndpointRetriesWithoutFailingExecutionCommand(@TempDir final Path directory) {
    try (var fixture = new Fixture(directory)) {
      fixture.plan(MemberChangeOperationType.MEMBER_REMOVE);
      fixture
          .state
          .getPlacementDriverTopologyState()
          .updateTopology(
              new MemberTopologyRecord()
                  .setMembers(
                      List.of(new MemberTopologyRecord.MemberRecord().setMemberId("meta-0"))));
      fixture.scheduler.runReady();
      verify(fixture.raft, never()).updateRaftMembership(any());
      fixture.topology(100, 100);
      fixture.tick(1000);
      verify(fixture.raft).updateRaftMembership(any());
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final MutableProcessingState state;
    private final StateStoreProcessingStore store;
    private final PlacementDriverRaftRuntime raft = mock(PlacementDriverRaftRuntime.class);
    private final RaftPartition partition = mock(RaftPartition.class);
    private final WorkflowScheduler scheduler = new WorkflowScheduler(new AtomicLong(1000));
    private final AtomicLong topologyChanges = new AtomicLong();
    private final List<LoggedEvent> commands = new ArrayList<>();
    private final MemberExecuteProcessor actor;
    private final TopologyMembershipChangeExecutor executor;
    private final TopologyMembershipChangeChecker checker;
    private CompletableActorFuture<Void> operation = new CompletableActorFuture<>();
    private boolean applyProgress = true;
    private long position;

    Fixture(final Path directory) {
      storage = new PartitionStorage(directory);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      when(raft.raftPartition()).thenReturn(partition);
      when(partition.role()).thenReturn(RaftRole.LEADER);
      when(partition.leaderId()).thenReturn("meta-0");
      when(partition.term()).thenReturn(1L);
      when(partition.lastAppliedIndex()).thenReturn(100L);
      when(partition.members())
          .thenReturn(
              List.of(
                  new RaftMember("meta-0", RaftRole.LEADER),
                  new RaftMember("meta-1", RaftRole.LEARNER)));
      when(raft.updateRaftMembership(any())).thenReturn(operation);
      when(raft.transferLeadership(any())).thenReturn(operation);
      topology(100, 100);
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
              () -> true,
              null,
              () -> new RaftPdMemberChangeTransport(raft),
              Duration.ofSeconds(15),
              Duration.ofSeconds(15));
      actor = new MemberExecuteProcessor(context, executor);
      checker = new TopologyMembershipChangeChecker(state, () -> true, scheduler.clock::get);
      final var advance = new MemberAdvanceProcessor(context);
      final var operations = TypedRecordProcessors.processors();
      TopologyMembershipChangeProcessors.register(operations, context);
      scheduler.processor =
          event -> {
            if (event.metadata().getIntent() == MemberChangeIntent.EXECUTE) {
              actor.processRecord(record((MemberChangeRecord) event.value()));
            } else if (event.metadata().getIntent() == MemberChangeIntent.EXECUTION_RESULT) {
              actor.results().processRecord(record((MemberExecutionRecord) event.value()));
            } else if (event.metadata().getIntent() == MemberChangeIntent.ADVANCE) {
              commands.add(event);
              if (applyProgress) {
                advance.processRecord(record((MemberChangeRecord) event.value()));
              }
            } else if (event.metadata().getIntent() == TopologyIntent.UPDATE) {
              topologyChanges.incrementAndGet();
            } else {
              commands.add(event);
              if (applyProgress) {
                @SuppressWarnings("unchecked")
                final var processor =
                    (TypedRecordProcessor<MemberChangeRecord>)
                        operations.get(ValueType.MEMBER_CHANGE, event.metadata().getIntent(), 1);
                processor.processRecord(record((MemberChangeRecord) event.value()));
              }
            }
            commit();
            checker.onProcessed(record((UnifiedRecordValue) event.value()));
          };
      commit();
      executor.onRecovered(scheduler);
      checker.onRecovered(scheduler);
    }

    <T extends UnifiedRecordValue> TypedRecord<T> record(final T value) {
      @SuppressWarnings("unchecked")
      final TypedRecord<T> record = mock(TypedRecord.class);
      when(record.getValue()).thenReturn(value);
      when(record.getPosition()).thenReturn(++position);
      when(record.getTimestamp()).thenReturn(scheduler.clock.get());
      return record;
    }

    void commit() {
      try {
        store.getCurrentTransaction().commit();
        store.getCurrentTransaction();
      } catch (final Exception error) {
        throw new AssertionError(error);
      }
    }

    void plan(final MemberChangeOperationType type) {
      final var change =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.PLACEMENT_DRIVER)
              .setResourceId(PlacementDriverMemberChangePlanner.RESOURCE_ID)
              .setChangeId("change")
              .setStatus(MemberChangeStatus.IN_PROGRESS)
              .setOperations(
                  List.of(
                      new MemberChangeOperation()
                          .setOperationId("operation")
                          .setType(type)
                          .setMemberId("meta-1")
                          .setBarrierIndex(100)
                          .setStatus(MemberChangeOperationStatus.IN_PROGRESS)));
      state.getMemberChangeState().put(change);
      state.getMemberChangeState().setActive(change);
      commit();
      checker.onProcessed(record(change));
    }

    void topology(final long leaderApplied, final long learnerApplied) {
      state
          .getPlacementDriverTopologyState()
          .updateTopology(
              new MemberTopologyRecord()
                  .setLeaderId("meta-0")
                  .setMembers(
                      List.of(
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("meta-0")
                              .setHost("localhost")
                              .setRaftPort(28000)
                              .setLastApplied(leaderApplied),
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("meta-1")
                              .setHost("localhost")
                              .setRaftPort(28001)
                              .setLastApplied(learnerApplied))));
    }

    void tick(final long elapsed) {
      scheduler.clock.addAndGet(elapsed);
      commit();
      checker.onProcessed(mock(TypedRecord.class));
      scheduler.runReady();
    }

    @Override
    public void close() {
      executor.onClose();
      checker.onClose();
      commit();
      storage.close();
    }
  }
}
