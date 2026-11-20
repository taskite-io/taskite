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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.taskite.placementdriver.broker.distribution.BrokerDistributionRoute;
import io.taskite.placementdriver.broker.distribution.BrokerJobSubscriptionDistribution;
import io.taskite.placementdriver.broker.distribution.BrokerJobSubscriptionTransport;
import io.taskite.placementdriver.processing.broker.distribution.BrokerDistributionScheduler;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionAcknowledgeProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionCompleteProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionDispatchProcessor;
import io.taskite.placementdriver.processing.broker.distribution.CommandDistributionRetryProcessor;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.immutable.BrokerDistributionScheduleState.Entry;
import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.StateWriter;
import io.taskite.streamplatform.api.writer.TypedRejectionWriter;
import io.taskite.topology.BrokerEndpoint;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Exercises durable distribution processing against real RocksDB and controlled RPC responses. */
class BrokerDistributionSchedulingTest {
  private static final String VARIABLES = "{\"amount\":42}";
  private static final BrokerEndpoint ENDPOINT =
      new BrokerEndpoint("broker", "127.0.0.1", 26500, 26600);
  @TempDir Path directory;

  @Test
  void preservesDeploymentMirroringExpansionRedistributionDeletionAndRetry() throws Exception {
    final Snapshot result = script(directory.resolve("distribution"));
    assertEquals(List.of("101:1:true", "102:0:true"), result.completed());
    assertTrue(result.rejections().isEmpty());
    assertEquals(13, result.requests().size());
    assertEquals(
        4,
        result.events().stream()
            .filter(intent -> intent == CommandDistributionIntent.COMPLETED)
            .count());
  }

  private Snapshot script(final Path path) throws Exception {
    try (final var fixture = new Fixture(path)) {
      fixture.seed(deployment(), 2);
      fixture.state.getCommandDistributionState().putTarget(101, "backup", 1);
      fixture.state.getCommandDistributionState().markPending(101, "backup", 1);
      fixture.commit();
      fixture.failNext = true;
      fixture.tick(1100);
      fixture.tick(1100);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().putTarget(101, "demo", 3);
      fixture.state.getCommandDistributionState().markPending(101, "demo", 3);
      fixture.commit();
      fixture.wake();
      fixture.tick(1100);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().putDistribution(deployment().setGeneration(1));
      fixture.state.getCommandDistributionState().resetTarget(101, "demo", 3);
      fixture.state.getCommandDistributionState().resetTarget(101, "backup", 1);
      fixture.commit();
      fixture.wake();
      fixture.tick(1100);
      final var deletion =
          new CommandDistributionRecord<JobSubscriptionDeletionRecord>()
              .setDistributionKey(102)
              .setClusterId("demo")
              .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
              .setIntent(JobSubscriptionIntent.DELETE)
              .setValue(
                  new JobSubscriptionDeletionRecord()
                      .setClusterId("demo")
                      .setTenantId("tenant")
                      .setSubscriptionKey(101));
      fixture.seed(deletion, 3);
      fixture.state.getCommandDistributionState().putTarget(102, "backup", 1);
      fixture.state.getCommandDistributionState().markPending(102, "backup", 1);
      fixture.commit();
      fixture.wake();
      fixture.tick(1100);
      fixture.tick(1100);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(102));
      assertEquals(-1, fixture.state.getCommandDistributionState().scheduling().nextDueAt());
      assertTrue(fixture.scheduler.tasks.isEmpty());
      return new Snapshot(
          List.copyOf(fixture.requests),
          fixture.state.getCommandDistributionState().distributions().stream()
              .map(
                  value ->
                      value.getDistributionKey()
                          + ":"
                          + value.getGeneration()
                          + ":"
                          + fixture
                              .state
                              .getCommandDistributionState()
                              .isPlanComplete(value.getDistributionKey()))
              .toList(),
          fixture.events.stream()
              .filter(
                  intent ->
                      intent == CommandDistributionIntent.ACKNOWLEDGED
                          || intent == CommandDistributionIntent.COMPLETED)
              .toList(),
          List.copyOf(fixture.rejections));
    }
  }

  @Test
  void bDoesNotSendUntilCommittedAndRecoversAttemptDeadlineFromRocksDb() throws Exception {
    final Path path = directory.resolve("recovery");
    long attempt;
    long due;
    try (final var fixture = new Fixture(path)) {
      fixture.seed(deployment(), 1);
      fixture.response = () -> new CompletableActorFuture<>();
      fixture.tick(1);
      attempt = fixture.entry().attemptId();
      due = fixture.entry().dueAt();
      assertTrue(attempt > 0);
      assertEquals(1, fixture.requests.size());
      // The transport itself asserts the prepare event was committed before sending.
    }
    try (final var recovered = new Fixture(path)) {
      assertEquals(attempt, recovered.entry().attemptId());
      assertEquals(due, recovered.entry().dueAt());
      recovered.position.set(attempt + 100);
      recovered.response = () -> new CompletableActorFuture<>();
      recovered.clock.set(due - 1);
      recovered.scheduler.runReady();
      assertTrue(recovered.requests.isEmpty());
      recovered.clock.set(due + 100);
      recovered.scheduler.runReady();
      assertEquals(1, recovered.requests.size());
      assertNotEquals(attempt, recovered.entry().attemptId());
    }
  }

  @Test
  void bFencesLateSuccessAndRetryByAttemptAndGeneration() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("fencing"))) {
      fixture.seed(deployment(), 1);
      final var future = new CompletableActorFuture<Void>();
      fixture.response = () -> future;
      fixture.tick(1);
      final var first = fixture.requestIdentity();
      fixture.clock.set(fixture.entry().dueAt() + 100);
      fixture.scheduler.runReady();
      final long next = fixture.entry().attemptId();
      assertNotEquals(first.getDispatchAttemptId(), next);
      fixture.process(CommandDistributionIntent.ACKNOWLEDGE, first);
      fixture.process(CommandDistributionIntent.RETRY, first);
      assertEquals(next, fixture.entry().attemptId());
      assertFalse(fixture.state.getCommandDistributionState().isPlanComplete(101));
      fixture.state.getCommandDistributionState().putDistribution(deployment().setGeneration(1));
      fixture.state.getCommandDistributionState().resetTarget(101, "demo", 1);
      fixture.commit();
      fixture.process(CommandDistributionIntent.ACKNOWLEDGE, first);
      assertEquals(List.of(RejectionType.INVALID_STATE), fixture.rejections);
      assertEquals(0, fixture.entry().attemptId());
    }
  }

  @Test
  void bCompensatesDroppedResultAndSingleCommandBudgetThenStops() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("admission"))) {
      fixture.seed(deployment(), 3);
      fixture.scheduler.capacity = 1;
      fixture.scheduler.admission =
          event -> event.metadata().getIntent() != CommandDistributionIntent.ACKNOWLEDGE;
      fixture.tick(1);
      assertEquals(1, fixture.requests.size());
      fixture.tick(100);
      fixture.tick(100);
      assertEquals(3, fixture.requests.size());
      fixture.scheduler.admission = ignored -> true;
      fixture.tick(30_100);
      for (int i = 0; i < 4; i++) {
        fixture.tick(100);
      }
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
      assertTrue(fixture.scheduler.tasks.isEmpty());
    }
  }

  @Test
  void bClosesOldCallbacksAndRemovesDeadlinesOnClusterDeletion() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("lifecycle"))) {
      fixture.seed(deployment(), 1);
      final var future = new CompletableActorFuture<Void>();
      fixture.response = () -> future;
      fixture.tick(1);
      fixture.bProcessor.onClose();
      fixture.bScheduler.onClose();
      future.complete(null);
      assertTrue(fixture.scheduler.tasks.isEmpty());
      fixture.state.getCommandDistributionState().deleteCluster("demo");
      fixture.commit();
      assertEquals(-1, fixture.state.getCommandDistributionState().scheduling().nextDueAt());
      assertTrue(
          fixture
              .state
              .getCommandDistributionState()
              .scheduling()
              .get(new PendingTarget(101, "demo", 1))
              .isEmpty());
    }
  }

  @Test
  void bCapsDurableReservationsAndPreservesFailureBackoffOnRecovery() throws Exception {
    final Path path = directory.resolve("cap");
    try (final var fixture = new Fixture(path)) {
      fixture.seed(deployment(), 300);
      fixture.response = () -> new CompletableActorFuture<>();
      fixture.tick(1);
      fixture.tick(100);
      fixture.tick(100);
      fixture.tick(100);
      assertEquals(256, fixture.requests.size());
      fixture.leader.set(false);
      fixture.tick(100);
      assertEquals(256, fixture.requests.size());
    }
    final Path retryPath = directory.resolve("retry");
    long deadline;
    try (final var fixture = new Fixture(retryPath)) {
      fixture.seed(deployment(), 1);
      fixture.failNext = true;
      fixture.tick(1);
      deadline = fixture.entry().dueAt();
      assertEquals(fixture.clock.get() + 1000, deadline);
    }
    try (final var fixture = new Fixture(retryPath)) {
      fixture.position.set(1000);
      fixture.clock.set(deadline - 1);
      fixture.scheduler.runReady();
      assertTrue(fixture.requests.isEmpty());
      fixture.clock.set(deadline + 100);
      fixture.scheduler.runReady();
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
    }
  }

  @Test
  void pendingTargetsResumeAfterLostAcknowledgementAndRepeatedRestart() throws Exception {
    final Path path = directory.resolve("restart");
    long deadline;
    try (final var initial = new Fixture(path)) {
      initial.seed(deployment(), 1);
      initial.scheduler.admission =
          event -> event.metadata().getIntent() != CommandDistributionIntent.ACKNOWLEDGE;
      initial.tick(1100);
      deadline = initial.entry().dueAt();
      assertFalse(initial.state.getCommandDistributionState().isPlanComplete(101));
    }
    try (final var recovered = new Fixture(path)) {
      recovered.response = () -> new CompletableActorFuture<>();
      recovered.clock.set(deadline + 100);
      recovered.tick(1);
      assertTrue(recovered.entry().attemptId() > 0);
      deadline = recovered.entry().dueAt();
    }
    try (final var recovered = new Fixture(path)) {
      recovered.clock.set(deadline + 100);
      recovered.tick(1);
      assertTrue(recovered.state.getCommandDistributionState().isPlanComplete(101));
      assertEquals(-1, recovered.state.getCommandDistributionState().scheduling().nextDueAt());
    }
    try (final var recovered = new Fixture(path)) {
      recovered.tick(1);
      assertTrue(recovered.scheduler.tasks.isEmpty());
      assertTrue(recovered.requests.isEmpty());
    }
  }

  @Test
  void bRetriesMissingRouteAndUsesNewPartitionLeader() throws Exception {
    try (final var fixture = new Fixture(directory.resolve("route"))) {
      fixture.seed(deployment(), 1);
      fixture.route.set(new BrokerDistributionRoute(1, Map.of()));
      fixture.tick(1);
      assertTrue(fixture.requests.isEmpty());
      assertEquals(11_001, fixture.entry().dueAt());
      final var replacement = new BrokerEndpoint("replacement", "127.0.0.2", 26501, 26601);
      fixture.route.set(new BrokerDistributionRoute(1, Map.of(1, replacement)));
      fixture.tick(1100);
      assertEquals(List.of(replacement), fixture.endpoints);
      assertTrue(fixture.state.getCommandDistributionState().isPlanComplete(101));
    }
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> deployment() {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(101)
        .setClusterId("demo")
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setName("charge")
                .setJobType("process")
                .setVariables(VARIABLES));
  }

  private record Snapshot(
      List<String> requests,
      List<String> completed,
      List<Intent> events,
      List<RejectionType> rejections) {}

  private static final class Fixture implements AutoCloseable {
    private final PartitionStorage storage;
    private final StateStoreProcessingStore store;
    private final MutableProcessingState state;
    private final AtomicLong clock = new AtomicLong(10_000);
    private final AtomicLong position = new AtomicLong(100);
    private final AtomicBoolean leader = new AtomicBoolean(true);
    private final WorkflowScheduler scheduler = new WorkflowScheduler(clock);
    private final List<String> requests = new ArrayList<>();
    private final List<BrokerEndpoint> endpoints = new ArrayList<>();
    private final AtomicReference<BrokerDistributionRoute> route = new AtomicReference<>();
    private final List<Intent> events = new ArrayList<>();
    private final List<RejectionType> rejections = new ArrayList<>();
    private Supplier<ActorFuture<Void>> response = () -> CompletableActorFuture.completed(null);
    private boolean failNext;
    private final CommandDistributionDispatchProcessor bProcessor;
    private final CommandDistributionRetryProcessor retryProcessor;
    private final BrokerDistributionScheduler bScheduler;
    private final CommandDistributionAcknowledgeProcessor ack;
    private final CommandDistributionCompleteProcessor complete;
    private boolean committed;

    private Fixture(final Path path) {
      storage = new PartitionStorage(path);
      store = new StateStoreProcessingStore(storage);
      state = new MutableProcessingState(store);
      position.set(
          state
              .getCommandDistributionState()
              .scheduling()
              .get(new PendingTarget(101, "demo", 1))
              .map(entry -> entry.attemptId() + 100)
              .orElse(100L));
      final var context = mock(TypedRecordProcessorContext.class);
      when(context.getWriters()).thenReturn(scheduler.writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      when(context.getKeyGenerator()).thenReturn(position::incrementAndGet);
      final var appliers = PlacementDriverEventAppliers.create(state);
      final var stateWriter = mock(StateWriter.class);
      when(scheduler.writers.state()).thenReturn(stateWriter);
      doAnswer(
              invocation -> {
                committed = false;
                final Intent intent = invocation.getArgument(1);
                events.add(intent);
                appliers.applyState(
                    invocation.getArgument(0), intent, invocation.getArgument(2), 1);
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      doAnswer(
              invocation -> {
                committed = false;
                appliers.applyState(
                    invocation.getArgument(0),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    1);
                return null;
              })
          .when(stateWriter)
          .appendFollowUpEvent(
              anyLong(), any(ValueType.class), any(Intent.class), any(UnifiedRecordValue.class));
      final var rejection = mock(TypedRejectionWriter.class);
      when(scheduler.writers.rejection()).thenReturn(rejection);
      doAnswer(
              invocation -> {
                rejections.add(invocation.getArgument(0));
                return null;
              })
          .when(rejection)
          .appendRejection(any(RejectionType.class), any(String.class));
      ack = new CommandDistributionAcknowledgeProcessor(context);
      complete = new CommandDistributionCompleteProcessor(context);
      final var commandWriter = scheduler.writers.command();
      doAnswer(
              invocation -> {
                process(invocation.getArgument(1), invocation.getArgument(2));
                return null;
              })
          .when(commandWriter)
          .appendFollowUpCommand(anyLong(), any(Intent.class), any(UnifiedRecordValue.class));
      final BrokerJobSubscriptionTransport transport =
          new BrokerJobSubscriptionTransport() {
            @Override
            public ActorFuture<Void> distributeJobSubscription(
                final BrokerEndpoint endpoint,
                final BrokerJobSubscriptionDistribution distribution) {
              assertTrue(committed, "RPC must start after the prepare transaction commits");
              endpoints.add(endpoint);
              requests.add(
                  distribution.distributionKey()
                      + ":"
                      + distribution.clusterId()
                      + ":"
                      + distribution.partitionId()
                      + ":"
                      + distribution.valueType()
                      + ":"
                      + distribution.intent()
                      + ":"
                      + distribution.value().toJson());
              if (failNext) {
                failNext = false;
                return CompletableActorFuture.completedExceptionally(
                    new IllegalStateException("retry"));
              }
              return response.get();
            }

            @Override
            public void close() {}
          };
      route.set(
          new BrokerDistributionRoute(
              300,
              IntStream.rangeClosed(1, 300)
                  .boxed()
                  .collect(Collectors.toMap(partition -> partition, ignored -> ENDPOINT))));
      bProcessor =
          new CommandDistributionDispatchProcessor(
              context,
              leader::get,
              () -> transport,
              ignored -> route.get(),
              Duration.ofSeconds(30));
      bScheduler =
          new BrokerDistributionScheduler(
              state.getCommandDistributionState(), leader::get, clock::get);
      scheduler.processor =
          event -> {
            process(event.metadata().getIntent(), (CommandDistributionRecord<?>) event.value());
            commit();
          };
      state.getCommandDistributionState().rebuildPendingIndex();
      commit();
      retryProcessor = new CommandDistributionRetryProcessor(context, bProcessor::active);
      bProcessor.onRecovered(scheduler);
      bScheduler.onRecovered(scheduler);
    }

    private void process(final Intent intent, final CommandDistributionRecord<?> value) {
      final long commandPosition = position.incrementAndGet();
      final TypedRecord<CommandDistributionRecord<?>> record =
          new TypedRecord<>() {
            @Override
            public long getKey() {
              return value.getDistributionKey();
            }

            @Override
            public CommandDistributionRecord<?> getValue() {
              return value;
            }

            @Override
            public Intent getIntent() {
              return intent;
            }

            @Override
            public ValueType getValueType() {
              return ValueType.COMMAND_DISTRIBUTION;
            }

            @Override
            public long getTimestamp() {
              return clock.get();
            }

            @Override
            public long getPosition() {
              return commandPosition;
            }

            @Override
            public String getRequestStreamId() {
              return "";
            }

            @Override
            public String getRequestId() {
              return "";
            }

            @Override
            public int getLength() {
              return value.getLength();
            }
          };
      if (intent == CommandDistributionIntent.DISPATCH) {
        bProcessor.processRecord(record);
      } else if (intent == CommandDistributionIntent.RETRY) {
        retryProcessor.processRecord(record);
      } else if (intent == CommandDistributionIntent.ACKNOWLEDGE) {
        ack.processRecord(record);
      } else if (intent == CommandDistributionIntent.COMPLETE) {
        complete.processRecord(record);
      }
      bScheduler.onProcessed(record);
    }

    private void seed(final CommandDistributionRecord<?> value, final int partitions) {
      state.getCommandDistributionState().putDistribution(value);
      state.getCommandDistributionState().putTarget(value.getDistributionKey(), "demo", partitions);
      for (int partition = 1; partition <= partitions; partition++) {
        state
            .getCommandDistributionState()
            .markPending(value.getDistributionKey(), "demo", partition);
      }
      commit();
      wake();
    }

    private void wake() {
      process(CommandDistributionIntent.QUERY, deployment());
    }

    private void tick(final long millis) {
      clock.addAndGet(millis);
      scheduler.runReady();
    }

    private void commit() {
      try {
        store.getCurrentTransaction().commit();
      } catch (final Exception error) {
        throw new AssertionError("Failed to commit test state", error);
      }
      committed = true;
    }

    private Entry entry() {
      return state
          .getCommandDistributionState()
          .scheduling()
          .get(new PendingTarget(101, "demo", 1))
          .orElseThrow();
    }

    private CommandDistributionRecord<?> requestIdentity() {
      final var entry = entry();
      final var value =
          deployment()
              .setGeneration(entry.generation())
              .setPartitionId(1)
              .setDispatchAttemptId(entry.attemptId())
              .setNextAttemptAtEpochMillis(entry.dueAt());
      final var bytes = new UnsafeBuffer(new byte[value.getLength()]);
      value.write(bytes, 0);
      final var copy = new CommandDistributionRecord<>();
      copy.wrap(bytes);
      return copy;
    }

    @Override
    public void close() throws Exception {
      bProcessor.onClose();
      bScheduler.onClose();
      store.close();
      storage.close();
    }
  }
}
