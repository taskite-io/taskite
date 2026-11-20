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
package io.taskite.streamplatform.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbKey;
import io.taskite.db.DbTransaction;
import io.taskite.db.DbValue;
import io.taskite.db.KeyValuePairVisitor;
import io.taskite.db.TransactionOperation;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.NoopTransaction;
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.command.StreamPartitionCommandDispatcher;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import io.taskite.streamplatform.state.PersistedKeyGenerator;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Guards ZEP004's central invariant for Layer A: <b>a command changes state only by writing events,
 * and replaying those events rebuilds exactly the same state</b> ("Activities during Replay"). The
 * test drives the two real state machines the {@link StreamProcessor} owns — the leader {@link
 * ProcessingStateMachine} (processes commands, eager-applies their events) and the follower {@link
 * ReplayStateMachine} (applies only committed events) — and asserts the resulting business state
 * <em>and</em> the key-generator high-water are identical.
 *
 * <p>The domain is deliberately Layer-A only (a "create entity with a generated key" command),
 * because stream-platform must not depend on the engine's business processors. It exercises the
 * generic mechanism, including the Phase 1 key-generator recovery: a follower that replays {@code
 * CREATED} events must, once promoted, issue keys strictly above every replayed key.
 *
 * <p>A negative case registers a processor that writes state directly (bypassing the event) and
 * shows the equivalence check catches the drift — proving the guardrail actually guards.
 */
final class ProcessingReplayEquivalenceTest {

  private static final int PARTITION_ID = 1;
  private static final String LATEST_KEY = "keygen/latestKey";

  private final DirectConcurrencyControl control = new DirectConcurrencyControl();
  private final TypedEventRegistry eventRegistry =
      TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build();

  // ------------------------------------------------------------------------------------------
  // Positive: processing state == replay state, for both business columns and the key high-water.
  // ------------------------------------------------------------------------------------------

  @Test
  void replayRebuildsTheSameStateAsProcessing() {
    // The committed log holds three CREATE commands; a leader processes each, minting a key and
    // eager-applying a CREATED event whose follow-up entry is captured back into the log.
    final List<LoggedEvent> log = new ArrayList<>(commands(3));
    final InMemoryStore leaderStore = new InMemoryStore();
    process(leaderStore, log, /* rogue= */ false);

    // A fresh follower replays the full committed log (commands skipped, CREATED events applied).
    final InMemoryStore followerStore = new InMemoryStore();
    replay(followerStore, log);

    assertEquals(
        leaderStore.snapshot(),
        followerStore.snapshot(),
        "state rebuilt by replay must equal state produced by processing");
    // Three entities created ⇒ the persisted high-water is the third partition-encoded key.
    assertEquals(
        Long.toString(Protocol.encodePartitionId(PARTITION_ID, 3)),
        leaderStore.snapshot().get("KEY:" + LATEST_KEY));
  }

  @Test
  void processesAdmittedFollowUpCommandInTheSameBatch() {
    final List<LoggedEvent> log = new ArrayList<>(List.of(command(1, 1234)));
    final InMemoryStore leaderStore = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(leaderStore, appender);
    final TypedRecordProcessors processors = chainedProcessors(context);
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    leaderStore, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            processors,
            new StreamProcessorContext.Batching(2, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(1, appender.appendCalls());
    assertEquals(4, log.size());
    assertEquals(1L, leaderStore.getLastProcessedPosition());
    assertTrue(log.get(2).shouldSkipProcessing());
    assertEquals(1L, log.get(1).getSourceEventPosition());
    assertEquals(1L, log.get(2).getSourceEventPosition());
    assertEquals(1L, log.get(3).getSourceEventPosition());
    assertEquals(1234, context.getProcessingTime());
    assertEquals(1, metrics.batches.get());
    assertEquals(2, metrics.batchCommands.get());
    assertEquals(0, metrics.continuations.get());
    assertTrue(metrics.batchDurationNanos.get() >= 0);

    final InMemoryStore followerStore = new InMemoryStore();
    replay(followerStore, log);
    assertEquals(leaderStore.snapshot(), followerStore.snapshot());
  }

  @Test
  void leavesFollowUpCommandForTheNextBatchWhenLimitIsOne() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore leaderStore = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(leaderStore, appender);
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    leaderStore, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            chainedProcessors(context),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(2, appender.appendCalls());
    assertEquals(4, log.size());
    assertFalse(log.get(2).shouldSkipProcessing());
    assertEquals(1L, log.get(2).getSourceEventPosition());
    assertEquals(3L, log.get(3).getSourceEventPosition());
    assertEquals(3L, leaderStore.getLastProcessedPosition());
    assertEquals(2, metrics.batches.get());
    assertEquals(2, metrics.batchCommands.get());
    assertEquals(1, metrics.continuations.get());

    final InMemoryStore followerStore = new InMemoryStore();
    replay(followerStore, log);
    assertEquals(leaderStore.snapshot(), followerStore.snapshot());
  }

  @Test
  void processesSiblingFollowUpCommandsInWriteOrder() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore store = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(store, appender);
    final List<Long> processedKeys = new ArrayList<>();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB, JobIntent.COMPLETE, new FanOutProcessor(context.getWriters()))
                .onCommand(
                    ValueType.JOB, JobIntent.FAIL, new RecordingActivateProcessor(processedKeys)),
            new StreamProcessorContext.Batching(3, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(List.of(11L, 22L), processedKeys);
    assertEquals(1, appender.appendCalls());
    assertTrue(log.get(1).shouldSkipProcessing());
    assertTrue(log.get(2).shouldSkipProcessing());
  }

  @Test
  void defersNewFollowUpCommandsWhenTheRecordBudgetIsReached() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore store = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(store, appender);
    final List<Long> processedKeys = new ArrayList<>();
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB, JobIntent.COMPLETE, new FanOutProcessor(context.getWriters()))
                .onCommand(
                    ValueType.JOB, JobIntent.FAIL, new RecordingActivateProcessor(processedKeys)),
            new StreamProcessorContext.Batching(3, 2, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(List.of(11L, 22L), processedKeys);
    assertFalse(log.get(1).shouldSkipProcessing());
    assertFalse(log.get(2).shouldSkipProcessing());
    assertEquals(2, metrics.recordLimitContinuations.get());
    assertEquals(0, metrics.timeLimitContinuations.get());
  }

  @Test
  void defersNewFollowUpCommandsWhenTheTimeBudgetIsReached() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore store = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(store, appender);
    final List<Long> processedKeys = new ArrayList<>();
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB, JobIntent.COMPLETE, new FanOutProcessor(context.getWriters()))
                .onCommand(
                    ValueType.JOB, JobIntent.FAIL, new RecordingActivateProcessor(processedKeys)),
            new StreamProcessorContext.Batching(3, 0, Duration.ofNanos(1), Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(List.of(11L, 22L), processedKeys);
    assertFalse(log.get(1).shouldSkipProcessing());
    assertFalse(log.get(2).shouldSkipProcessing());
    assertEquals(0, metrics.recordLimitContinuations.get());
    assertEquals(2, metrics.timeLimitContinuations.get());
  }

  @Test
  void retriesWithCompletedCommandCountAsTheTemporaryLimit() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore store = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final AtomicInteger failures = new AtomicInteger();
    final CommandFailureObserver failureObserver = (requestId, error) -> failures.incrementAndGet();
    final TypedRecordProcessorContext context = context(store, appender, 1024, failureObserver);
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(store, () -> 1L, ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    failureObserver,
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new SmallFollowUpProcessor(context.getWriters()))
                .onCommand(
                    ValueType.JOB,
                    JobIntent.FAIL,
                    new OversizedFollowUpProcessor(context.getWriters())),
            new StreamProcessorContext.Batching(2, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(1, appender.appendCalls());
    assertEquals(2, log.size());
    assertEquals(RecordType.COMMAND, metadata(log.get(1)).getRecordType());
    assertFalse(log.get(1).shouldSkipProcessing());
    assertEquals(1L, store.getLastProcessedPosition());
    assertEquals(1, metrics.retries.get());
    assertEquals(1, metrics.continuations.get());
    assertEquals(0, failures.get());
  }

  @Test
  void writesMinimalRejectionWhenRootCommandExceedsPayloadLimit() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore store = new InMemoryStore();
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final AtomicInteger failures = new AtomicInteger();
    final CommandFailureObserver failureObserver = (requestId, error) -> failures.incrementAndGet();
    final TypedRecordProcessorContext context = context(store, appender, 1024, failureObserver);
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(store, () -> 1L, ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    failureObserver,
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new OversizedFollowUpProcessor(context.getWriters())),
            new StreamProcessorContext.Batching(2, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(1, appender.appendCalls());
    assertEquals(2, log.size());
    final RecordMetadata rejection = metadata(log.get(1));
    assertEquals(RecordType.COMMAND_REJECTION, rejection.getRecordType());
    assertEquals(RejectionType.EXCEEDED_BATCH_RECORD_SIZE, rejection.getRejectionType());
    assertEquals("", rejection.getRejectionReason());
    assertEquals(0, eventRegistry.read(log.get(1), rejection).getLength());
    assertEquals(1, metrics.retries.get());
    assertEquals(0, failures.get());
  }

  @Test
  void handlesDefensiveSequencerOverflowThroughTheSameRetryPath() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final AtomicInteger appendCalls = new AtomicInteger();
    final RecordAppender appender =
        (entries, sourcePosition) -> {
          if (appendCalls.incrementAndGet() == 1) {
            return CompletableActorFuture.completedExceptionally(
                new ExceededBatchRecordSizeException(1025, 1024));
          }
          long position = initialAppendPosition(log);
          for (final LogAppendEntry entry : entries) {
            log.add(LoggedEvent.of(entry, position++, sourcePosition, 0));
          }
          return CompletableActorFuture.completed(position - 1);
        };
    final InMemoryStore store = new InMemoryStore();
    final AtomicInteger failures = new AtomicInteger();
    final CommandFailureObserver failureObserver = (requestId, error) -> failures.incrementAndGet();
    final TypedRecordProcessorContext context =
        context(store, appender, Integer.MAX_VALUE, failureObserver);
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(store, () -> 1L, ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    failureObserver,
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new SmallFollowUpProcessor(context.getWriters())),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);

    assertEquals(2, appendCalls.get());
    assertEquals(2, log.size());
    assertEquals(RejectionType.EXCEEDED_BATCH_RECORD_SIZE, metadata(log.get(1)).getRejectionType());
    assertEquals(1, metrics.retries.get());
    assertEquals(0, failures.get());
  }

  @Test
  void promotedFollowerIssuesKeysAboveEveryReplayedKey() {
    // Models ZEP004 "Key Generation" recovery through ReplayStateMachine: CREATED events whose keys
    // sit far above a fresh generator's start are replayed; the recovered high-water must make the
    // next minted key strictly greater than all of them (no re-issue after promotion).
    final long highCounter = 500;
    final List<LoggedEvent> log =
        List.of(
            createdEvent(10L, 1L, Protocol.encodePartitionId(PARTITION_ID, highCounter)),
            createdEvent(11L, 2L, Protocol.encodePartitionId(PARTITION_ID, highCounter + 1)));

    final InMemoryStore followerStore = new InMemoryStore();
    replay(followerStore, log);

    // Promote: a brand-new generator over the recovered store must not collide with a replayed key.
    final PersistedKeyGenerator promoted =
        new PersistedKeyGenerator(0, PARTITION_ID, followerStore);
    assertEquals(Protocol.encodePartitionId(PARTITION_ID, highCounter + 2), promoted.nextKey());
  }

  @Test
  void closingWhileAppendIsPendingRollsBackCurrentTransaction() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final TrackingTransaction transaction = new TrackingTransaction();
    final InMemoryStore store = new InMemoryStore(transaction);
    final TypedRecordProcessorContext context =
        context(store, (entry, sourcePosition) -> new CompletableActorFuture<>());
    final TypedRecordProcessors processors =
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB,
                JobIntent.COMPLETE,
                new CreateProcessor(context.getKeyGenerator(), context.getWriters()));
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            processors,
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);
    stateMachine.close();

    assertTrue(transaction.rolledBack);
    assertTrue(!transaction.committed);
  }

  @Test
  void shouldIgnoreLateAppendCompletionAfterClose() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var append = new CompletableActorFuture<Long>();
    final var context = context(store, (entry, source) -> append);
    final var log = new ArrayList<>(commands(1));
    final var machine = machine(store, context, log);
    machine.startProcessing(0);
    machine.close();
    append.complete(2L);
    assertTrue(transaction.rolledBack);
    assertTrue(!transaction.committed);
    assertEquals(0, machine.getLastSuccessfulProcessedRecordPosition());
  }

  @Test
  void recordsProcessingMetricsOnlyAfterTheTransactionCommits() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var append = new CompletableActorFuture<Long>();
    final var context = context(store, (entry, source) -> append);
    final var log = new ArrayList<>(commands(1));
    final var metrics = new RecordingProcessingMetrics();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new CreateProcessor(context.getKeyGenerator(), context.getWriters())),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertEquals(0, metrics.processed.get());
    assertEquals(0, metrics.written.get());
    assertEquals(0, metrics.batches.get());

    append.complete(2L);

    assertTrue(transaction.committed);
    assertEquals(1, metrics.processed.get());
    assertEquals(1, metrics.written.get());
    assertEquals(1, metrics.batches.get());
    assertEquals(1, metrics.batchCommands.get());
    assertEquals(0, metrics.continuations.get());
    assertEquals(1, metrics.processedPosition.get());
    assertEquals(1, metrics.committedPosition.get());
  }

  @Test
  void stateCommitWithoutResponseDoesNotCompleteSubmittedRequest() {
    for (final boolean reject : List.of(false, true)) {
      final var transaction = new TrackingTransaction();
      final var store = new InMemoryStore(transaction);
      final var append = new CompletableActorFuture<Long>();
      final var context = context(store, (entry, source) -> append);
      final var log = new ArrayList<>(commands(1));
      final var dispatcher =
          new StreamPartitionCommandDispatcher(
              PARTITION_ID,
              1,
              () -> true,
              (entry, source) -> CompletableActorFuture.completed(1L),
              control);
      final var metadata = log.getFirst().readMetadata(new RecordMetadata());
      final var result =
          dispatcher.submit(
              LoggedEvent.ofRecord(
                  log.getFirst().key(), metadata, eventRegistry.read(log.getFirst(), metadata)));
      final var machine =
          new ProcessingStateMachine(
              new StreamStateMachineContext(
                  new StreamStateMachineContext.Identity(
                      PARTITION_ID, StreamProcessorMode.PROCESSING),
                  new StreamStateMachineContext.Processing(eventRegistry, context),
                  new StreamProcessorContext.State(
                      store, () -> committedPosition(log), ignored -> {}),
                  new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                  new StreamStateMachineContext.Observers(
                      dispatcher::onFailure,
                      dispatcher::onResponse,
                      CommittedRecordObserver.noop(),
                      ProcessingMetrics.NOOP)),
              TypedRecordProcessors.processors()
                  .onCommand(
                      ValueType.JOB,
                      JobIntent.COMPLETE,
                      new TypedRecordProcessor<JobRecord>() {
                        @Override
                        public void processRecord(final TypedRecord<JobRecord> record) {
                          if (reject) {
                            context
                                .getWriters()
                                .rejection()
                                .appendRejection(RejectionType.INVALID_STATE, "already applied");
                          }
                        }
                      }),
              new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));
      machine.startProcessing(0);
      if (reject) {
        assertFalse(result.isDone());
        assertFalse(transaction.committed);
        append.complete(2L);
      }
      assertTrue(transaction.committed);
      assertFalse(result.isDone());
      machine.close();
      dispatcher.close();
    }
  }

  @Test
  void childRejectionDoesNotReplaceExplicitRootResponseWithTheSameRequestId() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var context = context(store, noopAppender());
    final var log = new ArrayList<>(commands(1));
    final var dispatcher =
        new StreamPartitionCommandDispatcher(
            PARTITION_ID,
            1,
            () -> true,
            (entry, source) -> CompletableActorFuture.completed(1L),
            control);
    final var metadata = log.getFirst().readMetadata(new RecordMetadata());
    final var result =
        dispatcher.submit(
            LoggedEvent.ofRecord(
                log.getFirst().key(), metadata, eventRegistry.read(log.getFirst(), metadata)));
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    dispatcher::onFailure,
                    dispatcher::onResponse,
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new TypedRecordProcessor<JobRecord>() {
                      @Override
                      public void processRecord(final TypedRecord<JobRecord> record) {
                        new ChainedCreateProcessor(context.getKeyGenerator(), context.getWriters())
                            .processRecord(record);
                        context
                            .getWriters()
                            .response()
                            .writeEventOnCommand(
                                record.getKey(), JobIntent.CREATED, record.getValue(), record);
                      }
                    })
                .onCommand(
                    ValueType.JOB,
                    JobIntent.FAIL,
                    new TypedRecordProcessor<JobRecord>() {
                      @Override
                      public void processRecord(final TypedRecord<JobRecord> record) {
                        context
                            .getWriters()
                            .rejection()
                            .appendRejection(RejectionType.INVALID_STATE, "child rejected");
                      }
                    }),
            new StreamProcessorContext.Batching(2, 0, Duration.ZERO, Integer.MAX_VALUE));
    machine.startProcessing(0);
    assertTrue(transaction.committed);
    assertTrue(result.isDone());
    assertFalse(result.isCompletedExceptionally());
    assertEquals(RecordType.EVENT, result.join().metadata().getRecordType());
    assertEquals(JobIntent.CREATED, result.join().metadata().getIntent());
    assertEquals("req", result.join().metadata().getRequestId());
    machine.close();
    dispatcher.close();
  }

  @Test
  void deliversResponsesThenPostCommitActionsOnlyAfterStateCommit() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var append = new CompletableActorFuture<Long>();
    final var context = context(store, (entry, source) -> append);
    final var log = new ArrayList<>(commands(1));
    final List<String> order = new ArrayList<>();
    final AtomicInteger committedObservations = new AtomicInteger();
    final CommandResponseObserver responses =
        response -> {
          assertTrue(transaction.committed);
          assertEquals("req", response.metadata().getRequestId());
          order.add("response");
        };
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    responses,
                    new CommittedRecordObserver() {
                      @Override
                      public void onProcessed(final LoggedEvent record) {
                        committedObservations.incrementAndGet();
                      }

                      @Override
                      public void onFailure(final LoggedEvent record, final Throwable error) {}
                    },
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .withLifecycleListener(
                    new StreamProcessorLifecycleAware() {
                      @Override
                      public void onRecovered(final ProcessingScheduleService scheduling) {}

                      @Override
                      public void onProcessed(final TypedRecord<?> command) {
                        assertTrue(transaction.committed);
                        assertEquals(1, command.getPosition());
                        order.add("processed");
                      }
                    })
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new ResponseAndPostCommitProcessor(
                        context,
                        () -> {
                          assertTrue(transaction.committed);
                          order.add("action");
                        })),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertFalse(transaction.committed);
    assertTrue(order.isEmpty());

    append.complete(2L);

    assertTrue(transaction.committed);
    assertEquals(List.of("response", "action", "processed"), order);
    assertEquals(0, committedObservations.get());
  }

  @Test
  void discardsResponsesAndPostCommitActionsWhenStateCommitFails() {
    final var transaction = new FailingCommitTransaction();
    final var store = new InMemoryStore(transaction);
    final var context =
        context(store, (entries, sourcePosition) -> CompletableActorFuture.completed(2L));
    final var log = new ArrayList<>(commands(1));
    final AtomicInteger responses = new AtomicInteger();
    final AtomicInteger actions = new AtomicInteger();
    final AtomicInteger processed = new AtomicInteger();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    response -> responses.incrementAndGet(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .withLifecycleListener(
                    new StreamProcessorLifecycleAware() {
                      @Override
                      public void onRecovered(final ProcessingScheduleService scheduling) {}

                      @Override
                      public void onProcessed(final TypedRecord<?> command) {
                        processed.incrementAndGet();
                      }
                    })
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new ResponseAndPostCommitProcessor(context, actions::incrementAndGet)),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertTrue(transaction.rolledBack);
    assertEquals(0, responses.get());
    assertEquals(0, actions.get());
    assertEquals(0, processed.get());
  }

  @Test
  void replaysCommittedResultsAfterStateCommitFailureWithoutRepeatingSideEffects() {
    final var transaction = new FailingCommitTransaction();
    final var failedStore = new InMemoryStore(transaction);
    final var log = new ArrayList<>(commands(1));
    final var appender = new CapturingAppender(log, initialAppendPosition(log));
    final var context = context(failedStore, appender);
    final AtomicInteger responses = new AtomicInteger();
    final AtomicInteger actions = new AtomicInteger();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    failedStore, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    response -> responses.incrementAndGet(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new ResponseAndPostCommitProcessor(context, actions::incrementAndGet)),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertTrue(transaction.rolledBack);
    assertEquals(2, log.size());
    assertEquals(0, responses.get());
    assertEquals(0, actions.get());

    final var recoveredStore = new InMemoryStore();
    replay(recoveredStore, log);
    final var recovered = recoveredStore.snapshot();
    assertEquals(1L, recoveredStore.getLastProcessedPosition());
    assertEquals(
        1, recovered.entrySet().stream().filter(e -> e.getKey().startsWith("JOBS:")).count());

    replay(recoveredStore, log);
    assertEquals(recovered, recoveredStore.snapshot());
    assertEquals(0, responses.get());
    assertEquals(0, actions.get());
  }

  @Test
  void continuesAfterIndividualPostCommitFailuresWithoutRollingBackState() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var context =
        context(store, (entries, sourcePosition) -> CompletableActorFuture.completed(2L));
    final var log = new ArrayList<>(commands(1));
    final AtomicInteger completedActions = new AtomicInteger();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    response -> {
                      throw new IllegalStateException("response delivery failed");
                    },
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            TypedRecordProcessors.processors()
                .onCommand(
                    ValueType.JOB,
                    JobIntent.COMPLETE,
                    new ResponseAndPostCommitProcessor(
                        context,
                        () -> {
                          throw new IllegalStateException("post-commit failed");
                        },
                        completedActions::incrementAndGet)),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertTrue(transaction.committed);
    assertFalse(transaction.rolledBack);
    assertEquals(1, completedActions.get());
  }

  @Test
  void recordsCommittedEventsAsSkippedDuringProcessing() {
    final var store = new InMemoryStore();
    final var context = context(store, noopAppender());
    final var log =
        new ArrayList<>(
            List.of(createdEvent(1L, 1L, Protocol.encodePartitionId(PARTITION_ID, 1L))));
    final var metrics = new RecordingProcessingMetrics();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors(),
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertEquals(1, metrics.skipped.get());
  }

  @Test
  void boundsFastDrainByTheConfiguredRecordBudget() {
    final var store = new InMemoryStore();
    final var context = context(store, noopAppender());
    final var log =
        new ArrayList<>(
            List.of(
                createdEvent(1L, 1L, Protocol.encodePartitionId(PARTITION_ID, 1L)),
                createdEvent(2L, 2L, Protocol.encodePartitionId(PARTITION_ID, 2L)),
                createdEvent(3L, 3L, Protocol.encodePartitionId(PARTITION_ID, 3L))));
    final var metrics = new RecordingProcessingMetrics();
    final var machine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)),
            TypedRecordProcessors.processors(),
            new StreamProcessorContext.Batching(1, 2, Duration.ofSeconds(1), Integer.MAX_VALUE));

    machine.startProcessing(0);

    assertEquals(3, metrics.skipped.get());
    assertEquals(2, control.runCalls.get());
  }

  @Test
  void shouldRunScheduledReadsOnlyOutsidePendingCommandTransaction() {
    final var transaction = new TrackingTransaction();
    final var store = new InMemoryStore(transaction);
    final var append = new CompletableActorFuture<Long>();
    final var context = context(store, (entry, source) -> append);
    final var log = new ArrayList<>(commands(1));
    final var machine = machine(store, context, log);
    machine.startProcessing(0);
    final var scans = new AtomicLong();
    machine.setCommitBoundaryTask(
        () -> {
          assertTrue(transaction.committed);
          scans.incrementAndGet();
        });
    machine.tryToReadNextRecord();
    assertEquals(0, scans.get());
    append.complete(2L);
    assertEquals(1, scans.get());
    assertEquals(1, machine.getLastSuccessfulProcessedRecordPosition());
  }

  @Test
  void shouldCancelReplayRetryAndIgnoreAlreadyQueuedWakeupOnClose() {
    final var log = new ArrayList<>(List.of(createdEvent(1, 1, 1)));
    final var reader = new VisibilityDelayedReader(log);
    final var store = new InMemoryStore();
    final var replay =
        new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(
                    eventRegistry, context(store, noopAppender())),
                new StreamProcessorContext.State(store, () -> 1, ignored -> {}),
                new StreamStateMachineContext.Runtime(control, reader),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)));
    final var recovery = replay.startRecover(0);
    assertEquals(1, control.scheduled.size());
    replay.close();
    reader.reveal();
    replay.replayNextEvent();
    assertTrue(control.scheduled.isEmpty());
    assertTrue(recovery.isDone());
    assertTrue(store.snapshot().isEmpty());
  }

  private ProcessingStateMachine machine(
      final InMemoryStore store,
      final TypedRecordProcessorContext context,
      final List<LoggedEvent> log) {
    return new ProcessingStateMachine(
        new StreamStateMachineContext(
            new StreamStateMachineContext.Identity(PARTITION_ID, StreamProcessorMode.PROCESSING),
            new StreamStateMachineContext.Processing(eventRegistry, context),
            new StreamProcessorContext.State(store, () -> committedPosition(log), ignored -> {}),
            new StreamStateMachineContext.Runtime(control, new ListReader(log)),
            new StreamStateMachineContext.Observers(
                CommandFailureObserver.noop(),
                CommandResponseObserver.noop(),
                CommittedRecordObserver.noop(),
                ProcessingMetrics.NOOP)),
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB,
                JobIntent.COMPLETE,
                new CreateProcessor(context.getKeyGenerator(), context.getWriters())),
        new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));
  }

  private TypedRecordProcessors chainedProcessors(final TypedRecordProcessorContext context) {
    return TypedRecordProcessors.processors()
        .onCommand(
            ValueType.JOB,
            JobIntent.COMPLETE,
            new ChainedCreateProcessor(context.getKeyGenerator(), context.getWriters()))
        .onCommand(ValueType.JOB, JobIntent.FAIL, new ActivateProcessor(context.getWriters()));
  }

  @Test
  void processingRetriesWhenCommittedRecordIsNotYetVisible() {
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final VisibilityDelayedReader reader = new VisibilityDelayedReader(log);
    final InMemoryStore store = new InMemoryStore();
    final AtomicLong appliedPosition = new AtomicLong(-1);
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(store, appender);
    final TypedRecordProcessors processors =
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB,
                JobIntent.COMPLETE,
                new CreateProcessor(context.getKeyGenerator(), context.getWriters()));
    final ProcessingStateMachine stateMachine =
        new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), appliedPosition::set),
                new StreamStateMachineContext.Runtime(control, reader),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            processors,
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE));

    stateMachine.startProcessing(0L);
    assertEquals(0L, store.getLastProcessedPosition());
    assertEquals(-1L, appliedPosition.get());

    reader.reveal();
    control.runNextScheduled();

    assertEquals(1L, store.getLastProcessedPosition());
    assertEquals(2L, appliedPosition.get());
  }

  @Test
  void replayRetriesWhenCommittedRecordIsNotYetVisible() {
    final List<LoggedEvent> log =
        new ArrayList<>(
            List.of(createdEvent(1L, 1L, Protocol.encodePartitionId(PARTITION_ID, 1L))));
    final VisibilityDelayedReader reader = new VisibilityDelayedReader(log);
    final InMemoryStore store = new InMemoryStore();
    final AtomicLong appliedPosition = new AtomicLong(-1);
    final TypedRecordProcessorContext context = context(store, noopAppender());
    final RecordingProcessingMetrics metrics = new RecordingProcessingMetrics();
    final ReplayStateMachine stateMachine =
        new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(PARTITION_ID, StreamProcessorMode.REPLAY),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), appliedPosition::set),
                new StreamStateMachineContext.Runtime(control, reader),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    metrics)));

    stateMachine.startRecover(0L);
    assertTrue(store.snapshot().isEmpty());
    assertEquals(-1L, appliedPosition.get());
    assertEquals(0L, metrics.processedPosition.get());
    assertEquals(1L, metrics.committedPosition.get());

    reader.reveal();
    control.runNextScheduled();

    assertEquals(
        Protocol.encodePartitionId(PARTITION_ID, 1L),
        Long.parseLong(store.snapshot().get("KEY:" + LATEST_KEY)));
    assertEquals(1L, appliedPosition.get());
    assertEquals(1L, metrics.processedPosition.get());
    assertEquals(1L, metrics.committedPosition.get());
  }

  @Test
  void replayCommitsAllEventsFromTheSameSourceAtomically() {
    final long firstKey = Protocol.encodePartitionId(PARTITION_ID, 1L);
    final long secondKey = Protocol.encodePartitionId(PARTITION_ID, 2L);
    final List<LoggedEvent> log =
        List.of(createdEvent(2L, 1L, firstKey), createdEvent(3L, 1L, secondKey));
    final TrackingTransaction transaction = new TrackingTransaction();
    final InMemoryStore store = new InMemoryStore(transaction);
    final TypedRecordProcessorContext context = context(store, noopAppender());

    new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(PARTITION_ID, StreamProcessorMode.REPLAY),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)))
        .startRecover(0L);

    assertEquals(1, transaction.commitCalls);
    assertEquals(1L, store.getLastProcessedPosition());
    assertEquals("payment|worker-a|t", store.snapshot().get("JOBS:" + firstKey));
    assertEquals("payment|worker-a|t", store.snapshot().get("JOBS:" + secondKey));
  }

  @Test
  void replayContinuesWhenCommandResponseDeliveryFailsAfterCommit() {
    final List<LoggedEvent> log =
        new ArrayList<>(
            List.of(
                createdEvent(1L, 1L, Protocol.encodePartitionId(PARTITION_ID, 1L)),
                createdEvent(2L, 2L, Protocol.encodePartitionId(PARTITION_ID, 2L))));
    final InMemoryStore store = new InMemoryStore();
    final AtomicInteger responseAttempts = new AtomicInteger();
    final AtomicInteger committedObservations = new AtomicInteger();
    final AtomicInteger failures = new AtomicInteger();
    final var replay =
        new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(
                    eventRegistry, context(store, noopAppender())),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    (requestId, error) -> failures.incrementAndGet(),
                    response -> {
                      if (responseAttempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("response delivery failed");
                      }
                    },
                    new CommittedRecordObserver() {
                      @Override
                      public void onProcessed(final LoggedEvent record) {
                        committedObservations.incrementAndGet();
                      }

                      @Override
                      public void onFailure(final LoggedEvent record, final Throwable error) {}
                    },
                    ProcessingMetrics.NOOP)));

    final ActorFuture<Long> recovery = replay.startRecover(0);

    assertTrue(recovery.isDone());
    assertFalse(recovery.isCompletedExceptionally());
    assertEquals(2L, recovery.join());
    assertEquals(2, responseAttempts.get());
    assertEquals(2, committedObservations.get());
    assertEquals(0, failures.get());
    assertEquals(
        2,
        store.snapshot().entrySet().stream().filter(e -> e.getKey().startsWith("JOBS:")).count());
  }

  // ------------------------------------------------------------------------------------------
  // Negative: a processor that mutates state directly (bypassing events) breaks equivalence.
  // ------------------------------------------------------------------------------------------

  @Test
  void equivalenceCheckCatchesStateWrittenOutsideAnEvent() {
    // The leader uses a rogue processor that writes an extra business row directly, with no
    // matching
    // event. Replay (which only applies events) can never reproduce that row, so the snapshots must
    // differ — the guardrail fires.
    final List<LoggedEvent> log = new ArrayList<>(commands(1));
    final InMemoryStore leaderStore = new InMemoryStore();
    process(leaderStore, log, /* rogue= */ true);

    final InMemoryStore followerStore = new InMemoryStore();
    replay(followerStore, log);

    assertTrue(
        leaderStore.snapshot().containsKey("JOBS:" + Long.MAX_VALUE),
        "rogue processor must have written the out-of-band row on the leader");
    assertNotEquals(
        leaderStore.snapshot(),
        followerStore.snapshot(),
        "state written outside an event cannot be rebuilt by replay — equivalence must fail");
  }

  // ------------------------------------------------------------------------------------------
  // Drivers
  // ------------------------------------------------------------------------------------------

  /** Runs a leader over {@code log}, appending each follow-up event back into {@code log}. */
  private void process(
      final InMemoryStore store, final List<LoggedEvent> log, final boolean rogue) {
    final CapturingAppender appender = new CapturingAppender(log, initialAppendPosition(log));
    final TypedRecordProcessorContext context = context(store, appender);
    final TypedRecordProcessors processors =
        TypedRecordProcessors.processors()
            .onCommand(
                ValueType.JOB,
                JobIntent.COMPLETE,
                rogue
                    ? new RogueCreateProcessor(context)
                    : new CreateProcessor(context.getKeyGenerator(), context.getWriters()));
    new ProcessingStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(
                    PARTITION_ID, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)),
            processors,
            new StreamProcessorContext.Batching(1, 0, Duration.ZERO, Integer.MAX_VALUE))
        .startProcessing(0L);
  }

  /** Runs a follower that replays {@code log} into {@code store}. */
  private void replay(final InMemoryStore store, final List<LoggedEvent> log) {
    // The follower never appends; a no-op appender satisfies the writer wiring it does not use.
    final TypedRecordProcessorContext context = context(store, noopAppender());
    new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(PARTITION_ID, StreamProcessorMode.REPLAY),
                new StreamStateMachineContext.Processing(eventRegistry, context),
                new StreamProcessorContext.State(
                    store, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)))
        .startRecover(0L);
  }

  /**
   * Builds a processor context whose writers eager-apply through {@link CreatedApplier} and recover
   * the key high-water, exactly like production ({@code StreamProcessor.createCommandWriters}).
   */
  private TypedRecordProcessorContext context(
      final InMemoryStore store, final RecordAppender appender) {
    return context(store, appender, Integer.MAX_VALUE, CommandFailureObserver.noop());
  }

  @Test
  void externalPdAcknowledgementDoesNotAdvancePartitionNineDuringAppendOrReplay() {
    final long pdKey = Protocol.encodeKey(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, 500);
    assertEquals(0, Protocol.decodeClusterKey(pdKey));
    assertEquals(1, Protocol.decodePartitionId(pdKey));
    final var registry =
        TypedEventRegistry.builder()
            .register(ValueType.COMMAND_DISTRIBUTION, CommandDistributionRecord::new)
            .build();
    final var appliers =
        new EventAppliers()
            .register(CommandDistributionIntent.ACKNOWLEDGED, EventAppliers.NOOP_EVENT_APPLIER);
    final var store = new InMemoryStore();
    final var keys = new PersistedKeyGenerator(1, 9, store);
    final var log = new ArrayList<LoggedEvent>();
    final var writers =
        new DirectWriters(
            new CapturingAppender(log, 1), CommandFailureObserver.noop(), appliers, keys, control);
    final var value = new CommandDistributionRecord<>().setDistributionKey(pdKey).setPartitionId(9);
    final var metadata =
        new RecordMetadata()
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.COMMAND_DISTRIBUTION)
            .intent(CommandDistributionIntent.ACKNOWLEDGE);
    final var command = new TypedRecordImpl(9);
    command.wrap(
        LoggedEvent.ofRecord(Protocol.encodeKey(1, 9, 0), metadata, value), metadata, value);
    writers.beginBatch(command);
    writers.state().appendFollowUpEvent(pdKey, CommandDistributionIntent.ACKNOWLEDGED, value);
    writers.awaitAppends().join();
    assertEquals(Protocol.encodeKey(1, 9, 1), keys.nextKey());

    final var follower = new InMemoryStore();
    final var recoveredKeys = new PersistedKeyGenerator(1, 9, follower);
    final var replayContext =
        new TypedRecordProcessorContextImpl(9, recoveredKeys, writers, follower, appliers);
    final var replay =
        new ReplayStateMachine(
            new StreamStateMachineContext(
                new StreamStateMachineContext.Identity(9, StreamProcessorMode.PROCESSING),
                new StreamStateMachineContext.Processing(registry, replayContext),
                new StreamProcessorContext.State(
                    follower, () -> committedPosition(log), ignored -> {}),
                new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                new StreamStateMachineContext.Observers(
                    CommandFailureObserver.noop(),
                    CommandResponseObserver.noop(),
                    CommittedRecordObserver.noop(),
                    ProcessingMetrics.NOOP)));
    replay.startRecover(0).join();
    assertEquals(Protocol.encodeKey(1, 9, 1), recoveredKeys.nextKey());
  }

  private TypedRecordProcessorContext context(
      final InMemoryStore store,
      final RecordAppender appender,
      final int maxBatchBytes,
      final CommandFailureObserver failureObserver) {
    final KeyGenerator keyGenerator = new PersistedKeyGenerator(0, PARTITION_ID, store);
    final EventAppliers appliers =
        new EventAppliers().register(JobIntent.CREATED, new CreatedApplier(store));
    final Writers writers =
        new DirectWriters(
            appender, failureObserver, appliers, keyGenerator, maxBatchBytes, control);
    return new TypedRecordProcessorContextImpl(
        PARTITION_ID, keyGenerator, writers, store, appliers);
  }

  private static long initialAppendPosition(final List<LoggedEvent> log) {
    return committedPosition(log) + 1;
  }

  private static long committedPosition(final List<LoggedEvent> log) {
    long max = 0;
    for (final LoggedEvent event : log) {
      max = Math.max(max, event.getPosition());
    }
    return max;
  }

  private RecordAppender noopAppender() {
    return (entries, sourcePosition) -> CompletableActorFuture.completed(-1L);
  }

  // ------------------------------------------------------------------------------------------
  // Layer-A domain: "create entity with generated key" command + its applier.
  // ------------------------------------------------------------------------------------------

  /** Mints a key and emits a CREATED event — the only way it touches state. */
  private static final class CreateProcessor implements TypedRecordProcessor<JobRecord> {
    private final KeyGenerator keyGenerator;
    private final Writers writers;

    CreateProcessor(final KeyGenerator keyGenerator, final Writers writers) {
      this.keyGenerator = keyGenerator;
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      final long key = keyGenerator.nextKey();
      writers.state().appendFollowUpEvent(key, JobIntent.CREATED, copy(record.getValue()));
    }
  }

  private static final class ChainedCreateProcessor implements TypedRecordProcessor<JobRecord> {
    private final KeyGenerator keyGenerator;
    private final Writers writers;

    private ChainedCreateProcessor(final KeyGenerator keyGenerator, final Writers writers) {
      this.keyGenerator = keyGenerator;
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      final long key = keyGenerator.nextKey();
      final JobRecord created = copy(record.getValue());
      writers.state().appendFollowUpEvent(key, JobIntent.CREATED, created);
      writers.command().appendFollowUpCommand(key, JobIntent.FAIL, created);
    }
  }

  private static final class ActivateProcessor implements TypedRecordProcessor<JobRecord> {
    private final Writers writers;

    private ActivateProcessor(final Writers writers) {
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      final JobRecord activated = copy(record.getValue()).setWorker("batch-worker");
      writers.state().appendFollowUpEvent(record.getKey(), JobIntent.CREATED, activated);
    }
  }

  private static final class FanOutProcessor implements TypedRecordProcessor<JobRecord> {
    private final Writers writers;

    private FanOutProcessor(final Writers writers) {
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      writers.command().appendFollowUpCommand(11L, JobIntent.FAIL, job());
      writers.command().appendFollowUpCommand(22L, JobIntent.FAIL, job());
    }
  }

  private static final class SmallFollowUpProcessor implements TypedRecordProcessor<JobRecord> {
    private final Writers writers;

    private SmallFollowUpProcessor(final Writers writers) {
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      writers.command().appendFollowUpCommand(11L, JobIntent.FAIL, job());
    }
  }

  private static final class OversizedFollowUpProcessor implements TypedRecordProcessor<JobRecord> {
    private final Writers writers;

    private OversizedFollowUpProcessor(final Writers writers) {
      this.writers = writers;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      writers
          .command()
          .appendFollowUpCommand(
              22L,
              JobIntent.COMPLETE,
              new JobRecord().setJobType("oversized").setVariablesJson("x".repeat(4096)));
    }
  }

  private static final class RecordingActivateProcessor implements TypedRecordProcessor<JobRecord> {
    private final List<Long> processedKeys;

    private RecordingActivateProcessor(final List<Long> processedKeys) {
      this.processedKeys = processedKeys;
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      processedKeys.add(record.getKey());
    }
  }

  private static final class ResponseAndPostCommitProcessor
      implements TypedRecordProcessor<JobRecord> {
    private final Writers writers;
    private final KeyGenerator keyGenerator;
    private final List<Runnable> postCommitActions;

    private ResponseAndPostCommitProcessor(
        final TypedRecordProcessorContext context, final Runnable... postCommitActions) {
      writers = context.getWriters();
      keyGenerator = context.getKeyGenerator();
      this.postCommitActions = List.of(postCommitActions);
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      final long key = keyGenerator.nextKey();
      writers.state().appendFollowUpEvent(key, JobIntent.CREATED, record.getValue());
      writers.response().writeEventOnCommand(key, JobIntent.CREATED, record.getValue(), record);
      postCommitActions.forEach(writers.postCommit()::append);
    }
  }

  /** Emits a proper event AND illegally writes an extra row straight into state. */
  private static final class RogueCreateProcessor implements TypedRecordProcessor<JobRecord> {
    private final KeyGenerator keyGenerator;
    private final Writers writers;
    private final InMemoryStore store;

    RogueCreateProcessor(final TypedRecordProcessorContext context) {
      keyGenerator = context.getKeyGenerator();
      writers = context.getWriters();
      store = (InMemoryStore) context.getProcessingState();
    }

    @Override
    public void processRecord(final TypedRecord<JobRecord> record) {
      final long key = keyGenerator.nextKey();
      writers.state().appendFollowUpEvent(key, JobIntent.CREATED, copy(record.getValue()));
      // Illegal: mutate business state directly, with no event to replay.
      store.putJob(Long.MAX_VALUE, (JobRecord) record.getValue());
    }
  }

  private static final class CreatedApplier implements TypedEventApplier<JobIntent, JobRecord> {
    private final InMemoryStore store;

    CreatedApplier(final InMemoryStore store) {
      this.store = store;
    }

    @Override
    public void applyState(final long key, final JobRecord value) {
      store.putJob(key, value);
    }
  }

  private static final class RecordingProcessingMetrics implements ProcessingMetrics {
    private final AtomicInteger processed = new AtomicInteger();
    private final AtomicInteger skipped = new AtomicInteger();
    private final AtomicInteger written = new AtomicInteger();
    private final AtomicInteger batches = new AtomicInteger();
    private final AtomicInteger batchCommands = new AtomicInteger();
    private final AtomicInteger continuations = new AtomicInteger();
    private final AtomicInteger recordLimitContinuations = new AtomicInteger();
    private final AtomicInteger timeLimitContinuations = new AtomicInteger();
    private final AtomicInteger retries = new AtomicInteger();
    private final AtomicLong batchDurationNanos = new AtomicLong();
    private final AtomicLong processedPosition = new AtomicLong();
    private final AtomicLong committedPosition = new AtomicLong();

    @Override
    public void onProcessed(final List<LoggedEvent> writtenRecords) {
      processed.incrementAndGet();
      written.addAndGet(writtenRecords.size());
    }

    @Override
    public void onBatchProcessed(
        final int commandCount,
        final int commandLimitContinuationCount,
        final int recordLimitContinuationCount,
        final int timeLimitContinuationCount,
        final long durationNanos) {
      batches.incrementAndGet();
      batchCommands.addAndGet(commandCount);
      continuations.addAndGet(
          commandLimitContinuationCount
              + recordLimitContinuationCount
              + timeLimitContinuationCount);
      recordLimitContinuations.addAndGet(recordLimitContinuationCount);
      timeLimitContinuations.addAndGet(timeLimitContinuationCount);
      batchDurationNanos.addAndGet(durationNanos);
    }

    @Override
    public void onBatchRetried() {
      retries.incrementAndGet();
    }

    @Override
    public void onSkipped() {
      skipped.incrementAndGet();
    }

    @Override
    public void onRecordPositions(
        final long processedRecordPosition, final long committedRecordPosition) {
      processedPosition.set(Math.max(0, processedRecordPosition));
      committedPosition.set(Math.max(0, committedRecordPosition));
    }
  }

  private static JobRecord copy(final UnifiedRecordValue value) {
    final JobRecord source = (JobRecord) value;
    return new JobRecord()
        .setJobType(source.getType())
        .setWorker(source.getWorker())
        .setTenantId(source.getTenantId())
        .setVariablesJson(source.getVariablesJson());
  }

  // ------------------------------------------------------------------------------------------
  // Log helpers
  // ------------------------------------------------------------------------------------------

  private List<LoggedEvent> commands(final int count) {
    final List<LoggedEvent> commands = new ArrayList<>(count);
    for (int i = 1; i <= count; i++) {
      commands.add(command(i, 0));
    }
    return commands;
  }

  private LoggedEvent command(final long position, final long timestamp) {
    return LoggedEvent.of(
        LogAppendEntry.of(
            LogAppendEntry.KEY_NULL_VALUE,
            metadata().recordType(RecordType.COMMAND).intent(JobIntent.COMPLETE),
            job()),
        position,
        -1,
        timestamp);
  }

  private static RecordMetadata metadata(final LoggedEvent event) {
    return event.readMetadata(new RecordMetadata());
  }

  private LoggedEvent createdEvent(final long position, final long sourcePosition, final long key) {
    return LoggedEvent.of(
        LogAppendEntry.of(
            key, metadata().recordType(RecordType.EVENT).intent(JobIntent.CREATED), job()),
        position,
        sourcePosition,
        0);
  }

  private static RecordMetadata metadata() {
    return new RecordMetadata().requestStreamId("0").requestId("req").valueType(ValueType.JOB);
  }

  private static JobRecord job() {
    return new JobRecord().setJobType("payment").setWorker("worker-a").setTenantId("t");
  }

  /**
   * Synchronous {@link ConcurrencyControl}: runs actions inline so a pass completes in the call.
   */
  private static final class DirectConcurrencyControl implements ConcurrencyControl {
    private final Deque<Runnable> scheduled = new ArrayDeque<>();
    private final AtomicInteger runCalls = new AtomicInteger();

    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.onComplete(callback, Runnable::run);
    }

    @Override
    public void run(final Runnable action) {
      runCalls.incrementAndGet();
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      try {
        return CompletableActorFuture.completed(callable.call());
      } catch (final Throwable error) {
        return CompletableActorFuture.completedExceptionally(error);
      }
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      scheduled.addLast(runnable);
      return () -> scheduled.remove(runnable);
    }

    private void runNextScheduled() {
      scheduled.removeFirst().run();
    }
  }

  /** Reads a committed log in position order. */
  private static final class ListReader implements LogStreamReader {
    private final List<LoggedEvent> records;
    private int nextIndex;

    ListReader(final List<LoggedEvent> records) {
      this.records = records;
    }

    @Override
    public void seekToNextEvent(final long position) {
      seek(position + 1);
    }

    @Override
    public void seek(final long position) {
      nextIndex = 0;
      while (nextIndex < records.size() && records.get(nextIndex).getPosition() < position) {
        nextIndex++;
      }
    }

    @Override
    public long getNextPosition() {
      return hasNext() ? records.get(nextIndex).getPosition() : Long.MAX_VALUE;
    }

    @Override
    public boolean hasNext() {
      return nextIndex < records.size();
    }

    @Override
    public LoggedEvent next() {
      return records.get(nextIndex++);
    }

    @Override
    public void close() {}
  }

  private static final class VisibilityDelayedReader implements LogStreamReader {
    private final ListReader delegate;
    private boolean visible;

    private VisibilityDelayedReader(final List<LoggedEvent> records) {
      delegate = new ListReader(records);
    }

    private void reveal() {
      visible = true;
    }

    @Override
    public void seekToNextEvent(final long position) {
      delegate.seekToNextEvent(position);
    }

    @Override
    public void seek(final long position) {
      delegate.seek(position);
    }

    @Override
    public long getNextPosition() {
      return delegate.hasNext()
          ? delegate.records.get(delegate.nextIndex).getPosition()
          : Long.MAX_VALUE;
    }

    @Override
    public boolean hasNext() {
      return visible && delegate.hasNext();
    }

    @Override
    public LoggedEvent next() {
      return delegate.next();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  /**
   * Appender for the leader pass: turns each eager follow-up into a committed byte-backed {@link
   * LoggedEvent} appended to the shared {@code log}, positioned after the commands, so a follower
   * can later replay exactly what the leader wrote.
   */
  private static final class CapturingAppender implements RecordAppender {
    private final List<LoggedEvent> log;
    private final AtomicLong nextPosition;
    private final AtomicInteger appendCalls = new AtomicInteger();

    CapturingAppender(final List<LoggedEvent> log, final long firstPosition) {
      this.log = log;
      nextPosition = new AtomicLong(firstPosition);
    }

    @Override
    public ActorFuture<Long> append(final List<LogAppendEntry> entries, final long sourcePosition) {
      appendCalls.incrementAndGet();
      long highestPosition = -1L;
      for (final LogAppendEntry entry : entries) {
        highestPosition = nextPosition.getAndIncrement();
        log.add(LoggedEvent.of(entry, highestPosition, sourcePosition, 0));
      }
      return CompletableActorFuture.completed(highestPosition);
    }

    private int appendCalls() {
      return appendCalls.get();
    }
  }

  // ------------------------------------------------------------------------------------------
  // In-memory ProcessingStateStore
  // ------------------------------------------------------------------------------------------

  /**
   * Map-backed {@link ProcessingStateStore} exposing a readable {@code snapshot()} of the KEY and
   * JOBS column families for equivalence assertions. Values round-trip through the same {@code
   * write}/{@code wrap} contract as RocksDB so the stored bytes match production.
   */
  private static final class InMemoryStore implements ProcessingStateStore {
    private final Map<ColumnFamilies, InMemoryColumnFamily<?, ?>> families = new HashMap<>();
    private final DbTransaction transaction;
    private long lastProcessedPosition;

    private InMemoryStore() {
      this(NoopTransaction.INSTANCE);
    }

    private InMemoryStore(final DbTransaction transaction) {
      this.transaction = transaction;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <KeyType extends DbKey, ValueType extends DbValue>
        ColumnFamily<KeyType, ValueType> createColumnFamily(
            final ColumnFamilies columnFamily,
            final KeyType keyInstance,
            final ValueType valueInstance) {
      return (ColumnFamily<KeyType, ValueType>)
          families.computeIfAbsent(columnFamily, cf -> new InMemoryColumnFamily<>(valueInstance));
    }

    void putJob(final long key, final JobRecord value) {
      final ColumnFamily<DbLong, DbString> jobs =
          createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final DbLong dbKey = new DbLong();
      dbKey.wrapLong(key);
      final DbString summary = new DbString();
      summary.wrapString(value.getType() + "|" + value.getWorker() + "|" + value.getTenantId());
      jobs.upsert(dbKey, summary);
    }

    /** A stable, human-readable view of the state that must match across processing and replay. */
    Map<String, String> snapshot() {
      final Map<String, String> out = new TreeMap<>();
      final InMemoryColumnFamily<?, ?> key = families.get(ColumnFamilies.KEY);
      if (key != null) {
        key.forEachRaw((k, v) -> out.put("KEY:" + asString(k), Long.toString(asLong(v))));
      }
      final InMemoryColumnFamily<?, ?> jobs = families.get(ColumnFamilies.JOBS);
      if (jobs != null) {
        jobs.forEachRaw((k, v) -> out.put("JOBS:" + asLong(k), asString(v)));
      }
      return out;
    }

    private static long asLong(final byte[] bytes) {
      final DbLong v = new DbLong();
      v.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      return v.getValue();
    }

    private static String asString(final byte[] bytes) {
      final DbString v = new DbString();
      v.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
      return v.toString();
    }

    @Override
    public long getLastProcessedPosition() {
      return lastProcessedPosition;
    }

    @Override
    public void markAsProcessed(final long position) {
      lastProcessedPosition = Math.max(lastProcessedPosition, position);
    }

    @Override
    public DbTransaction getCurrentTransaction() {
      return transaction;
    }

    @Override
    public void close() {}
  }

  private static final class TrackingTransaction implements DbTransaction {
    private boolean committed;
    private boolean rolledBack;
    private int commitCalls;

    @Override
    public void run(final TransactionOperation operations) throws Exception {
      operations.run();
    }

    @Override
    public void commit() {
      committed = true;
      commitCalls++;
    }

    @Override
    public void rollback() {
      rolledBack = true;
    }
  }

  private static final class FailingCommitTransaction implements DbTransaction {
    private boolean rolledBack;

    @Override
    public void run(final TransactionOperation operations) throws Exception {
      operations.run();
    }

    @Override
    public void commit() {
      throw new IllegalStateException("commit failed");
    }

    @Override
    public void rollback() {
      rolledBack = true;
    }
  }

  private static final class InMemoryColumnFamily<K extends DbKey, V extends DbValue>
      implements ColumnFamily<K, V> {
    private final V valueInstance;
    private final Map<BufferKey, byte[]> entries = new HashMap<>();

    InMemoryColumnFamily(final V valueInstance) {
      this.valueInstance = valueInstance;
    }

    void forEachRaw(final BiConsumer<byte[], byte[]> consumer) {
      entries.forEach((k, v) -> consumer.accept(k.bytes, v));
    }

    @Override
    public void insert(final K key, final V value) {
      upsert(key, value);
    }

    @Override
    public void update(final K key, final V value) {
      upsert(key, value);
    }

    @Override
    public void upsert(final K key, final V value) {
      entries.put(BufferKey.of(key), serialize(value));
    }

    @Override
    public V get(final K key) {
      final byte[] bytes = entries.get(BufferKey.of(key));
      if (bytes == null) {
        return null;
      }
      wrap(valueInstance, bytes);
      return valueInstance;
    }

    @Override
    public V get(final K key, final Supplier<V> valueSupplier) {
      final byte[] bytes = entries.get(BufferKey.of(key));
      if (bytes == null) {
        return null;
      }
      final V v = valueSupplier.get();
      wrap(v, bytes);
      return v;
    }

    @Override
    public void forEach(final Consumer<V> consumer) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void forEach(final BiConsumer<K, V> consumer) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void whileTrue(final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void whileTrue(final K startAtKey, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void whileEqualPrefix(final DbKey keyPrefix, final BiConsumer<K, V> visitor) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void whileEqualPrefix(final DbKey keyPrefix, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void whileEqualPrefix(
        final DbKey keyPrefix, final K startAtKey, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by this test");
    }

    @Override
    public void deleteExisting(final K key) {
      entries.remove(BufferKey.of(key));
    }

    @Override
    public void deleteIfExists(final K key) {
      entries.remove(BufferKey.of(key));
    }

    @Override
    public boolean exists(final K key) {
      return entries.containsKey(BufferKey.of(key));
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public long count() {
      return entries.size();
    }

    @Override
    public long countEqualPrefix(final DbKey prefix) {
      throw new UnsupportedOperationException("not used by this test");
    }

    private static byte[] serialize(final DbValue value) {
      final byte[] out = new byte[value.getLength()];
      value.write(new UnsafeBuffer(out), 0);
      return out;
    }

    private static void wrap(final DbValue value, final byte[] bytes) {
      value.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
    }
  }

  /** Value-object key over a DbKey's serialized bytes, suitable for HashMap keys. */
  private static final class BufferKey {
    private final byte[] bytes;
    private final int hashCode;

    private BufferKey(final byte[] bytes) {
      this.bytes = bytes;
      int h = 1;
      for (final byte b : bytes) {
        h = 31 * h + b;
      }
      hashCode = h;
    }

    static BufferKey of(final DbKey key) {
      final byte[] out = new byte[key.getLength()];
      final MutableDirectBuffer buf = new UnsafeBuffer(out);
      key.write(buf, 0);
      return new BufferKey(out);
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof BufferKey other)) {
        return false;
      }
      if (bytes.length != other.bytes.length) {
        return false;
      }
      for (int i = 0; i < bytes.length; i++) {
        if (bytes[i] != other.bytes[i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
