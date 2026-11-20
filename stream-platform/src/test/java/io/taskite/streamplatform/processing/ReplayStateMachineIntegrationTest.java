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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ReplayStateMachineIntegrationTest {
  private static final int PARTITION_ID = 1;

  @TempDir Path tempDirectory;

  @Test
  void rollsBackTheWholeSourceGroupWhenALaterEventFails() {
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final ColumnFamily<DbLong, DbString> jobs =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final EventAppliers appliers =
          new EventAppliers()
              .register(
                  JobIntent.CREATED,
                  (key, value) -> {
                    upsert(jobs, key, ((JobRecord) value).getWorker());
                    if (key == 2L) {
                      throw new IllegalStateException("second event failed");
                    }
                  });
      final DirectControl control = new DirectControl();
      final DirectWriters writers =
          new DirectWriters(
              (entries, sourcePosition) -> CompletableActorFuture.completed(-1L),
              CommandFailureObserver.noop(),
              appliers,
              () -> 1L,
              control);
      final var context =
          new TypedRecordProcessorContextImpl(PARTITION_ID, () -> 1L, writers, store, appliers);
      final List<LoggedEvent> log = List.of(createdEvent(2L, 1L, 1L), createdEvent(3L, 1L, 2L));
      final AtomicLong applied = new AtomicLong();
      final ReplayStateMachine replay =
          new ReplayStateMachine(
              new StreamStateMachineContext(
                  new StreamStateMachineContext.Identity(
                      PARTITION_ID, StreamProcessorMode.PROCESSING),
                  new StreamStateMachineContext.Processing(eventRegistry(), context),
                  new StreamProcessorContext.State(store, () -> 3L, applied::set),
                  new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                  new StreamStateMachineContext.Observers(
                      CommandFailureObserver.noop(),
                      CommandResponseObserver.noop(),
                      CommittedRecordObserver.noop(),
                      ProcessingMetrics.NOOP)));

      final ActorFuture<Long> recovery = replay.startRecover(0L);

      assertTrue(recovery.isCompletedExceptionally());
      assertNull(value(jobs, 1L));
      assertNull(value(jobs, 2L));
      assertEquals(0L, store.getLastProcessedPosition());
      assertEquals(0L, applied.get(), "a rolled-back result group must not advance readiness");
    }
  }

  @Test
  void sourceGroupTracksConsumedTailWithoutMovingDurableSourceCursor() {
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final ColumnFamily<DbLong, DbString> jobs =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final EventAppliers appliers =
          new EventAppliers()
              .register(
                  JobIntent.CREATED,
                  (key, record) -> upsert(jobs, key, ((JobRecord) record).getWorker()));
      final AtomicLong applied = new AtomicLong();
      final ReplayStateMachine replay =
          followerReplay(
              store,
              appliers,
              List.of(createdEvent(2L, 1L, 1L), createdEvent(3L, 1L, 2L)),
              applied);

      final ActorFuture<Long> recovery = replay.startRecover(0L);

      assertTrue(replay.hasCaughtUp());
      assertFalse(recovery.isDone(), "a caught-up follower keeps its replay lifecycle open");
      assertEquals("worker", value(jobs, 1L));
      assertEquals("worker", value(jobs, 2L));
      assertEquals(
          1L, store.getLastProcessedPosition(), "durable deduplication still uses the source");
      assertEquals(3L, applied.get(), "readiness must observe the fully applied result tail");
      replay.close();
    }
  }

  @Test
  void reopenedFollowerPublishesAlreadyAppliedResultTailWithoutReapplying() throws Exception {
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final ColumnFamily<DbLong, DbString> jobs =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      upsert(jobs, 1L, "worker");
      upsert(jobs, 2L, "worker");
      store.markAsProcessed(1L);
      store.getCurrentTransaction().commit();
    }
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final ColumnFamily<DbLong, DbString> jobs =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final AtomicInteger applications = new AtomicInteger();
      final EventAppliers appliers =
          new EventAppliers()
              .register(JobIntent.CREATED, (key, record) -> applications.incrementAndGet());
      final AtomicLong applied = new AtomicLong(store.getLastProcessedPosition());
      final ReplayStateMachine replay =
          followerReplay(
              store,
              appliers,
              List.of(createdEvent(2L, 1L, 1L), createdEvent(3L, 1L, 2L)),
              applied);

      replay.startRecover(store.getLastProcessedPosition());

      assertTrue(replay.hasCaughtUp());
      assertEquals(0, applications.get(), "durable result groups must not be applied twice");
      assertEquals("worker", value(jobs, 1L));
      assertEquals("worker", value(jobs, 2L));
      assertEquals(1L, store.getLastProcessedPosition());
      assertEquals(
          3L, applied.get(), "restart must recover the consumed tail from skipped results");
      replay.close();
    }
  }

  @Test
  void commandOnlyReplayDoesNotReportUnappliedBusinessProgress() {
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final LoggedEvent command =
          LoggedEvent.of(
              LogAppendEntry.of(
                  1L,
                  new RecordMetadata()
                      .recordType(RecordType.COMMAND)
                      .recordVersion(1)
                      .valueType(ValueType.JOB)
                      .intent(JobIntent.COMPLETE),
                  new JobRecord().setJobType("test")),
              2L,
              0L,
              100L);
      final AtomicLong applied = new AtomicLong();
      final ReplayStateMachine replay =
          followerReplay(store, new EventAppliers(), List.of(command), applied);

      replay.startRecover(0L);

      assertTrue(replay.hasCaughtUp());
      assertEquals(0L, store.getLastProcessedPosition());
      assertEquals(
          0L, applied.get(), "merely reading a command must not pretend its result is applied");
      replay.close();
    }
  }

  private static ReplayStateMachine followerReplay(
      final StateStoreProcessingStore store,
      final EventAppliers appliers,
      final List<LoggedEvent> log,
      final AtomicLong applied) {
    final DirectControl control = new DirectControl();
    final DirectWriters writers =
        new DirectWriters(
            (entries, sourcePosition) -> CompletableActorFuture.completed(-1L),
            CommandFailureObserver.noop(),
            appliers,
            () -> 1L,
            control);
    final var context =
        new TypedRecordProcessorContextImpl(PARTITION_ID, () -> 1L, writers, store, appliers);
    return new ReplayStateMachine(
        new StreamStateMachineContext(
            new StreamStateMachineContext.Identity(PARTITION_ID, StreamProcessorMode.REPLAY),
            new StreamStateMachineContext.Processing(eventRegistry(), context),
            new StreamProcessorContext.State(
                store, () -> log.getLast().getPosition(), applied::set),
            new StreamStateMachineContext.Runtime(control, new ListReader(log)),
            new StreamStateMachineContext.Observers(
                CommandFailureObserver.noop(),
                CommandResponseObserver.noop(),
                CommittedRecordObserver.noop(),
                ProcessingMetrics.NOOP)));
  }

  private static LoggedEvent createdEvent(
      final long position, final long sourcePosition, final long key) {
    return LoggedEvent.of(
        LogAppendEntry.of(
            key,
            new RecordMetadata()
                .requestStreamId("stream")
                .requestId("request")
                .recordType(RecordType.EVENT)
                .recordVersion(1)
                .valueType(ValueType.JOB)
                .intent(JobIntent.CREATED),
            new JobRecord().setJobType("test").setWorker("worker").setTenantId("tenant")),
        position,
        sourcePosition,
        100L);
  }

  private static TypedEventRegistry eventRegistry() {
    return TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build();
  }

  private static void upsert(
      final ColumnFamily<DbLong, DbString> jobs, final long key, final String value) {
    final DbLong dbKey = new DbLong();
    dbKey.wrapLong(key);
    final DbString dbValue = new DbString();
    dbValue.wrapString(value);
    jobs.upsert(dbKey, dbValue);
  }

  private static String value(final ColumnFamily<DbLong, DbString> jobs, final long key) {
    final DbLong dbKey = new DbLong();
    dbKey.wrapLong(key);
    final DbString value = jobs.get(dbKey, DbString::new);
    return value == null ? null : value.toString();
  }

  private static final class ListReader implements LogStreamReader {
    private final List<LoggedEvent> log;
    private int cursor;

    private ListReader(final List<LoggedEvent> log) {
      this.log = log;
    }

    @Override
    public void seekToNextEvent(final long position) {
      seek(position + 1);
    }

    @Override
    public void seek(final long position) {
      cursor = 0;
      while (cursor < log.size() && log.get(cursor).getPosition() < position) {
        cursor++;
      }
    }

    @Override
    public long getNextPosition() {
      return hasNext() ? log.get(cursor).getPosition() : Long.MAX_VALUE;
    }

    @Override
    public boolean hasNext() {
      return cursor < log.size();
    }

    @Override
    public LoggedEvent next() {
      return hasNext() ? log.get(cursor++) : null;
    }

    @Override
    public void close() {}
  }

  private static final class DirectControl implements ConcurrencyControl {
    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.whenComplete(callback);
    }

    @Override
    public void run(final Runnable action) {
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      try {
        return CompletableActorFuture.completed(callable.call());
      } catch (final Exception error) {
        return CompletableActorFuture.completedExceptionally(error);
      }
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      return () -> {};
    }
  }
}
