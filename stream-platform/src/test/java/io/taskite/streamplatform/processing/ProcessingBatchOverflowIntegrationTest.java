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
import static org.junit.jupiter.api.Assertions.assertNull;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextImpl;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.api.state.EventAppliers;
import io.taskite.streamplatform.api.writer.Writers;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.DirectWriters;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ProcessingBatchOverflowIntegrationTest {
  private static final int PARTITION_ID = 1;

  @TempDir Path tempDirectory;

  @Test
  void rollsBackTheOverflowingCommandBeforeShrinkingAndRetrying() throws Exception {
    try (PartitionStorage partitionStorage = new PartitionStorage(tempDirectory)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(partitionStorage);
      final ColumnFamily<DbLong, DbString> jobs =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final EventAppliers appliers =
          new EventAppliers()
              .register(
                  JobIntent.CREATED,
                  (key, value) -> upsert(jobs, key, ((JobRecord) value).getWorker()));
      final List<LoggedEvent> log = new ArrayList<>(List.of(rootCommand()));
      final DirectControl control = new DirectControl();
      final AtomicInteger failures = new AtomicInteger();
      final AtomicInteger responses = new AtomicInteger();
      final AtomicInteger actions = new AtomicInteger();
      final CommandFailureObserver failureObserver =
          (requestId, error) -> failures.incrementAndGet();
      final DirectWriters writers =
          new DirectWriters(
              capturingAppender(log), failureObserver, appliers, () -> 1L, 2048, control);
      final var context =
          new TypedRecordProcessorContextImpl(PARTITION_ID, () -> 1L, writers, store, appliers);
      final Writers processorWriters = context.getWriters();
      final ProcessingStateMachine stateMachine =
          new ProcessingStateMachine(
              new StreamStateMachineContext(
                  new StreamStateMachineContext.Identity(
                      PARTITION_ID, StreamProcessorMode.PROCESSING),
                  new StreamStateMachineContext.Processing(eventRegistry(), context),
                  new StreamProcessorContext.State(store, () -> 1L, ignored -> {}),
                  new StreamStateMachineContext.Runtime(control, new ListReader(log)),
                  new StreamStateMachineContext.Observers(
                      failureObserver,
                      response -> responses.incrementAndGet(),
                      CommittedRecordObserver.noop(),
                      ProcessingMetrics.NOOP)),
              TypedRecordProcessors.processors()
                  .onCommand(
                      ValueType.JOB,
                      JobIntent.COMPLETE,
                      new TypedRecordProcessor<JobRecord>() {
                        @Override
                        public void processRecord(final TypedRecord<JobRecord> record) {
                          processorWriters
                              .state()
                              .appendFollowUpEvent(1L, JobIntent.CREATED, job("root-committed"));
                          processorWriters
                              .command()
                              .appendFollowUpCommand(2L, JobIntent.FAIL, job("continuation"));
                          processorWriters
                              .response()
                              .writeEventOnCommand(
                                  1L, JobIntent.CREATED, job("root-committed"), record);
                          processorWriters.postCommit().append(actions::incrementAndGet);
                        }
                      })
                  .onCommand(
                      ValueType.JOB,
                      JobIntent.FAIL,
                      new TypedRecordProcessor<JobRecord>() {
                        @Override
                        public void processRecord(final TypedRecord<JobRecord> record) {
                          processorWriters
                              .state()
                              .appendFollowUpEvent(2L, JobIntent.CREATED, job("must-roll-back"));
                          processorWriters
                              .command()
                              .appendFollowUpCommand(
                                  3L,
                                  JobIntent.COMPLETE,
                                  new JobRecord().setVariablesJson("x".repeat(4096)));
                        }
                      }),
              new StreamProcessorContext.Batching(2, 0, Duration.ZERO, Integer.MAX_VALUE));

      stateMachine.startProcessing(0L);

      assertEquals("root-committed", value(jobs, 1L));
      assertNull(value(jobs, 2L));
      assertEquals(1L, store.getLastProcessedPosition());
      assertEquals(3, log.size());
      assertEquals(RecordType.EVENT, metadata(log.get(1)).getRecordType());
      assertEquals(RecordType.COMMAND, metadata(log.get(2)).getRecordType());
      assertEquals(0, failures.get());
      assertEquals(1, responses.get());
      assertEquals(1, actions.get());
      store.getCurrentTransaction().rollback();
    }
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

  private static TypedEventRegistry eventRegistry() {
    return TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build();
  }

  private static JobRecord job(final String worker) {
    return new JobRecord().setJobType("test").setWorker(worker).setTenantId("tenant");
  }

  private static LoggedEvent rootCommand() {
    return LoggedEvent.of(
        LogAppendEntry.of(
            new RecordMetadata()
                .requestStreamId("stream")
                .requestId("request")
                .recordType(RecordType.COMMAND)
                .recordVersion(1)
                .valueType(ValueType.JOB)
                .intent(JobIntent.COMPLETE),
            job("request")),
        1L,
        -1L,
        100L);
  }

  private static RecordMetadata metadata(final LoggedEvent event) {
    return event.readMetadata(new RecordMetadata());
  }

  private static RecordAppender capturingAppender(final List<LoggedEvent> log) {
    return (entries, sourcePosition) -> {
      long position = log.size() + 1L;
      for (final LogAppendEntry entry : entries) {
        log.add(LoggedEvent.of(entry, position++, sourcePosition, 100L));
      }
      return CompletableActorFuture.completed(position - 1);
    };
  }

  private static final class ListReader implements LogStreamReader {
    private final List<LoggedEvent> log;
    private int cursor;

    private ListReader(final List<LoggedEvent> log) {
      this.log = log;
    }

    @Override
    public void seekToNextEvent(final long position) {
      cursor = 0;
      while (cursor < log.size() && log.get(cursor).getPosition() <= position) {
        cursor++;
      }
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
      return cursor < log.size() ? log.get(cursor).getPosition() : Long.MAX_VALUE;
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
