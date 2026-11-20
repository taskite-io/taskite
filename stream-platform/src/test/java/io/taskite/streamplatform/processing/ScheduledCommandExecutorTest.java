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

import static org.junit.jupiter.api.Assertions.*;

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
import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogBatchSizing;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

final class ScheduledCommandExecutorTest {
  @TempDir Path directory;

  @Test
  void shouldRegisterExternalTasksOnActorAndHandleCancellationBeforeRegistration()
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      actor.queueRuns = true;
      final var scans = new AtomicInteger();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entry, source) -> CompletableActorFuture.completed(1L),
              batching(2, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> 1,
              () -> {});
      final var thread =
          Thread.ofPlatform()
              .start(() -> executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet()));
      thread.join();
      assertTrue(actor.timers.isEmpty(), "external thread only submits actor work");
      actor.drain();
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(1, scans.get());
      final var cancelled = executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      cancelled.cancel();
      actor.drain();
      assertTrue(actor.timers.isEmpty(), "cancelled task must never be registered");
      final var racing = executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      actor.beforeTimerReturn = racing::cancel;
      actor.drain();
      assertTrue(actor.timers.isEmpty(), "cancellation during timer registration must be retained");
      actor.beforeTimerReturn = () -> {};
      executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      executor.close();
      actor.drain();
      assertTrue(actor.timers.isEmpty(), "closed executor must ignore queued registrations");
      assertEquals(1, scans.get());
    }
  }

  @Test
  void shouldSkipCancelledReadyTaskBeforeActorCleanup() {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final var scans = new AtomicInteger();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entry, source) -> CompletableActorFuture.completed(1L),
              batching(2, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> 1,
              () -> {});
      final var task = executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      actor.fire();
      actor.queueRuns = true;
      task.cancel();
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      actor.drain();
      executor.close();
    }
  }

  @Test
  void shouldWaitForAppendAndProcessingBeforeRescanningAndCopyCommand() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var actor = new Control();
      final var processed = new AtomicLong(-1);
      final List<LogAppendEntry> commands = new ArrayList<>();
      final var append = new CompletableActorFuture<Long>();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              store,
              registry(),
              (batch, source) -> {
                assertEquals(-1, source);
                commands.addAll(batch);
                return append;
              },
              batching(2, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              processed::get,
              () -> {});
      final JobRecord value = new JobRecord().setTenantId("tenant-a").setDeadline(100);
      executor.schedule(
          Duration.ZERO,
          writer -> {
            writer.append(1, ValueType.JOB, JobIntent.TIME_OUT, value);
            value.setDeadline(200);
          });
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(100, ((JobRecord) commands.getFirst().recordValue()).getDeadline());
      final var scans = new AtomicInteger();
      executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      append.complete(10L);
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      processed.set(10);
      executor.runAtCommitBoundary();
      assertEquals(1, scans.get());
      executor.close();
    }
  }

  @Test
  void shouldRetryAfterPartialAppendFailureAndNotWaitForFailedPosition() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final var processed = new AtomicLong(-1);
      final var attempts = new AtomicInteger();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entry, source) ->
                  attempts.incrementAndGet() == 1
                      ? CompletableActorFuture.completed(8L)
                      : CompletableActorFuture.completedExceptionally(
                          new IllegalStateException("backpressure")),
              batching(2, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              processed::get,
              () -> {});
      executor.schedule(
          Duration.ZERO,
          writer -> {
            writer.append(1, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord());
            writer.append(2, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord());
          });
      final var scans = new AtomicInteger();
      executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      actor.fire();
      actor.fire();
      executor.runAtCommitBoundary();
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      processed.set(8);
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      assertEquals(Duration.ofMillis(100), actor.delay);
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(1, scans.get());
    }
  }

  @Test
  void shouldCancelReadyTasksAndIgnoreCompletionAfterClose() throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final var completed = new CompletableActorFuture<Long>();
      final var wakeups = new AtomicInteger();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entry, source) -> completed,
              batching(2, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> -1,
              wakeups::incrementAndGet);
      final var scans = new AtomicInteger();
      final var cancelled = executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      actor.fire();
      cancelled.cancel();
      executor.runAtCommitBoundary();
      assertEquals(0, scans.get());
      executor.schedule(
          Duration.ZERO,
          writer -> writer.append(1, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
      actor.fire();
      executor.runAtCommitBoundary();
      executor.schedule(Duration.ZERO, writer -> scans.incrementAndGet());
      final int before = wakeups.get();
      executor.close();
      completed.complete(10L);
      executor.runAtCommitBoundary();
      assertTrue(actor.timers.isEmpty());
      assertEquals(before, wakeups.get());
      assertEquals(0, scans.get());
    }
  }

  @ParameterizedTest
  @CsvSource({"3,0,3", "10,2,2", "2,10,2", "1100,0,1100"})
  void shouldUseSharedCommandAndRecordLimits(int commandLimit, int recordLimit, int expected) {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final List<LogAppendEntry> appended = new ArrayList<>();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entries, source) -> {
                appended.addAll(entries);
                return CompletableActorFuture.completed(1L);
              },
              batching(
                  commandLimit, recordLimit, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> 1,
              () -> {});
      executor.schedule(
          Duration.ZERO,
          writer -> {
            for (int key = 0; writer.hasRemainingCapacity(); key++) {
              assertTrue(writer.tryAppend(key, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
            }
            assertFalse(writer.tryAppend(9999, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
          });
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(expected, appended.size());
      executor.close();
    }
  }

  @ParameterizedTest
  @CsvSource({"10,1", "0,2"})
  void shouldUseSoftWorkDurationAndAllowDisablingIt(long durationNanos, int expected) {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final var clock = new AtomicLong();
      final List<LogAppendEntry> appended = new ArrayList<>();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entries, source) -> {
                appended.addAll(entries);
                return CompletableActorFuture.completed(1L);
              },
              batching(
                  10, 0, Duration.ofNanos(durationNanos), LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> 1,
              () -> {},
              clock::get);
      executor.schedule(
          Duration.ZERO,
          writer -> {
            clock.set(20); // One state query can consume the soft budget before emitting a command.
            assertTrue(writer.tryAppend(1, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
            assertEquals(durationNanos == 0, writer.hasRemainingCapacity());
            assertEquals(
                durationNanos == 0,
                writer.tryAppend(2, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
          });
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(expected, appended.size());
      executor.close();
    }
  }

  @Test
  void shouldCancelAppendFailureBackoffWhenClosed() {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final var wakeups = new AtomicInteger();
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entries, source) ->
                  CompletableActorFuture.completedExceptionally(
                      new IllegalStateException("backpressure")),
              batching(1, 0, Duration.ZERO, LogBatchSizing.DEFAULT_MAX_BATCH_BYTES),
              () -> -1,
              wakeups::incrementAndGet);
      executor.schedule(
          Duration.ZERO,
          writer -> writer.append(1, ValueType.JOB, JobIntent.TIME_OUT, new JobRecord()));
      actor.fire();
      executor.runAtCommitBoundary();
      assertFalse(actor.timers.isEmpty());
      final int before = wakeups.get();
      executor.close();
      assertTrue(actor.timers.isEmpty());
      assertEquals(before, wakeups.get());
    }
  }

  @Test
  void shouldCountEncodedFramingAndRejectOversizedSingleCommand() {
    try (var storage = new PartitionStorage(directory)) {
      final var actor = new Control();
      final List<LogAppendEntry> appended = new ArrayList<>();
      final var value =
          new JobRecord().setTenantId("tenant").setVariables(new UnsafeBuffer(new byte[600]));
      final int exactBytes =
          Math.toIntExact(
              LogBatchSizing.payloadBytes(
                  List.of(
                      LogAppendEntry.of(
                          1,
                          new RecordMetadata()
                              .recordType(RecordType.COMMAND)
                              .valueType(ValueType.JOB)
                              .intent(JobIntent.TIME_OUT),
                          value))));
      final var executor =
          new ScheduledCommandExecutor(
              actor,
              new StateStoreProcessingStore(storage),
              registry(),
              (entries, source) -> {
                appended.addAll(entries);
                return CompletableActorFuture.completed(1L);
              },
              batching(10, 0, Duration.ZERO, exactBytes),
              () -> 1,
              () -> {});
      executor.schedule(
          Duration.ZERO,
          writer -> {
            assertTrue(writer.tryAppend(1, ValueType.JOB, JobIntent.TIME_OUT, value));
            assertFalse(writer.tryAppend(2, ValueType.JOB, JobIntent.TIME_OUT, value));
            assertFalse(writer.hasRemainingCapacity());
          });
      actor.fire();
      executor.runAtCommitBoundary();
      assertEquals(1, appended.size());
      assertEquals(exactBytes, LogBatchSizing.payloadBytes(appended));
      executor.schedule(
          Duration.ZERO,
          writer ->
              writer.tryAppend(
                  3,
                  ValueType.JOB,
                  JobIntent.TIME_OUT,
                  new JobRecord().setVariables(new UnsafeBuffer(new byte[8192]))));
      actor.fire();
      assertThrows(ExceededBatchRecordSizeException.class, executor::runAtCommitBoundary);
      assertEquals(1, appended.size());
      executor.close();
    }
  }

  private StreamProcessorContext.Batching batching(
      int commands, int records, Duration duration, int bytes) {
    return new StreamProcessorContext.Batching(commands, records, duration, bytes);
  }

  private TypedEventRegistry registry() {
    return TypedEventRegistry.builder().register(ValueType.JOB, JobRecord::new).build();
  }

  private static final class Control implements ConcurrencyControl {
    private final ArrayDeque<Runnable> timers = new ArrayDeque<>();
    private final ArrayDeque<Runnable> mailbox = new ArrayDeque<>();
    private boolean queueRuns;
    private Runnable beforeTimerReturn = () -> {};
    private Duration delay;

    void fire() {
      timers.remove().run();
    }

    @Override
    public void run(Runnable action) {
      if (queueRuns) {
        mailbox.add(action);
      } else {
        action.run();
      }
    }

    void drain() {
      while (!mailbox.isEmpty()) {
        mailbox.remove().run();
      }
    }

    @Override
    public <T> ActorFuture<T> call(Callable<T> action) {
      try {
        return CompletableActorFuture.completed(action.call());
      } catch (Exception e) {
        return CompletableActorFuture.completedExceptionally(e);
      }
    }

    @Override
    public <T> void runOnCompletion(ActorFuture<T> future, BiConsumer<T, Throwable> action) {
      future.onComplete(action, Runnable::run);
    }

    @Override
    public ScheduledTimer schedule(Duration delay, Runnable action) {
      this.delay = delay;
      timers.add(action);
      beforeTimerReturn.run();
      return () -> timers.remove(action);
    }
  }
}
