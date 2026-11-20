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
package io.taskite.streamplatform.logstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

final class LogStreamTest {
  private final DirectControl control = new DirectControl();

  @Test
  void appendCompletesWhenStorageCommits() {
    final TestLogStorage storage = new TestLogStorage();
    final LogStream logStream = new LogStream(1, storage, control);

    final ActorFuture<Long> result = logStream.append(jobEntry(), -1L);
    assertFalse(result.isDone());

    storage.commitAppend();

    assertEquals(1L, result.join());
    logStream.close();
  }

  @Test
  void appendsMultipleRecordsAsOneAtomicStorageBatch() {
    final TestLogStorage storage = new TestLogStorage();
    final LogStream logStream = new LogStream(1, storage, control);

    final ActorFuture<Long> result = logStream.append(List.of(jobEntry(), jobEntry()), -1L);
    assertFalse(result.isDone());
    assertEquals(1L, storage.lowestPosition);
    assertEquals(2L, storage.highestPosition);
    assertEquals(2, BatchPayloadCodec.decode(storage.batchPayload).size());

    storage.commitAppend();

    assertEquals(2L, result.join());
    logStream.close();
  }

  @Test
  void rejectedAppendCompletesExceptionally() {
    final LogStream logStream = new LogStream(1, new TestLogStorage(), control);
    final LogAppendEntry invalidEntry =
        new LogAppendEntry() {
          @Override
          public long key() {
            return KEY_NULL_VALUE;
          }

          @Override
          public int sourceIndex() {
            return -1;
          }

          @Override
          public RecordMetadata recordMetadata() {
            return null;
          }

          @Override
          public UnifiedRecordValue recordValue() {
            return new UnifiedRecordValue();
          }
        };

    assertTrue(logStream.append(invalidEntry, -1L).isCompletedExceptionally());
    logStream.close();
  }

  @Test
  void closeIsIdempotentAndRejectsLaterAppends() {
    final TestLogStorage storage = new TestLogStorage();
    final LogStream logStream = new LogStream(1, storage, control);

    logStream.close();
    logStream.close();

    assertEquals(1, storage.removeListenerCalls.get());
    assertTrue(logStream.append(jobEntry(), -1L).isCompletedExceptionally());
  }

  @Test
  void publishesCommitOnlyToRegisteredAwaiters() {
    final TestLogStorage storage = new TestLogStorage();
    final LogStream logStream = new LogStream(1, storage, control);
    final AtomicInteger notifications = new AtomicInteger();
    final LogRecordAwaiter awaiter = notifications::incrementAndGet;

    logStream.registerRecordAvailableListener(awaiter);
    storage.publishCommit();
    logStream.removeRecordAvailableListener(awaiter);
    storage.publishCommit();

    assertEquals(1, notifications.get());
    logStream.close();
  }

  private static LogAppendEntry jobEntry() {
    return LogAppendEntry.of(
        new RecordMetadata()
            .recordType(RecordType.COMMAND)
            .valueType(ValueType.JOB)
            .intent(JobIntent.COMPLETE),
        new JobRecord().setJobType("type").setWorker("worker").setTenantId("tenant"));
  }

  private static final class TestLogStorage implements LogStorage {
    private final AtomicInteger removeListenerCalls = new AtomicInteger();
    private AppendListener appendListener;
    private CommitListener commitListener;
    private byte[] batchPayload;
    private long lowestPosition;
    private long highestPosition;

    @Override
    public LogStreamReader newReader() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void append(
        final long lowestPosition,
        final long highestPosition,
        final byte[] batchPayload,
        final AppendListener listener) {
      this.lowestPosition = lowestPosition;
      this.highestPosition = highestPosition;
      this.batchPayload = batchPayload;
      appendListener = listener;
    }

    @Override
    public void addCommitListener(final CommitListener listener) {
      commitListener = listener;
    }

    @Override
    public void removeCommitListener(final CommitListener listener) {
      if (commitListener == listener) {
        commitListener = null;
      }
      removeListenerCalls.incrementAndGet();
    }

    @Override
    public long getCommittedPosition() {
      return 0;
    }

    void commitAppend() {
      appendListener.onCommit(highestPosition);
    }

    void publishCommit() {
      if (commitListener != null) {
        commitListener.onCommit();
      }
    }
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
