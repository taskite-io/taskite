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

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogBatchSizing;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Coordinates timer wakeups, state reads and asynchronous command commits for one leader. */
final class ScheduledCommandExecutor implements ProcessingScheduleService, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledCommandExecutor.class);
  private static final Duration APPEND_RETRY_DELAY = Duration.ofMillis(100);
  private final ConcurrencyControl actor;
  private final ProcessingStateStore storage;
  private final TypedEventRegistry registry;
  private final RecordAppender appender;
  private final StreamProcessorContext.Batching batching;
  private final LongSupplier nanoTime;
  private final LongSupplier processedPosition;
  private final Runnable wakeup;
  private final Set<Task> tasks = new HashSet<>();
  private final ArrayDeque<Task> ready = new ArrayDeque<>();
  private int pendingAppends;
  private long appendedPosition = -1;
  private boolean appendFailed;
  private ScheduledTimer retryTimer;
  private volatile boolean closed;

  ScheduledCommandExecutor(
      final ConcurrencyControl actor,
      final ProcessingStateStore storage,
      final TypedEventRegistry registry,
      final RecordAppender appender,
      final StreamProcessorContext.Batching batching,
      final LongSupplier processedPosition,
      final Runnable wakeup) {
    this(actor, storage, registry, appender, batching, processedPosition, wakeup, System::nanoTime);
  }

  ScheduledCommandExecutor(
      final ConcurrencyControl actor,
      final ProcessingStateStore storage,
      final TypedEventRegistry registry,
      final RecordAppender appender,
      final StreamProcessorContext.Batching batching,
      final LongSupplier processedPosition,
      final Runnable wakeup,
      final LongSupplier nanoTime) {
    this.actor = actor;
    this.storage = storage;
    this.registry = registry;
    this.appender = appender;
    this.batching = batching;
    this.nanoTime = nanoTime;
    this.processedPosition = processedPosition;
    this.wakeup = wakeup;
  }

  @Override
  public ScheduledTimer schedule(final Duration delay, final Consumer<CommandWriter> action) {
    if (closed) {
      return () -> {};
    }
    final Task task = new Task(action);
    actor.run(() -> register(task, delay));
    return task;
  }

  private void register(final Task task, final Duration delay) {
    if (closed || task.cancelled) {
      return;
    }
    tasks.add(task);
    task.timer =
        actor.schedule(
            delay,
            () -> {
              if (!closed && !task.cancelled) {
                ready.add(task);
                wakeup.run();
              }
            });
    if (closed || task.cancelled) {
      task.timer.cancel();
      tasks.remove(task);
      ready.remove(task);
    }
  }

  /** Called only when the processing state machine has no open command transaction. */
  void runAtCommitBoundary() {
    if (closed
        || retryTimer != null
        || pendingAppends > 0
        || processedPosition.getAsLong() < appendedPosition) {
      return;
    }
    Task task = ready.poll();
    while (task != null && task.cancelled) {
      tasks.remove(task);
      task = ready.poll();
    }
    if (task == null) {
      return;
    }
    tasks.remove(task);
    final var commands = new ArrayList<LogAppendEntry>();
    final var transaction = storage.getCurrentTransaction();
    try {
      task.action.accept(new BudgetedCommandWriter(commands));
    } finally {
      try {
        transaction.rollback();
      } catch (final Exception error) {
        throw new IllegalStateException("Failed to close scheduled task read transaction", error);
      }
    }
    pendingAppends = commands.size();
    appendFailed = false;
    for (final LogAppendEntry command : commands) {
      try {
        actor.runOnCompletion(appender.append(command), this::onAppend);
      } catch (final Exception error) {
        onAppend(null, error);
      }
    }
    if (!ready.isEmpty() && pendingAppends == 0) {
      actor.run(wakeup);
    }
  }

  private void onAppend(final Long position, final Throwable error) {
    if (closed) {
      return;
    }
    if (error == null) {
      appendedPosition = Math.max(appendedPosition, position);
    } else {
      // The durable due index is unchanged. The next check will retry this command.
      LOG.warn("Scheduled command append failed; retained for the next due check", error);
      appendFailed = true;
    }
    if (--pendingAppends == 0) {
      if (appendFailed) {
        retryTimer =
            actor.schedule(
                APPEND_RETRY_DELAY,
                () -> {
                  retryTimer = null;
                  if (!closed) {
                    wakeup.run();
                  }
                });
      } else {
        actor.run(wakeup);
      }
    }
  }

  @Override
  public void close() {
    closed = true;
    if (retryTimer != null) {
      retryTimer.cancel();
      retryTimer = null;
    }
    new ArrayList<>(tasks).forEach(Task::cancel);
    tasks.clear();
    ready.clear();
  }

  private final class BudgetedCommandWriter implements CommandWriter {
    private final ArrayList<LogAppendEntry> commands;
    private final long startedAt = nanoTime.getAsLong();
    private final long durationNanos = batching.maxBatchDuration().toNanos();
    private long payloadBytes = LogBatchSizing.EMPTY_BATCH_BYTES;
    private boolean bytesExhausted;

    private BudgetedCommandWriter(final ArrayList<LogAppendEntry> commands) {
      this.commands = commands;
    }

    @Override
    public int remainingCapacity() {
      if (bytesExhausted
          || (!commands.isEmpty()
              && durationNanos > 0
              && nanoTime.getAsLong() - startedAt >= durationNanos)) {
        return 0;
      }
      final int commandLimit = batching.maxCommandsInBatch();
      final int recordLimit =
          batching.maxRecordsInBatch() == 0 ? commandLimit : batching.maxRecordsInBatch();
      return Math.max(0, Math.min(commandLimit, recordLimit) - commands.size());
    }

    @Override
    public void append(
        final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
      if (!tryAppend(key, type, intent, value)) {
        throw new IllegalStateException("Scheduled task exceeded the shared processing budget");
      }
    }

    @Override
    public boolean tryAppend(
        final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
      if (intent.isEvent()) {
        throw new IllegalArgumentException("Scheduled task must emit commands");
      }
      if (!hasRemainingCapacity()) {
        return false;
      }
      final var metadata =
          new RecordMetadata().recordType(RecordType.COMMAND).valueType(type).intent(intent);
      final var entry = LogAppendEntry.of(key, metadata, value);
      // A single oversized command cannot make progress by retrying in an empty round.
      LogBatchSizing.ensureWithinLimit(
          LogBatchSizing.payloadBytesAfterAppend(LogBatchSizing.EMPTY_BATCH_BYTES, entry),
          batching.maxBatchBytes());
      final long nextBytes = LogBatchSizing.payloadBytesAfterAppend(payloadBytes, entry);
      if (nextBytes > batching.maxBatchBytes()) {
        bytesExhausted = true;
        return false;
      }
      final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
      value.write(buffer, 0);
      final var copy = registry.create(type, intent);
      copy.wrap(buffer);
      commands.add(LogAppendEntry.of(key, metadata, copy));
      payloadBytes = nextBytes;
      return true;
    }
  }

  private final class Task implements ScheduledTimer {
    private final Consumer<CommandWriter> action;
    private volatile ScheduledTimer timer;
    private volatile boolean cancelled;

    private Task(final Consumer<CommandWriter> action) {
      this.action = action;
    }

    @Override
    public void cancel() {
      cancelled = true;
      if (timer != null) {
        timer.cancel();
      }
      if (!closed) {
        actor.run(
            () -> {
              tasks.remove(this);
              ready.remove(this);
            });
      }
    }
  }
}
