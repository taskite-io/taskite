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
package io.taskite.streamplatform.command;

import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.metrics.CommandDispatchMetrics;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tracks submitted commands until their committed business response is delivered. */
public final class StreamPartitionCommandDispatcher implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamPartitionCommandDispatcher.class);

  private final int partitionId;
  private final BooleanSupplier leaderSupplier;
  private final RecordAppender appender;
  private final ConcurrencyControl concurrencyControl;
  private final CommandDispatchMetrics metrics;
  private final LongSupplier nanoTime;
  private final PartitionRequestLimiter<Intent> inFlightLimiter;
  private final ConcurrentHashMap<String, PendingCommand> pending = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object admissionLock = new Object();
  private LeadershipTransfer leadershipTransfer;

  public StreamPartitionCommandDispatcher(
      final int partitionId,
      final int maxInFlight,
      final BooleanSupplier leaderSupplier,
      final RecordAppender appender,
      final ConcurrencyControl concurrencyControl) {
    this(
        partitionId,
        maxInFlight,
        leaderSupplier,
        appender,
        concurrencyControl,
        CommandDispatchMetrics.NOOP);
  }

  public StreamPartitionCommandDispatcher(
      final int partitionId,
      final int maxInFlight,
      final BooleanSupplier leaderSupplier,
      final RecordAppender appender,
      final ConcurrencyControl concurrencyControl,
      final CommandDispatchMetrics metrics) {
    this(
        partitionId,
        maxInFlight,
        leaderSupplier,
        appender,
        concurrencyControl,
        metrics,
        System::nanoTime);
  }

  StreamPartitionCommandDispatcher(
      final int partitionId,
      final int maxInFlight,
      final BooleanSupplier leaderSupplier,
      final RecordAppender appender,
      final ConcurrencyControl concurrencyControl,
      final CommandDispatchMetrics metrics,
      final LongSupplier nanoTime) {
    this(
        partitionId,
        new PartitionRequestLimiter<>(
            BackpressureConfiguration.fixed(Math.max(1, maxInFlight)), ignored -> false),
        leaderSupplier,
        appender,
        concurrencyControl,
        metrics,
        nanoTime);
  }

  public StreamPartitionCommandDispatcher(
      final int partitionId,
      final PartitionRequestLimiter<Intent> limiter,
      final BooleanSupplier leaderSupplier,
      final RecordAppender appender,
      final ConcurrencyControl concurrencyControl,
      final CommandDispatchMetrics metrics) {
    this(
        partitionId,
        limiter,
        leaderSupplier,
        appender,
        concurrencyControl,
        metrics,
        System::nanoTime);
  }

  private StreamPartitionCommandDispatcher(
      final int partitionId,
      final PartitionRequestLimiter<Intent> limiter,
      final BooleanSupplier leaderSupplier,
      final RecordAppender appender,
      final ConcurrencyControl concurrencyControl,
      final CommandDispatchMetrics metrics,
      final LongSupplier nanoTime) {
    this.partitionId = partitionId;
    this.leaderSupplier = Objects.requireNonNull(leaderSupplier, "leaderSupplier");
    this.appender = Objects.requireNonNull(appender, "appender");
    this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    inFlightLimiter = Objects.requireNonNull(limiter, "limiter");
  }

  public ActorFuture<LoggedEvent> submit(final LoggedEvent commandRecord) {
    synchronized (admissionLock) {
      return submitUnderLock(commandRecord);
    }
  }

  private ActorFuture<LoggedEvent> submitUnderLock(final LoggedEvent commandRecord) {
    if (closed.get()) {
      return failed(new IllegalStateException("stream partition is closed: " + partitionId));
    }
    if (leadershipTransfer != null || !leaderSupplier.getAsBoolean()) {
      return failed(new PartitionNotLeaderBeforeAppendException(partitionId));
    }
    final var permit = inFlightLimiter.tryAcquire(commandRecord.metadata().getIntent());
    if (permit.isEmpty()) {
      return failed(new BackpressureException(partitionId));
    }

    final String requestId = requestId(commandRecord);
    final CommandTiming timing = new CommandTiming();
    final ActorFuture<LoggedEvent> result = concurrencyControl.createFuture();
    final var command =
        new PendingCommand(commandRecord.metadata().getValueType(), result, permit.get(), timing);
    if (pending.putIfAbsent(requestId, command) != null) {
      permit.get().ignore();
      return failed(new IllegalArgumentException("duplicate in-flight requestId: " + requestId));
    }
    recordMetric("pending commands", () -> metrics.onSubmitted(pending.size() - 1));
    try {
      final LogAppendEntry entry =
          LogAppendEntry.of(commandRecord.key(), commandRecord.metadata(), commandRecord.value());
      concurrencyControl.runOnCompletion(
          appender.append(entry, -1L),
          (ignored, error) -> {
            timing.onAppendCompleted(error);
            if (error != null) {
              fail(requestId, command, error);
            }
          });
    } catch (final Throwable error) {
      fail(requestId, command, error);
    }
    return result;
  }

  public void onResponse(final LoggedEvent record) {
    final RecordType recordType = record.metadata().getRecordType();
    if (recordType != RecordType.EVENT && recordType != RecordType.COMMAND_REJECTION) {
      return;
    }
    final String requestId = record.metadata().getRequestId();
    synchronized (admissionLock) {
      final PendingCommand command = pending.get(requestId);
      if (command != null && command.accepts(record)) {
        complete(requestId, command, record);
      }
    }
  }

  private void complete(
      final String requestId, final PendingCommand command, final LoggedEvent response) {
    if (pending.remove(requestId, command)) {
      command.permit().success();
      command.timing().onCommandCompleted();
      if (!command.future().isDone()) {
        command.future().complete(response);
      }
      transferIfDrained();
    }
  }

  /**
   * Stops admission atomically with pending insertion and waits for accepted commands to finish.
   */
  public ActorFuture<Void> transferLeadership(
      final Supplier<ActorFuture<Void>> transfer, final Duration drainTimeout) {
    Objects.requireNonNull(transfer, "transfer");
    if (drainTimeout == null || drainTimeout.isZero() || drainTimeout.isNegative()) {
      return failed(new IllegalArgumentException("leadership drain timeout must be positive"));
    }
    synchronized (admissionLock) {
      if (closed.get() || !leaderSupplier.getAsBoolean()) {
        return failed(new IllegalStateException("partition " + partitionId + " is not leader"));
      }
      if (leadershipTransfer != null) {
        return failed(new IllegalStateException("leadership transfer already in flight"));
      }
      final var attempt = new LeadershipTransfer(transfer);
      leadershipTransfer = attempt;
      try {
        attempt.timer =
            concurrencyControl.schedule(
                drainTimeout,
                () -> {
                  synchronized (admissionLock) {
                    if (leadershipTransfer == attempt) {
                      finishTransfer(
                          attempt, new IllegalStateException("leadership transfer timed out"));
                    }
                  }
                });
        transferIfDrained();
      } catch (final Throwable error) {
        finishTransfer(attempt, error);
      }
      return attempt.result;
    }
  }

  private void transferIfDrained() {
    final var attempt = leadershipTransfer;
    if (attempt == null || attempt.started || !pending.isEmpty()) {
      return;
    }
    attempt.started = true;
    try {
      attempt
          .transfer
          .get()
          .onComplete(
              (ignored, error) -> {
                synchronized (admissionLock) {
                  if (error != null) {
                    finishTransfer(attempt, error);
                  } else {
                    attempt.acknowledged = true;
                    if (attempt.leadershipLost || !leaderSupplier.getAsBoolean()) {
                      finishTransfer(attempt, null);
                    }
                  }
                }
              },
              Runnable::run);
    } catch (final Throwable error) {
      finishTransfer(attempt, error);
    }
  }

  private void finishTransfer(final LeadershipTransfer attempt, final Throwable error) {
    if (leadershipTransfer != attempt) {
      return;
    }
    leadershipTransfer = null;
    if (attempt.timer != null) {
      attempt.timer.cancel();
    }
    if (error == null) {
      attempt.result.complete(null);
    } else {
      attempt.result.completeExceptionally(error);
    }
  }

  public void onProcessingFailure(final LoggedEvent record, final Throwable error) {
    if (record != null) {
      fail(record.metadata().getRequestId(), error);
    }
  }

  public void onFailure(final String requestId, final Throwable error) {
    if (requestId != null) {
      fail(requestId, error);
    }
  }

  public void onLeadershipLost() {
    synchronized (admissionLock) {
      final var error = new IllegalStateException("partition " + partitionId + " lost leadership");
      // During the actual Raft transfer leadership loss is expected; its callback owns completion.
      if (leadershipTransfer != null && !leadershipTransfer.started) {
        finishTransfer(leadershipTransfer, error);
      } else if (leadershipTransfer != null) {
        leadershipTransfer.leadershipLost = true;
        if (leadershipTransfer.acknowledged) {
          finishTransfer(leadershipTransfer, null);
        }
      }
      failAll(error);
    }
  }

  @Override
  public void close() {
    synchronized (admissionLock) {
      if (closed.compareAndSet(false, true)) {
        final var error = new IllegalStateException("partition " + partitionId + " is closed");
        if (leadershipTransfer != null) {
          finishTransfer(leadershipTransfer, error);
        }
        failAll(error);
        recordMetric("close", metrics::close);
      }
    }
  }

  private String requestId(final LoggedEvent commandRecord) {
    final String requestId = commandRecord.metadata().getRequestId();
    if (requestId != null && !requestId.isBlank()) {
      return requestId;
    }
    final String generated = UUID.randomUUID().toString();
    commandRecord.metadata().requestId(generated);
    return generated;
  }

  private void fail(final String requestId, final Throwable error) {
    fail(requestId, null, error);
  }

  private void fail(final String requestId, final PendingCommand expected, final Throwable error) {
    synchronized (admissionLock) {
      final PendingCommand command = pending.get(requestId);
      if (command != null
          && (expected == null || expected == command)
          && pending.remove(requestId, command)) {
        command.permit().drop();
        if (!command.future().isDone()) {
          command.future().completeExceptionally(error);
        }
        transferIfDrained();
      }
    }
  }

  private void failAll(final Throwable error) {
    final var commands = List.copyOf(pending.values());
    pending.clear();
    inFlightLimiter.reset();
    for (final var command : commands) {
      command.permit().ignore();
      if (!command.future().isDone()) {
        command.future().completeExceptionally(error);
      }
    }
  }

  private static <T> ActorFuture<T> failed(final Throwable error) {
    return CompletableActorFuture.completedExceptionally(error);
  }

  private void recordMetric(final String measurement, final Runnable recorder) {
    try {
      recorder.run();
    } catch (final RuntimeException error) {
      LOG.warn("failed to record {} metric for partition {}", measurement, partitionId, error);
    }
  }

  private final class CommandTiming {
    private final long submittedAtNanos = nanoTime.getAsLong();
    private long appendCompletedAtNanos = -1;
    private long commandCompletedAtNanos = -1;
    private boolean processingRecorded;

    private void onAppendCompleted(final Throwable error) {
      if (error != null) {
        return;
      }
      final long completedAtNanos = nanoTime.getAsLong();
      recordMetric(
          "Raft append duration",
          () -> metrics.onRaftAppendCompleted(completedAtNanos - submittedAtNanos));
      synchronized (this) {
        appendCompletedAtNanos = completedAtNanos;
        recordProcessingIfComplete();
      }
    }

    private void onCommandCompleted() {
      synchronized (this) {
        commandCompletedAtNanos = nanoTime.getAsLong();
        recordProcessingIfComplete();
      }
    }

    private void recordProcessingIfComplete() {
      if (processingRecorded || appendCompletedAtNanos < 0 || commandCompletedAtNanos < 0) {
        return;
      }
      processingRecorded = true;
      recordMetric(
          "processing duration",
          () ->
              metrics.onProcessingCompleted(
                  Math.max(0, commandCompletedAtNanos - appendCompletedAtNanos)));
    }
  }

  private record PendingCommand(
      ValueType valueType,
      ActorFuture<LoggedEvent> future,
      PartitionRequestLimiter<Intent>.Permit permit,
      CommandTiming timing) {
    boolean accepts(final LoggedEvent record) {
      return valueType == record.metadata().getValueType();
    }
  }
}
