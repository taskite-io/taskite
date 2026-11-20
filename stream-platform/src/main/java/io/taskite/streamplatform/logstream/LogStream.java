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

import io.taskite.cluster.raft.RaftPartition;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.api.metrics.LogAppendMetrics;
import io.taskite.util.Either;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Per-partition application log. It owns position sequencing, the Raft storage adapter, committed
 * readers, and record-available listeners.
 */
public final class LogStream implements AutoCloseable {
  private final int partitionId;
  private final ConcurrencyControl appendControl;
  private final LogStorage logStorage;
  private final LogAppendMetrics logAppendMetrics;
  private final Sequencer sequencer;
  private final CopyOnWriteArrayList<LogRecordAwaiter> recordAwaiters =
      new CopyOnWriteArrayList<>();
  private final LogStorage.CommitListener commitListener = this::onCommit;
  private final AtomicBoolean closed = new AtomicBoolean();

  public LogStream(
      final int partitionId,
      final RaftPartition raftPartition,
      final ConcurrencyControl appendControl) {
    this(partitionId, raftPartition, appendControl, LogAppendMetrics.NOOP);
  }

  public LogStream(
      final int partitionId,
      final RaftPartition raftPartition,
      final ConcurrencyControl appendControl,
      final LogAppendMetrics logAppendMetrics) {
    this(partitionId, raftPartition, appendControl, logAppendMetrics, Integer.MAX_VALUE);
  }

  public LogStream(
      final int partitionId,
      final RaftPartition raftPartition,
      final ConcurrencyControl appendControl,
      final LogAppendMetrics logAppendMetrics,
      final int maxBatchBytes) {
    this(
        partitionId,
        new RaftLogStorage(
            Objects.requireNonNull(raftPartition, "raftPartition"),
            Objects.requireNonNull(appendControl, "appendControl")),
        appendControl,
        logAppendMetrics,
        maxBatchBytes);
  }

  LogStream(
      final int partitionId, final LogStorage logStorage, final ConcurrencyControl appendControl) {
    this(partitionId, logStorage, appendControl, LogAppendMetrics.NOOP);
  }

  LogStream(
      final int partitionId,
      final LogStorage logStorage,
      final ConcurrencyControl appendControl,
      final LogAppendMetrics logAppendMetrics) {
    this(partitionId, logStorage, appendControl, logAppendMetrics, Integer.MAX_VALUE);
  }

  LogStream(
      final int partitionId,
      final LogStorage logStorage,
      final ConcurrencyControl appendControl,
      final LogAppendMetrics logAppendMetrics,
      final int maxBatchBytes) {
    this.partitionId = partitionId;
    this.logStorage = Objects.requireNonNull(logStorage, "logStorage");
    this.appendControl = Objects.requireNonNull(appendControl, "appendControl");
    this.logAppendMetrics = Objects.requireNonNull(logAppendMetrics, "logAppendMetrics");
    sequencer =
        new Sequencer(
            logStorage, logStorage.getCommittedPosition(), logAppendMetrics, maxBatchBytes);
    logStorage.addCommitListener(commitListener);
  }

  /** The partition this log stream belongs to. */
  public int getPartitionId() {
    return partitionId;
  }

  /** A new sequential reader over committed records, addressed by position. */
  public LogStreamReader newLogStreamReader() {
    return logStorage.newReader();
  }

  /** Appends one record and completes when the replicated batch is committed. */
  public ActorFuture<Long> append(final LogAppendEntry entry, final long sourcePosition) {
    return append(List.of(entry), sourcePosition);
  }

  /** Appends one atomic record batch and completes at its highest committed position. */
  public ActorFuture<Long> append(final List<LogAppendEntry> entries, final long sourcePosition) {
    final ActorFuture<Long> result = appendControl.createFuture();
    try {
      final Either<Sequencer.WriteFailure, Long> writeResult =
          sequencer.tryWrite(
              List.copyOf(entries),
              sourcePosition,
              new LogStorage.AppendListener() {
                @Override
                public void onCommit(final long position) {
                  result.complete(position);
                }

                @Override
                public void onWriteError(final Throwable error) {
                  result.completeExceptionally(error);
                }

                @Override
                public void onCommitError(final long position, final Throwable error) {
                  result.completeExceptionally(error);
                }
              });
      if (writeResult.isLeft()) {
        result.completeExceptionally(
            new IllegalStateException("log append rejected: " + writeResult.getLeft()));
      }
    } catch (final Throwable error) {
      result.completeExceptionally(error);
    }
    return result;
  }

  /** Registers a listener woken when new committed records are available to read. */
  public void registerRecordAvailableListener(final LogRecordAwaiter recordAwaiter) {
    recordAwaiters.add(Objects.requireNonNull(recordAwaiter, "recordAwaiter"));
  }

  /** Removes a previously registered record-available listener. */
  public void removeRecordAvailableListener(final LogRecordAwaiter recordAwaiter) {
    recordAwaiters.remove(recordAwaiter);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      sequencer.close();
      logStorage.removeCommitListener(commitListener);
      recordAwaiters.clear();
      logAppendMetrics.close();
    }
  }

  private void onCommit() {
    for (final LogRecordAwaiter awaiter : recordAwaiters) {
      awaiter.onRecordAvailable();
    }
  }
}
