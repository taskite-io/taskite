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

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.streamplatform.api.metrics.LogAppendMetrics;
import io.taskite.util.Either;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes concurrent writes, assigns contiguous application positions, frames each batch, and
 * appends it to {@link LogStorage}. Position assignment lives above consensus, so processing code
 * never reasons about the Raft index.
 *
 * <p>Append is asynchronous because replication completes later. {@code tryWrite} immediately
 * returns the highest assigned position, while an optional {@link LogStorage.AppendListener}
 * receives write and commit progress.
 */
final class Sequencer {
  private static final Logger LOG = LoggerFactory.getLogger(Sequencer.class);

  private final LogStorage logStorage;
  private final LogAppendMetrics metrics;
  private final int maxBatchBytes;
  private final AtomicLong positionCounter;
  private final ReentrantLock lock = new ReentrantLock();
  private volatile boolean closed = false;

  Sequencer(final LogStorage logStorage, final long initialPosition) {
    this(logStorage, initialPosition, LogAppendMetrics.NOOP, Integer.MAX_VALUE);
  }

  Sequencer(
      final LogStorage logStorage, final long initialPosition, final LogAppendMetrics metrics) {
    this(logStorage, initialPosition, metrics, Integer.MAX_VALUE);
  }

  Sequencer(
      final LogStorage logStorage,
      final long initialPosition,
      final LogAppendMetrics metrics,
      final int maxBatchBytes) {
    this.logStorage = logStorage;
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    LogBatchSizing.validateMaxBatchBytes(maxBatchBytes);
    this.maxBatchBytes = maxBatchBytes;
    this.positionCounter = new AtomicLong(Math.max(initialPosition, 0));
  }

  Either<WriteFailure, Long> tryWrite(
      final List<LogAppendEntry> appendEntries, final long sourcePosition) {
    return tryWrite(appendEntries, sourcePosition, NOOP_LISTENER);
  }

  /**
   * As {@link #tryWrite(List, long)} but notifies {@code listener} of the append lifecycle. Used by
   * the broker to complete the client response future when the batch commits.
   */
  Either<WriteFailure, Long> tryWrite(
      final List<LogAppendEntry> appendEntries,
      final long sourcePosition,
      final LogStorage.AppendListener listener) {
    if (closed) {
      return Either.left(WriteFailure.CLOSED);
    }
    if (appendEntries.isEmpty()) {
      return Either.left(WriteFailure.INVALID_ARGUMENT);
    }
    for (final LogAppendEntry entry : appendEntries) {
      if (!isValid(entry)) {
        return Either.left(WriteFailure.INVALID_ARGUMENT);
      }
    }
    LogBatchSizing.ensureWithinLimit(LogBatchSizing.payloadBytes(appendEntries), maxBatchBytes);

    lock.lock();
    try {
      // Raft recovery may publish its committed position after this sequencer is constructed.
      // Reconcile on every write while preserving positions already assigned to in-flight appends.
      positionCounter.accumulateAndGet(logStorage.getCommittedPosition(), Math::max);
      final long timestamp = System.currentTimeMillis();
      final long lowestPosition = positionCounter.get() + 1;
      final List<byte[]> framedEntries = new ArrayList<>(appendEntries.size());
      long position = lowestPosition;
      for (final LogAppendEntry entry : appendEntries) {
        framedEntries.add(serializeEntry(entry, position, sourcePosition, timestamp));
        position++;
      }
      final long highestPosition = position - 1;
      final byte[] payload = BatchPayloadCodec.encode(framedEntries);
      LogBatchSizing.ensureWithinLimit(payload.length, maxBatchBytes);
      logStorage.append(lowestPosition, highestPosition, payload, listener);
      positionCounter.set(highestPosition);
      recordBatchMetrics(appendEntries.size(), payload.length);
      return Either.right(highestPosition);
    } finally {
      lock.unlock();
    }
  }

  void close() {
    closed = true;
  }

  private void recordBatchMetrics(final int recordCount, final int payloadBytes) {
    try {
      metrics.onBatchAppended(recordCount, payloadBytes);
    } catch (final RuntimeException error) {
      LOG.warn("Failed to record log append metrics", error);
    }
  }

  private static byte[] serializeEntry(
      final LogAppendEntry entry,
      final long position,
      final long sourcePosition,
      final long timestamp) {
    final int length = LogEntryDescriptor.framedLength(entry);
    final byte[] bytes = new byte[length];
    LogEntryDescriptor.serialize(
        new UnsafeBuffer(bytes), 0, entry, position, sourcePosition, timestamp);
    return bytes;
  }

  private static boolean isValid(final LogAppendEntry entry) {
    final RecordMetadata metadata = entry.recordMetadata();
    final UnifiedRecordValue value = entry.recordValue();
    return metadata != null && value != null;
  }

  enum WriteFailure {
    CLOSED,
    FULL,
    INVALID_ARGUMENT
  }

  private static final LogStorage.AppendListener NOOP_LISTENER = new LogStorage.AppendListener() {};
}
