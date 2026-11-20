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
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Expands the journal's index-keyed, batch-carrying {@link RaftJournalReader} into a position-keyed
 * {@link LogStreamReader} the {@code StreamProcessor} consumes. Each raft entry carries a batch of
 * one or more business records spanning {@code [lowestPosition, highestPosition]}; this reader
 * walks those records one position at a time, advancing to the next raft entry only when the
 * current batch is drained. The StreamProcessor therefore reasons purely in positions and never
 * sees the raft index. The cursor maps at most the single segment it is traversing, so memory stays
 * bounded.
 */
final class RaftLogStreamReader implements LogStreamReader {
  private final Supplier<RaftJournalReader> readerSource;
  private final LongSupplier snapshotPositionSupplier;
  private RaftJournalReader delegate;

  // Records of the batch currently being walked, and the cursor within it.
  private List<byte[]> batchRecords = List.of();
  private long batchLowestPosition = -1;
  private int batchCursor = 0;
  // The next application position this reader will return.
  private long nextPosition = -1;

  RaftLogStreamReader(final RaftPartition raftPartition) {
    this(raftPartition::openLogReader, raftPartition::snapshotPosition);
  }

  RaftLogStreamReader(final Supplier<RaftJournalReader> readerSource) {
    this(readerSource, () -> 0L);
  }

  RaftLogStreamReader(
      final Supplier<RaftJournalReader> readerSource, final LongSupplier snapshotPositionSupplier) {
    this.readerSource = readerSource;
    this.snapshotPositionSupplier = snapshotPositionSupplier;
  }

  private RaftJournalReader delegate() {
    if (delegate == null) {
      delegate = readerSource.get();
    }
    return delegate;
  }

  @Override
  public void seekToNextEvent(final long position) {
    // Resume after the last processed position: the next position to apply is position + 1. A
    // negative position means "from the start".
    seek(position < 0 ? 1 : position + 1);
  }

  @Override
  public void seek(final long position) {
    final long target = Math.max(Math.max(position, 1), snapshotPosition() + 1);
    resetBatch();
    nextPosition = target;
    // Position the underlying index cursor at the start; the batch containing the target position
    // is
    // located by scanning forward in next()/hasNext(). Index and position are both monotonic and
    // the
    // journal is contiguous, so a forward scan from the first index reaches the owning batch.
    delegate().seekToFirst();
  }

  @Override
  public long getNextPosition() {
    return nextPosition;
  }

  @Override
  public long snapshotPosition() {
    return snapshotPositionSupplier.getAsLong();
  }

  @Override
  public boolean hasNext() {
    return ensurePositioned();
  }

  @Override
  public LoggedEvent next() {
    if (!ensurePositioned()) {
      throw new NoSuchElementException("no entry at position " + nextPosition);
    }
    final byte[] entryBytes = batchRecords.get(batchCursor);
    final LoggedEvent event = new LoggedEvent(entryBytes);
    batchCursor++;
    nextPosition++;
    return event;
  }

  /**
   * Ensures the in-batch cursor points at the record for {@code nextPosition}, advancing through
   * raft entries as needed. Returns false when no committed batch covers {@code nextPosition} yet.
   */
  private boolean ensurePositioned() {
    if (nextPosition <= snapshotPosition()) {
      seek(nextPosition);
    }
    // Still inside the current batch and the cursor lines up with nextPosition.
    if (batchCursor < batchRecords.size() && batchLowestPosition + batchCursor == nextPosition) {
      return true;
    }
    // Advance through raft entries until one whose span contains nextPosition.
    final RaftJournalReader reader = delegate();
    while (reader.hasNext()) {
      final PersistedRaftRecord entry = reader.next();
      if (entry.highestPosition() < nextPosition) {
        continue; // whole batch is before the target position
      }
      final List<byte[]> records = BatchPayloadCodec.decode(entry.data());
      final long offset = nextPosition - entry.lowestPosition();
      if (offset < 0) {
        // Snapshot installation may compact the journal prefix beyond the persisted business
        // cursor. Resume from the first position that is still available instead of waiting
        // forever for a compacted position.
        nextPosition = entry.lowestPosition();
        batchRecords = records;
        batchLowestPosition = entry.lowestPosition();
        batchCursor = 0;
        return true;
      }
      if (offset >= records.size()) {
        // The advertised span does not contain nextPosition.
        return false;
      }
      batchRecords = records;
      batchLowestPosition = entry.lowestPosition();
      batchCursor = (int) offset;
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
      delegate = null;
    }
    resetBatch();
  }

  private void resetBatch() {
    batchRecords = List.of();
    batchLowestPosition = -1;
    batchCursor = 0;
  }
}
