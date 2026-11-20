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
package io.taskite.cluster.raft.jraft.log;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * A forward, sequential {@link RaftJournalReader} over a jraft {@link Node}'s committed log
 * (migration decision D1). It presents the same cursor contract as the in-house journal reader so
 * the logstream layer's {@code RaftLogStreamReader} can expand batches into per-position records
 * without knowing whether raft is the in-house engine or jraft.
 *
 * <p>Each committed jraft DATA entry carries a {@link JRaftLogEntryCodec}-framed payload (the
 * {@code [lowestPosition, highestPosition]} span + business bytes). This reader walks committed
 * entries via {@link Node#readCommittedUserLog(long)} — which returns the first DATA entry at or
 * after a given index, bounded by {@code lastAppliedIndex}, transparently skipping jraft's internal
 * configuration/no-op entries — and reconstructs a {@link PersistedRaftRecord} for each, marrying
 * the jraft-assigned index with the decoded span.
 *
 * <p>Not thread-safe: each consumer (e.g. the apply loop) owns one, mirroring the in-house reader.
 */
public final class JRaftJournalReader implements RaftJournalReader {
  private final Node node;

  // The jraft index the next readCommittedUserLog() probe starts from (1-based; jraft rejects <=
  // 0).
  private long nextIndex = 1;
  // The DATA entry located by the last hasNext(), cached so next() returns it without re-reading.
  private UserLog cached;
  private long cachedIndex = -1;

  public JRaftJournalReader(final Node node) {
    this.node = Objects.requireNonNull(node, "node");
  }

  @Override
  public void seek(final long index) {
    nextIndex = Math.max(index, 1);
    clearCache();
  }

  @Override
  public void seekToFirst() {
    seek(1);
  }

  @Override
  public long nextIndex() {
    return nextIndex;
  }

  @Override
  public boolean hasNext() {
    if (cached != null) {
      return true;
    }
    while (true) {
      // Cheap bound first: nothing at or after nextIndex can be applied yet. Avoids
      // exception-driven control flow on the common "caught up" path (hasNext is polled in the
      // apply loop).
      if (nextIndex > node.getLastAppliedLogIndex()) {
        return false;
      }
      try {
        final UserLog log = node.readCommittedUserLog(nextIndex);
        if (JRaftLogEntryCodec.isExecutionAuthority(log.getData())) {
          nextIndex = log.getIndex() + 1;
          continue;
        }
        cached = log;
        // readCommittedUserLog may skip forward over non-DATA entries; the returned index is the
        // real DATA index, which the cursor must advance past.
        cachedIndex = log.getIndex();
        return true;
      } catch (final LogIndexOutOfBoundsException e) {
        // nextIndex is beyond lastAppliedIndex: no committed user entry available yet.
        return false;
      } catch (final LogNotFoundException e) {
        // A snapshot may compact a large prefix. Locate its boundary logarithmically so replay
        // neither consumes one Java stack frame nor throws one exception per deleted entry.
        final long lastAppliedIndex = node.getLastAppliedLogIndex();
        final UserLog log = findFirstAvailableUserLog(nextIndex, lastAppliedIndex);
        if (log == null) {
          nextIndex = lastAppliedIndex == Long.MAX_VALUE ? Long.MAX_VALUE : lastAppliedIndex + 1;
          return false;
        }
        if (JRaftLogEntryCodec.isExecutionAuthority(log.getData())) {
          nextIndex = log.getIndex() + 1;
          continue;
        }
        cached = log;
        cachedIndex = log.getIndex();
        return true;
      } catch (final IllegalStateException e) {
        // [nextIndex, lastAppliedIndex] holds only non-DATA (configuration/no-op) entries: no user
        // log to return yet. jraft signals this with an unchecked IllegalStateException.
        return false;
      }
    }
  }

  private UserLog findFirstAvailableUserLog(
      final long compactedIndex, final long lastAppliedIndex) {
    if (compactedIndex == Long.MAX_VALUE) {
      return null;
    }
    long low = compactedIndex + 1;
    long high = lastAppliedIndex;
    UserLog earliest = null;
    while (low <= high) {
      final long probe = low + ((high - low) >>> 1);
      try {
        earliest = node.readCommittedUserLog(probe);
        high = probe - 1;
      } catch (final LogNotFoundException e) {
        // The probe is still inside the compacted prefix.
        low = probe + 1;
      } catch (final LogIndexOutOfBoundsException e) {
        // The applied bound changed while probing.
        high = probe - 1;
      } catch (final IllegalStateException e) {
        // The probe is after the last DATA entry and only sees internal entries.
        high = probe - 1;
      }
    }
    return earliest;
  }

  @Override
  public PersistedRaftRecord next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no committed entry at index " + nextIndex);
    }
    final UserLog log = cached;
    final long index = cachedIndex;
    final ByteBuffer data = log.getData();
    final PersistedRaftRecord record = JRaftLogEntryCodec.decode(data, 0L, index);
    nextIndex = index + 1;
    clearCache();
    return record;
  }

  @Override
  public void close() {
    clearCache();
  }

  private void clearCache() {
    cached = null;
    cachedIndex = -1;
  }
}
