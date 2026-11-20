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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class JRaftSnapshotProgressTest {
  private static final LogEntryEncoder ENCODER = entry -> new byte[entry.getData().remaining()];

  @Test
  void tracksSnapshotCoverageSeparatelyFromRetainedBytes() {
    final JRaftSnapshotProgress progress = new JRaftSnapshotProgress();
    final AtomicInteger updates = new AtomicInteger();
    progress.subscribe(ignored -> updates.incrementAndGet());

    progress.onAppended(entry(1, 10), ENCODER);
    progress.onAppended(entry(2, 20), ENCODER);
    progress.onAppended(entry(3, 30), ENCODER);
    progress.onSnapshotPersisted(2, 2_048);

    assertEquals(3, progress.current().lastLogIndex());
    assertEquals(2, progress.current().lastSnapshotIndex());
    assertEquals(30, progress.current().uncoveredLogBytes());
    assertEquals(60, progress.current().retainedLogBytes());
    assertEquals(2_048, progress.current().lastSnapshotSizeBytes());

    progress.onPrefixTruncated(3);

    assertEquals(30, progress.current().retainedLogBytes());
    assertEquals(30, progress.current().uncoveredLogBytes());

    progress.onSnapshotLoaded(3, 4_096);
    progress.onAppended(entry(4, 40), ENCODER);

    assertEquals(3, progress.current().lastSnapshotIndex());
    assertEquals(40, progress.current().uncoveredLogBytes());
    assertEquals(40, progress.current().retainedLogBytes());
    assertEquals(4_096, progress.current().lastSnapshotSizeBytes());
    assertEquals(7, updates.get());
  }

  @Test
  void rebuildUsesTheRetainedPrefixAsAConservativeSnapshotBoundary() {
    final TestLogStorage storage = new TestLogStorage(List.of(entry(5, 50), entry(6, 60)));
    final JRaftSnapshotProgress progress = new JRaftSnapshotProgress();

    progress.rebuild(storage, ENCODER);

    assertEquals(6, progress.current().lastLogIndex());
    assertEquals(4, progress.current().lastSnapshotIndex());
    assertEquals(110, progress.current().uncoveredLogBytes());
    assertEquals(110, progress.current().retainedLogBytes());

    progress.onSnapshotPersisted(5, 8_192);

    assertEquals(60, progress.current().uncoveredLogBytes());
    assertEquals(8_192, progress.current().lastSnapshotSizeBytes());
  }

  private static LogEntry entry(final long index, final int encodedBytes) {
    final LogEntry entry = new LogEntry();
    entry.setId(new LogId(index, 1));
    entry.setData(ByteBuffer.allocate(encodedBytes));
    return entry;
  }

  private static final class TestLogStorage implements LogStorage {
    private final NavigableMap<Long, LogEntry> entries = new TreeMap<>();

    private TestLogStorage(final List<LogEntry> entries) {
      entries.forEach(entry -> this.entries.put(entry.getId().getIndex(), entry));
    }

    @Override
    public boolean init(final LogStorageOptions options) {
      return true;
    }

    @Override
    public void shutdown() {}

    @Override
    public long getFirstLogIndex() {
      return entries.isEmpty() ? 0 : entries.firstKey();
    }

    @Override
    public long getLastLogIndex() {
      return entries.isEmpty() ? 0 : entries.lastKey();
    }

    @Override
    public LogEntry getEntry(final long index) {
      return entries.get(index);
    }

    @Override
    public long getTerm(final long index) {
      final LogEntry entry = entries.get(index);
      return entry == null ? 0 : entry.getId().getTerm();
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
      entries.put(entry.getId().getIndex(), entry);
      return true;
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
      entries.forEach(this::appendEntry);
      return entries.size();
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
      entries.headMap(firstIndexKept, false).clear();
      return true;
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
      entries.tailMap(lastIndexKept, false).clear();
      return true;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
      entries.clear();
      return true;
    }
  }
}
