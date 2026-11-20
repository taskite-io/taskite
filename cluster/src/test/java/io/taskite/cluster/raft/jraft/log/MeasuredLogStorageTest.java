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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

final class MeasuredLogStorageTest {

  @Test
  void updatesProgressOnlyForEntriesPersistedByTheDelegate() {
    final TestLogStorage delegate = new TestLogStorage();
    final JRaftSnapshotProgress progress = new JRaftSnapshotProgress();
    final MeasuredLogStorage measured = measured(delegate, progress);

    delegate.appendAllowed = false;
    assertFalse(measured.appendEntry(dataEntry(1, 16)));
    assertEquals(0, progress.current().retainedLogBytes());

    delegate.appendAllowed = true;
    assertTrue(measured.appendEntry(dataEntry(1, 16)));
    final long firstEntryBytes = progress.current().retainedLogBytes();
    assertTrue(firstEntryBytes > 16);

    delegate.appendEntriesLimit = 1;
    assertEquals(1, measured.appendEntries(List.of(dataEntry(2, 32), dataEntry(3, 64))));
    assertEquals(2, progress.current().lastLogIndex());
    assertTrue(progress.current().retainedLogBytes() > firstEntryBytes);
  }

  @Test
  void measuresConfigurationEntriesAndSuccessfulTruncationAndReset() {
    final TestLogStorage delegate = new TestLogStorage();
    final JRaftSnapshotProgress progress = new JRaftSnapshotProgress();
    final MeasuredLogStorage measured = measured(delegate, progress);

    assertTrue(measured.appendEntry(configurationEntry(1)));
    assertTrue(measured.appendEntry(dataEntry(2, 32)));
    assertTrue(progress.current().retainedLogBytes() > 0);

    delegate.truncationAllowed = false;
    final long retained = progress.current().retainedLogBytes();
    assertFalse(measured.truncatePrefix(2));
    assertEquals(retained, progress.current().retainedLogBytes());

    delegate.truncationAllowed = true;
    assertTrue(measured.truncatePrefix(2));
    assertEquals(2, progress.current().lastLogIndex());
    assertTrue(progress.current().retainedLogBytes() < retained);

    assertTrue(measured.reset(3));
    assertEquals(0, progress.current().retainedLogBytes());
  }

  private static MeasuredLogStorage measured(
      final TestLogStorage delegate, final JRaftSnapshotProgress progress) {
    final MeasuredLogStorage measured = new MeasuredLogStorage(delegate, progress);
    final LogStorageOptions options = new LogStorageOptions();
    options.setLogEntryCodecFactory(LogEntryV2CodecFactory.getInstance());
    assertTrue(measured.init(options));
    return measured;
  }

  private static LogEntry dataEntry(final long index, final int bytes) {
    final LogEntry entry = new LogEntry(EntryType.ENTRY_TYPE_DATA);
    entry.setId(new LogId(index, 1));
    entry.setData(ByteBuffer.allocate(bytes));
    return entry;
  }

  private static LogEntry configurationEntry(final long index) {
    final LogEntry entry = new LogEntry(EntryType.ENTRY_TYPE_CONFIGURATION);
    entry.setId(new LogId(index, 1));
    entry.setPeers(List.of(new PeerId("127.0.0.1", 26502)));
    return entry;
  }

  private static final class TestLogStorage implements LogStorage {
    private final NavigableMap<Long, LogEntry> entries = new TreeMap<>();
    private boolean appendAllowed = true;
    private int appendEntriesLimit = Integer.MAX_VALUE;
    private boolean truncationAllowed = true;

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
      if (!appendAllowed) {
        return false;
      }
      entries.put(entry.getId().getIndex(), entry);
      return true;
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
      if (!appendAllowed) {
        return 0;
      }
      final int appended = Math.min(appendEntriesLimit, entries.size());
      for (int index = 0; index < appended; index++) {
        appendEntry(entries.get(index));
      }
      return appended;
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
      if (!truncationAllowed) {
        return false;
      }
      entries.headMap(firstIndexKept, false).clear();
      return true;
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
      if (!truncationAllowed) {
        return false;
      }
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
