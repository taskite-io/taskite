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

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import java.util.List;
import java.util.Objects;

/** Delegates JRaft persistence while maintaining an exact encoded-byte side index. */
final class MeasuredLogStorage implements LogStorage {
  private final LogStorage delegate;
  private final JRaftSnapshotProgress progress;
  private LogEntryEncoder encoder;

  MeasuredLogStorage(final LogStorage delegate, final JRaftSnapshotProgress progress) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.progress = Objects.requireNonNull(progress, "progress");
  }

  @Override
  public boolean init(final LogStorageOptions options) {
    final boolean initialized = delegate.init(options);
    if (initialized) {
      encoder = options.getLogEntryCodecFactory().encoder();
      progress.rebuild(delegate, encoder);
    }
    return initialized;
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public long getFirstLogIndex() {
    return delegate.getFirstLogIndex();
  }

  @Override
  public long getLastLogIndex() {
    return delegate.getLastLogIndex();
  }

  @Override
  public LogEntry getEntry(final long index) {
    return delegate.getEntry(index);
  }

  @Override
  public long getTerm(final long index) {
    return delegate.getTerm(index);
  }

  @Override
  public boolean appendEntry(final LogEntry entry) {
    final boolean appended = delegate.appendEntry(entry);
    if (appended) {
      progress.onAppended(entry, encoder);
    }
    return appended;
  }

  @Override
  public int appendEntries(final List<LogEntry> entries) {
    final int appended = delegate.appendEntries(entries);
    for (int i = 0; i < Math.min(appended, entries.size()); i++) {
      progress.onAppended(entries.get(i), encoder);
    }
    return appended;
  }

  @Override
  public boolean truncatePrefix(final long firstIndexKept) {
    final boolean truncated = delegate.truncatePrefix(firstIndexKept);
    if (truncated) {
      progress.onPrefixTruncated(firstIndexKept);
    }
    return truncated;
  }

  @Override
  public boolean truncateSuffix(final long lastIndexKept) {
    final boolean truncated = delegate.truncateSuffix(lastIndexKept);
    if (truncated) {
      progress.onSuffixTruncated(lastIndexKept);
    }
    return truncated;
  }

  @Override
  public boolean reset(final long nextLogIndex) {
    final boolean reset = delegate.reset(nextLogIndex);
    if (reset) {
      progress.rebuild(delegate, encoder);
    }
    return reset;
  }
}
