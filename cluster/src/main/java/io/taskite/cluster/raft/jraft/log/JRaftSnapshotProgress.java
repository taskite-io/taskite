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
import com.alipay.sofa.jraft.storage.LogStorage;
import io.taskite.snapshot.SnapshotProgress;
import io.taskite.snapshot.SnapshotProgressSource;
import io.taskite.snapshot.SnapshotProgressSubscription;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Group-scoped, restart-rebuildable byte progress for size-based snapshots. */
public final class JRaftSnapshotProgress implements SnapshotProgressSource {
  private final LogSizeIndex sizes = new LogSizeIndex();
  private final CopyOnWriteArrayList<Consumer<SnapshotProgress>> listeners =
      new CopyOnWriteArrayList<>();

  private volatile SnapshotProgress current = SnapshotProgress.EMPTY;
  private long snapshotIndex;
  private long snapshotSizeBytes;

  synchronized void rebuild(final LogStorage storage, final LogEntryEncoder encoder) {
    Objects.requireNonNull(storage, "storage");
    Objects.requireNonNull(encoder, "encoder");
    sizes.clear();
    final long first = storage.getFirstLogIndex();
    final long last = storage.getLastLogIndex();
    if (first > 0 && last >= first) {
      for (long index = first; index <= last; index++) {
        final LogEntry entry = storage.getEntry(index);
        if (entry != null) {
          sizes.put(index, encoder.encode(entry).length);
        }
      }
      if (snapshotIndex == 0) {
        snapshotIndex = Math.max(0, first - 1);
      }
    }
    publishLocked();
  }

  synchronized void onAppended(final LogEntry entry, final LogEntryEncoder encoder) {
    final LogEntry value = Objects.requireNonNull(entry, "entry");
    sizes.put(value.getId().getIndex(), encoder.encode(value).length);
    publishLocked();
  }

  synchronized void onPrefixTruncated(final long firstIndexKept) {
    sizes.removeBefore(firstIndexKept);
    snapshotIndex = Math.max(snapshotIndex, Math.max(0, firstIndexKept - 1));
    publishLocked();
  }

  synchronized void onSuffixTruncated(final long lastIndexKept) {
    sizes.removeAfter(lastIndexKept);
    if (snapshotIndex > lastIndexKept) {
      snapshotIndex = Math.max(0, lastIndexKept);
      snapshotSizeBytes = 0;
    }
    publishLocked();
  }

  public synchronized void onSnapshotPersisted(
      final long lastIncludedIndex, final long persistedSnapshotSizeBytes) {
    validateSnapshot(lastIncludedIndex, persistedSnapshotSizeBytes);
    if (lastIncludedIndex >= snapshotIndex) {
      snapshotSizeBytes = persistedSnapshotSizeBytes;
    }
    snapshotIndex = Math.max(snapshotIndex, lastIncludedIndex);
    publishLocked();
  }

  public synchronized void onSnapshotLoaded(
      final long lastIncludedIndex, final long loadedSnapshotSizeBytes) {
    validateSnapshot(lastIncludedIndex, loadedSnapshotSizeBytes);
    snapshotIndex = lastIncludedIndex;
    snapshotSizeBytes = loadedSnapshotSizeBytes;
    sizes.removeBefore(lastIncludedIndex + 1);
    publishLocked();
  }

  @Override
  public SnapshotProgress current() {
    return current;
  }

  @Override
  public SnapshotProgressSubscription subscribe(final Consumer<SnapshotProgress> listener) {
    final Consumer<SnapshotProgress> value = Objects.requireNonNull(listener, "listener");
    listeners.add(value);
    return () -> listeners.remove(value);
  }

  private void publishLocked() {
    final SnapshotProgress updated =
        new SnapshotProgress(
            sizes.lastIndex(),
            snapshotIndex,
            sizes.bytesAfter(snapshotIndex),
            sizes.totalBytes(),
            snapshotSizeBytes);
    current = updated;
    listeners.forEach(listener -> listener.accept(updated));
  }

  private static void validateSnapshot(final long index, final long sizeBytes) {
    if (index < 0 || sizeBytes < 0) {
      throw new IllegalArgumentException("snapshot index and size must not be negative");
    }
  }
}
