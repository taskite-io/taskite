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
package io.taskite.snapshot;

/** Current retained-log and snapshot coverage progress for one raft group. */
public record SnapshotProgress(
    long lastLogIndex,
    long lastSnapshotIndex,
    long uncoveredLogBytes,
    long retainedLogBytes,
    long lastSnapshotSizeBytes) {

  public static final SnapshotProgress EMPTY = new SnapshotProgress(0, 0, 0, 0, 0);

  public SnapshotProgress(
      final long lastLogIndex,
      final long lastSnapshotIndex,
      final long uncoveredLogBytes,
      final long retainedLogBytes) {
    this(lastLogIndex, lastSnapshotIndex, uncoveredLogBytes, retainedLogBytes, 0);
  }

  public SnapshotProgress {
    if (lastLogIndex < 0
        || lastSnapshotIndex < 0
        || uncoveredLogBytes < 0
        || retainedLogBytes < 0
        || lastSnapshotSizeBytes < 0) {
      throw new IllegalArgumentException("snapshot progress values must not be negative");
    }
  }

  public boolean hasUncoveredEntries() {
    return lastLogIndex > lastSnapshotIndex && uncoveredLogBytes > 0;
  }
}
