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

import java.util.Objects;

/** Coordinates durably covered by a completed snapshot. */
public record SnapshotResult(
    long lastIncludedIndex, long lastIncludedTerm, long snapshotSizeBytes, Outcome outcome) {
  public SnapshotResult {
    if (lastIncludedIndex < 0 || lastIncludedTerm < 0 || snapshotSizeBytes < 0) {
      throw new IllegalArgumentException("snapshot result values must not be negative");
    }
    Objects.requireNonNull(outcome, "outcome");
  }

  public static SnapshotResult persisted(
      final long lastIncludedIndex, final long lastIncludedTerm, final long snapshotSizeBytes) {
    return new SnapshotResult(
        lastIncludedIndex, lastIncludedTerm, snapshotSizeBytes, Outcome.PERSISTED);
  }

  public static SnapshotResult skipped(final long lastIncludedIndex, final long lastIncludedTerm) {
    return new SnapshotResult(lastIncludedIndex, lastIncludedTerm, 0, Outcome.SKIPPED);
  }

  public boolean skipped() {
    return outcome == Outcome.SKIPPED;
  }

  public enum Outcome {
    PERSISTED,
    SKIPPED
  }
}
