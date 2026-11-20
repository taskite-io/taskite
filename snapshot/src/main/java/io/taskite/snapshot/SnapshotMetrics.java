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

import java.time.Duration;

/** Metrics port for one leader-scoped snapshot director. */
public interface SnapshotMetrics extends AutoCloseable {

  SnapshotMetrics NOOP = new SnapshotMetrics() {};

  default void onProgress(final SnapshotProgress progress) {}

  default void onTriggered(final SnapshotTriggerReason reason) {}

  default void onCompleted(
      final SnapshotTriggerReason reason,
      final Duration duration,
      final SnapshotResult result,
      final Throwable error) {}

  default void onRetryScheduled(final Duration delay) {}

  @Override
  default void close() {}

  @FunctionalInterface
  interface Factory {
    SnapshotMetrics create(int partitionId);

    static Factory noop() {
      return partitionId -> NOOP;
    }
  }
}
