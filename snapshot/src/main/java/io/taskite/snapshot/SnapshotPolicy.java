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
import java.util.Objects;

/** Immutable trigger and retry policy for one partition's leader snapshot director. */
public record SnapshotPolicy(
    Duration interval,
    long logSizeThresholdBytes,
    Duration initialRetryDelay,
    Duration maxRetryDelay) {

  public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(15);
  public static final long DEFAULT_LOG_SIZE_THRESHOLD_BYTES = 128L * 1024 * 1024;
  public static final Duration DEFAULT_INITIAL_RETRY_DELAY = Duration.ofSeconds(1);
  public static final Duration DEFAULT_MAX_RETRY_DELAY = Duration.ofMinutes(1);

  public SnapshotPolicy {
    Objects.requireNonNull(interval, "interval");
    Objects.requireNonNull(initialRetryDelay, "initialRetryDelay");
    Objects.requireNonNull(maxRetryDelay, "maxRetryDelay");
    if (interval.isNegative()) {
      throw new IllegalArgumentException("snapshot interval must not be negative");
    }
    if (logSizeThresholdBytes < 0) {
      throw new IllegalArgumentException("snapshot log size threshold must not be negative");
    }
    if (interval.isZero() && logSizeThresholdBytes == 0) {
      throw new IllegalArgumentException(
          "snapshot interval and log size threshold must not both be zero");
    }
    requirePositive(initialRetryDelay, "initialRetryDelay");
    requirePositive(maxRetryDelay, "maxRetryDelay");
    if (initialRetryDelay.compareTo(maxRetryDelay) > 0) {
      throw new IllegalArgumentException("initialRetryDelay must not exceed maxRetryDelay");
    }
  }

  public static SnapshotPolicy defaults() {
    return of(DEFAULT_INTERVAL, DEFAULT_LOG_SIZE_THRESHOLD_BYTES);
  }

  public static SnapshotPolicy of(final Duration interval, final long logSizeThresholdBytes) {
    return new SnapshotPolicy(
        interval, logSizeThresholdBytes, DEFAULT_INITIAL_RETRY_DELAY, DEFAULT_MAX_RETRY_DELAY);
  }

  public boolean intervalEnabled() {
    return !interval.isZero();
  }

  public boolean sizeEnabled() {
    return logSizeThresholdBytes > 0;
  }

  private static void requirePositive(final Duration value, final String name) {
    if (value.isNegative() || value.isZero()) {
      throw new IllegalArgumentException(name + " must be positive");
    }
  }
}
