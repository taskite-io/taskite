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
package io.taskite.streamplatform.api.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.taskite.snapshot.SnapshotMetrics;
import io.taskite.snapshot.SnapshotProgress;
import io.taskite.snapshot.SnapshotResult;
import io.taskite.snapshot.SnapshotTriggerReason;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Micrometer metrics for one leader-scoped snapshot director. */
public final class MicrometerSnapshotMetrics implements SnapshotMetrics {
  public static final String TRIGGER = "taskite.snapshot.trigger";
  public static final String ATTEMPT = "taskite.snapshot.attempt";
  public static final String DURATION = "taskite.snapshot.duration";
  public static final String UNCOVERED_LOG_BYTES = "taskite.snapshot.uncovered.log.bytes";
  public static final String SNAPSHOT_SIZE_BYTES = "taskite.snapshot.size.bytes";
  public static final String RETAINED_LOG_BYTES = "taskite.raft.log.retained.bytes";
  public static final String LAST_INCLUDED_INDEX = "taskite.snapshot.last.included.index";
  public static final String RETRY = "taskite.snapshot.retry";

  private final MeterRegistry registry;
  private final Map<SnapshotTriggerReason, Counter> triggers =
      new EnumMap<>(SnapshotTriggerReason.class);
  private final Timer successfulDuration;
  private final Timer failedDuration;
  private final Counter successes;
  private final Counter failures;
  private final Counter skipped;
  private final Counter retries;
  private final AtomicLong uncoveredLogBytes = new AtomicLong();
  private final AtomicLong retainedLogBytes = new AtomicLong();
  private final AtomicLong snapshotSizeBytes = new AtomicLong();
  private final AtomicLong lastIncludedIndex = new AtomicLong();
  private final List<Meter> meters = new ArrayList<>();

  public MicrometerSnapshotMetrics(
      final MeterRegistry registry, final String clusterId, final int partitionId) {
    this.registry = Objects.requireNonNull(registry, "registry");
    final String cluster = Objects.requireNonNull(clusterId, "clusterId");
    final String partition = Integer.toString(partitionId);
    for (final SnapshotTriggerReason reason : SnapshotTriggerReason.values()) {
      final String reasonTag = reason.name().toLowerCase(Locale.ROOT);
      triggers.put(
          reason,
          register(
              Counter.builder(TRIGGER)
                  .description("Leader snapshot triggers")
                  .tag("cluster", cluster)
                  .tag("partition", partition)
                  .tag("reason", reasonTag)
                  .register(registry)));
    }
    successfulDuration = duration(registry, cluster, partition, "success");
    failedDuration = duration(registry, cluster, partition, "failure");
    successes = attempt(registry, cluster, partition, "success");
    failures = attempt(registry, cluster, partition, "failure");
    skipped = attempt(registry, cluster, partition, "skipped");
    retries =
        register(
            Counter.builder(RETRY)
                .description("Snapshot retries scheduled after an unsuccessful attempt")
                .tag("cluster", cluster)
                .tag("partition", partition)
                .register(registry));
    register(
        Gauge.builder(UNCOVERED_LOG_BYTES, uncoveredLogBytes, AtomicLong::get)
            .description("Encoded Raft log bytes not covered by the latest snapshot")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .baseUnit("bytes")
            .register(registry));
    register(
        Gauge.builder(RETAINED_LOG_BYTES, retainedLogBytes, AtomicLong::get)
            .description("Encoded bytes retained by the local Raft log")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .baseUnit("bytes")
            .register(registry));
    register(
        Gauge.builder(SNAPSHOT_SIZE_BYTES, snapshotSizeBytes, AtomicLong::get)
            .description("Physical size of the latest successfully persisted checkpoint")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .baseUnit("bytes")
            .register(registry));
    register(
        Gauge.builder(LAST_INCLUDED_INDEX, lastIncludedIndex, AtomicLong::get)
            .description("Last Raft log index included in a successful snapshot")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry));
  }

  @Override
  public void onProgress(final SnapshotProgress progress) {
    uncoveredLogBytes.set(progress.uncoveredLogBytes());
    retainedLogBytes.set(progress.retainedLogBytes());
    lastIncludedIndex.set(progress.lastSnapshotIndex());
    snapshotSizeBytes.set(progress.lastSnapshotSizeBytes());
  }

  @Override
  public void onTriggered(final SnapshotTriggerReason reason) {
    triggers.get(reason).increment();
  }

  @Override
  public void onCompleted(
      final SnapshotTriggerReason reason,
      final Duration duration,
      final SnapshotResult result,
      final Throwable error) {
    final long durationNanos = Math.max(0, duration.toNanos());
    if (error != null) {
      failures.increment();
      failedDuration.record(durationNanos, TimeUnit.NANOSECONDS);
    } else if (result != null && result.skipped()) {
      skipped.increment();
    } else {
      successes.increment();
      successfulDuration.record(durationNanos, TimeUnit.NANOSECONDS);
      if (result != null) {
        lastIncludedIndex.set(result.lastIncludedIndex());
        snapshotSizeBytes.set(result.snapshotSizeBytes());
      }
    }
  }

  @Override
  public void onRetryScheduled(final Duration delay) {
    retries.increment();
  }

  @Override
  public void close() {
    meters.forEach(registry::remove);
    meters.clear();
  }

  private Counter attempt(
      final MeterRegistry registry,
      final String cluster,
      final String partition,
      final String result) {
    return register(
        Counter.builder(ATTEMPT)
            .description("Completed snapshot attempts")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .tag("result", result)
            .register(registry));
  }

  private Timer duration(
      final MeterRegistry registry,
      final String cluster,
      final String partition,
      final String result) {
    return register(
        Timer.builder(DURATION)
            .description("End-to-end snapshot attempt duration")
            .serviceLevelObjectives(MetricBuckets.SNAPSHOT_LATENCY)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .tag("result", result)
            .register(registry));
  }

  private <T extends Meter> T register(final T meter) {
    meters.add(meter);
    return meter;
  }
}
