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

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Micrometer distributions for command queue depth and Raft/processing completion latency. */
public final class MicrometerCommandDispatchMetrics implements CommandDispatchMetrics {

  public static final String PENDING = "taskite.stream.partition.command.pending";
  public static final String DURATION = "taskite.stream.partition.command.duration";

  public enum Phase {
    RAFT_APPEND,
    PROCESSING;

    public String tagValue() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final MeterRegistry registry;
  private final DistributionSummary pending;
  private final Timer raftAppend;
  private final Timer processing;

  public MicrometerCommandDispatchMetrics(
      final MeterRegistry registry, final String clusterId, final int partitionId) {
    this.registry = Objects.requireNonNull(registry, "registry");
    final String cluster = Objects.requireNonNull(clusterId, "clusterId");
    final String partition = Integer.toString(partitionId);
    pending =
        DistributionSummary.builder(PENDING)
            .description("Pending partition commands observed when a command is submitted")
            .serviceLevelObjectives(MetricBuckets.COUNT)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    raftAppend = timer(cluster, partition, Phase.RAFT_APPEND);
    processing = timer(cluster, partition, Phase.PROCESSING);
  }

  @Override
  public void onSubmitted(final int pendingCommands) {
    pending.record(Math.max(0, pendingCommands));
  }

  @Override
  public void onRaftAppendCompleted(final long durationNanos) {
    raftAppend.record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
  }

  @Override
  public void onProcessingCompleted(final long durationNanos) {
    processing.record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
  }

  @Override
  public void close() {
    registry.remove(pending);
    registry.remove(raftAppend);
    registry.remove(processing);
  }

  private Timer timer(final String clusterId, final String partitionId, final Phase phase) {
    return Timer.builder(DURATION)
        .description("Partition command completion latency split by Raft and processing phase")
        .serviceLevelObjectives(MetricBuckets.LATENCY)
        .tag("cluster", clusterId)
        .tag("partition", partitionId)
        .tag("phase", phase.tagValue())
        .register(registry);
  }
}
