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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class MicrometerCommandDispatchMetricsTest {

  @Test
  void recordsPendingCommandsAndCommandPhaseDurations() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerCommandDispatchMetrics(registry, "cluster-a", 3);

    metrics.onSubmitted(4);
    metrics.onRaftAppendCompleted(2_000);
    metrics.onProcessingCompleted(7_000);

    final var pending =
        registry
            .get(MicrometerCommandDispatchMetrics.PENDING)
            .tags("cluster", "cluster-a", "partition", "3")
            .summary();
    assertEquals(1, pending.count());
    assertEquals(4, pending.totalAmount());
    assertEquals(
        2_000,
        duration(registry, MicrometerCommandDispatchMetrics.Phase.RAFT_APPEND.tagValue()),
        0.001);
    assertEquals(
        7_000,
        duration(registry, MicrometerCommandDispatchMetrics.Phase.PROCESSING.tagValue()),
        0.001);

    metrics.close();
    assertNull(registry.find(MicrometerCommandDispatchMetrics.PENDING).summary());
    assertNull(registry.find(MicrometerCommandDispatchMetrics.DURATION).timer());
    registry.close();
  }

  private double duration(final SimpleMeterRegistry registry, final String phase) {
    return registry
        .get(MicrometerCommandDispatchMetrics.DURATION)
        .tags("cluster", "cluster-a", "partition", "3", "phase", phase)
        .timer()
        .totalTime(TimeUnit.NANOSECONDS);
  }
}
