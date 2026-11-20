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
import io.taskite.snapshot.SnapshotProgress;
import io.taskite.snapshot.SnapshotResult;
import io.taskite.snapshot.SnapshotTriggerReason;
import java.time.Duration;
import org.junit.jupiter.api.Test;

final class MicrometerSnapshotMetricsTest {

  @Test
  void recordsProgressTriggersAttemptsAndRetries() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerSnapshotMetrics(registry, "cluster-a", 3);

    metrics.onProgress(new SnapshotProgress(20, 10, 4096, 8192));
    metrics.onTriggered(SnapshotTriggerReason.SIZE);
    metrics.onCompleted(
        SnapshotTriggerReason.SIZE,
        Duration.ofMillis(25),
        SnapshotResult.persisted(20, 2, 16_384),
        null);
    metrics.onCompleted(
        SnapshotTriggerReason.INTERVAL, Duration.ofMillis(1), SnapshotResult.skipped(20, 2), null);
    metrics.onRetryScheduled(Duration.ofSeconds(1));

    assertEquals(
        4096,
        registry
            .get(MicrometerSnapshotMetrics.UNCOVERED_LOG_BYTES)
            .tag("cluster", "cluster-a")
            .tag("partition", "3")
            .gauge()
            .value());
    assertEquals(
        8192,
        registry
            .get(MicrometerSnapshotMetrics.RETAINED_LOG_BYTES)
            .tag("cluster", "cluster-a")
            .tag("partition", "3")
            .gauge()
            .value());
    assertEquals(
        20,
        registry
            .get(MicrometerSnapshotMetrics.LAST_INCLUDED_INDEX)
            .tag("cluster", "cluster-a")
            .tag("partition", "3")
            .gauge()
            .value());
    assertEquals(
        16_384,
        registry
            .get(MicrometerSnapshotMetrics.SNAPSHOT_SIZE_BYTES)
            .tag("cluster", "cluster-a")
            .tag("partition", "3")
            .gauge()
            .value());
    assertEquals(
        1, registry.get(MicrometerSnapshotMetrics.TRIGGER).tag("reason", "size").counter().count());
    assertEquals(
        1,
        registry.get(MicrometerSnapshotMetrics.ATTEMPT).tag("result", "success").counter().count());
    assertEquals(
        1,
        registry.get(MicrometerSnapshotMetrics.ATTEMPT).tag("result", "skipped").counter().count());
    assertEquals(
        1,
        registry.get(MicrometerSnapshotMetrics.DURATION).tag("result", "success").timer().count());
    assertNull(registry.find(MicrometerSnapshotMetrics.DURATION).tag("result", "skipped").timer());
    assertEquals(1, registry.get(MicrometerSnapshotMetrics.RETRY).counter().count());

    metrics.close();

    assertNull(registry.find(MicrometerSnapshotMetrics.UNCOVERED_LOG_BYTES).gauge());
  }
}
