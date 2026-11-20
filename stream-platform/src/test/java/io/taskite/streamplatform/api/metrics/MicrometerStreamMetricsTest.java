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
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class MicrometerStreamMetricsTest {

  @Test
  void recordsCommittedStreamProcessorActions() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerStreamProcessorMetrics(registry, "cluster-a", 3);

    metrics.onProcessed(Collections.nCopies(2, (LoggedEvent) null));
    metrics.onSkipped();
    metrics.onSkipped();
    metrics.onRecordPositions(17, 23);
    metrics.onBatchProcessed(3, 2, 1, 3, TimeUnit.MILLISECONDS.toNanos(5));
    metrics.onBatchProcessed(1, 0, 0, 0, TimeUnit.MILLISECONDS.toNanos(7));
    metrics.onBatchRetried();
    metrics.onBatchRetried();
    metrics.onBatchResult(ProcessingMetrics.BatchResultState.RECORDS);
    metrics.onBatchResult(ProcessingMetrics.BatchResultState.RESPONSE_ONLY);
    metrics.onPostCommitCompleted(TimeUnit.MILLISECONDS.toNanos(3));
    metrics.onPhaseCompleted(
        ProcessingMetrics.Phase.STATE_COMMIT, TimeUnit.MILLISECONDS.toNanos(5));

    assertEquals(1, count(registry, "processed"));
    assertEquals(2, count(registry, "written"));
    assertEquals(2, count(registry, "skipped"));
    assertEquals(17, recordCount(registry, "processed"));
    assertEquals(23, recordCount(registry, "committed"));
    assertEquals(6, recordCount(registry, "pending"));
    assertEquals(
        2, registry.get(MicrometerStreamProcessorMetrics.BATCH_COMMANDS).summary().count());
    assertEquals(
        4, registry.get(MicrometerStreamProcessorMetrics.BATCH_COMMANDS).summary().totalAmount());
    assertEquals(3, registry.get(MicrometerStreamProcessorMetrics.BATCH_COMMANDS).summary().max());
    assertEquals(2, registry.get(MicrometerStreamProcessorMetrics.BATCH_DURATION).timer().count());
    assertEquals(
        12,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_DURATION)
            .timer()
            .totalTime(TimeUnit.MILLISECONDS));
    assertEquals(
        2,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_CONTINUATIONS)
            .tag("reason", "command_limit")
            .counter()
            .count());
    assertEquals(
        1,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_CONTINUATIONS)
            .tag("reason", "record_limit")
            .counter()
            .count());
    assertEquals(
        3,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_CONTINUATIONS)
            .tag("reason", "time_limit")
            .counter()
            .count());
    assertEquals(2, registry.get(MicrometerStreamProcessorMetrics.BATCH_RETRIES).counter().count());
    assertEquals(
        1,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_RESULTS)
            .tag("state", "records")
            .counter()
            .count());
    assertEquals(
        1,
        registry
            .get(MicrometerStreamProcessorMetrics.BATCH_RESULTS)
            .tag("state", "response_only")
            .counter()
            .count());
    assertEquals(
        3,
        registry
            .get(MicrometerStreamProcessorMetrics.POST_COMMIT_DURATION)
            .timer()
            .totalTime(TimeUnit.MILLISECONDS));
    assertEquals(
        5,
        registry
            .get(MicrometerStreamProcessorMetrics.PHASE_DURATION)
            .tag("phase", "state_commit")
            .timer()
            .totalTime(TimeUnit.MILLISECONDS));

    metrics.close();
    assertNull(registry.find(MicrometerStreamProcessorMetrics.RECORDS).counter());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.RECORD_COUNT).gauge());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.BATCH_COMMANDS).summary());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.BATCH_DURATION).timer());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.BATCH_CONTINUATIONS).counter());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.BATCH_RETRIES).counter());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.BATCH_RESULTS).counter());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.POST_COMMIT_DURATION).timer());
    assertNull(registry.find(MicrometerStreamProcessorMetrics.PHASE_DURATION).timer());
    registry.close();
  }

  private double count(final SimpleMeterRegistry registry, final String action) {
    return registry
        .get(MicrometerStreamProcessorMetrics.RECORDS)
        .tags("cluster", "cluster-a", "partition", "3", "action", action)
        .counter()
        .count();
  }

  private double recordCount(final SimpleMeterRegistry registry, final String state) {
    return registry
        .get(MicrometerStreamProcessorMetrics.RECORD_COUNT)
        .tags("cluster", "cluster-a", "partition", "3", "state", state)
        .gauge()
        .value();
  }
}
