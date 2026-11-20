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
import org.junit.jupiter.api.Test;

final class MicrometerLogAppendMetricsTest {

  @Test
  void recordsFinalLogBatchShape() {
    final var registry = new SimpleMeterRegistry();
    final var metrics = new MicrometerLogAppendMetrics(registry, "cluster-a", 3);

    metrics.onBatchAppended(4, 8192);
    metrics.onBatchAppended(1, 512);

    assertEquals(2, registry.get(MicrometerLogAppendMetrics.BATCH_RECORDS).summary().count());
    assertEquals(5, registry.get(MicrometerLogAppendMetrics.BATCH_RECORDS).summary().totalAmount());
    assertEquals(4, registry.get(MicrometerLogAppendMetrics.BATCH_RECORDS).summary().max());
    assertEquals(2, registry.get(MicrometerLogAppendMetrics.BATCH_BYTES).summary().count());
    assertEquals(
        8704, registry.get(MicrometerLogAppendMetrics.BATCH_BYTES).summary().totalAmount());
    assertEquals(8192, registry.get(MicrometerLogAppendMetrics.BATCH_BYTES).summary().max());

    metrics.close();
    assertNull(registry.find(MicrometerLogAppendMetrics.BATCH_RECORDS).summary());
    assertNull(registry.find(MicrometerLogAppendMetrics.BATCH_BYTES).summary());
    registry.close();
  }
}
