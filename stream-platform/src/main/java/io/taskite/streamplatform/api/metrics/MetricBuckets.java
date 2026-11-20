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

import java.time.Duration;

/** Explicit finite histogram boundaries; Prometheus adds +Inf without clamping observations. */
final class MetricBuckets {

  static final double[] COUNT = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000, 3000, 4096, 8192, 16384, 32768, 65536
  };

  static final double[] BYTES = {
    256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, 524288, 1048576, 2097152,
    3145728, 4194304, 8388608, 16777216
  };

  static final Duration[] LATENCY = {
    Duration.ofNanos(10_000),
    Duration.ofNanos(25_000),
    Duration.ofNanos(50_000),
    Duration.ofNanos(100_000),
    Duration.ofNanos(250_000),
    Duration.ofNanos(500_000),
    Duration.ofMillis(1),
    Duration.ofNanos(2_500_000),
    Duration.ofMillis(5),
    Duration.ofMillis(10),
    Duration.ofMillis(25),
    Duration.ofMillis(50),
    Duration.ofMillis(100),
    Duration.ofMillis(250),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofMillis(2500),
    Duration.ofSeconds(5),
    Duration.ofSeconds(10),
    Duration.ofSeconds(15),
    Duration.ofSeconds(20),
    Duration.ofSeconds(30),
    Duration.ofSeconds(60)
  };

  static final Duration[] SNAPSHOT_LATENCY = {
    Duration.ofMillis(10),
    Duration.ofMillis(25),
    Duration.ofMillis(50),
    Duration.ofMillis(100),
    Duration.ofMillis(250),
    Duration.ofMillis(500),
    Duration.ofSeconds(1),
    Duration.ofMillis(2500),
    Duration.ofSeconds(5),
    Duration.ofSeconds(10),
    Duration.ofSeconds(15),
    Duration.ofSeconds(30),
    Duration.ofMinutes(1),
    Duration.ofMinutes(2),
    Duration.ofMinutes(5),
    Duration.ofMinutes(10),
    Duration.ofMinutes(15),
    Duration.ofMinutes(30)
  };

  private MetricBuckets() {}
}
