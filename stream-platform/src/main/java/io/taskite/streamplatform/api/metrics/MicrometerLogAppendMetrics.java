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
import java.util.Objects;

/** Micrometer distributions for the final record and byte size of atomic log appends. */
public final class MicrometerLogAppendMetrics implements LogAppendMetrics {

  public static final String BATCH_RECORDS = "taskite.logstream.append.batch.records";
  public static final String BATCH_BYTES = "taskite.logstream.append.batch.bytes";

  private final MeterRegistry registry;
  private final DistributionSummary batchRecords;
  private final DistributionSummary batchBytes;

  public MicrometerLogAppendMetrics(
      final MeterRegistry registry, final String clusterId, final int partitionId) {
    this.registry = Objects.requireNonNull(registry, "registry");
    final String cluster = Objects.requireNonNull(clusterId, "clusterId");
    final String partition = Integer.toString(partitionId);
    batchRecords =
        DistributionSummary.builder(BATCH_RECORDS)
            .description("Records carried by each atomic log append")
            .baseUnit("records")
            .serviceLevelObjectives(MetricBuckets.COUNT)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    batchBytes =
        DistributionSummary.builder(BATCH_BYTES)
            .description("Final framed payload bytes carried by each atomic log append")
            .baseUnit("bytes")
            .serviceLevelObjectives(MetricBuckets.BYTES)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
  }

  @Override
  public void onBatchAppended(final int recordCount, final int payloadBytes) {
    batchRecords.record(Math.max(0, recordCount));
    batchBytes.record(Math.max(0, payloadBytes));
  }

  @Override
  public void close() {
    registry.remove(batchRecords);
    registry.remove(batchBytes);
  }
}
