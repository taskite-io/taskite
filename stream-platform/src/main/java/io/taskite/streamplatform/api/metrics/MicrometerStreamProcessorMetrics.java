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
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Micrometer counters and record high-water gauges for a stream processor partition. */
public final class MicrometerStreamProcessorMetrics implements ProcessingMetrics {

  private enum RecordAction {
    PROCESSED,
    SKIPPED,
    WRITTEN
  }

  private enum RecordState {
    PROCESSED,
    COMMITTED,
    PENDING
  }

  private enum ContinuationReason {
    COMMAND_LIMIT,
    RECORD_LIMIT,
    TIME_LIMIT
  }

  private static String tagValue(final Enum<?> value) {
    return value.name().toLowerCase(Locale.ROOT);
  }

  /** Micrometer base name. Prometheus exposes counters with the {@code _total} suffix. */
  public static final String RECORDS = "taskite_stream_processor_records";

  public static final String RECORDS_TOTAL = RECORDS + "_total";
  public static final String RECORD_COUNT = "taskite.stream.processor.record.count";
  public static final String BATCH_COMMANDS = "taskite.stream.processor.batch.commands";
  public static final String BATCH_DURATION = "taskite.stream.processor.batch.duration";
  public static final String BATCH_CONTINUATIONS = "taskite.stream.processor.batch.continuations";
  public static final String BATCH_RETRIES = "taskite.stream.processor.batch.retries";
  public static final String BATCH_RESULTS = "taskite.stream.processor.batch.results";
  public static final String POST_COMMIT_DURATION = "taskite.stream.processor.post.commit.duration";
  public static final String PHASE_DURATION = "taskite.stream.processor.phase.duration";

  private final MeterRegistry registry;
  private final Counter processed;
  private final Counter skipped;
  private final Counter written;
  private final DistributionSummary batchCommands;
  private final Timer batchDuration;
  private final Counter commandLimitContinuations;
  private final Counter recordLimitContinuations;
  private final Counter timeLimitContinuations;
  private final Counter batchRetries;
  private final Map<BatchResultState, Counter> batchResults = new EnumMap<>(BatchResultState.class);
  private final Timer postCommitDuration;
  private final Map<Phase, Timer> phaseDurations = new EnumMap<>(Phase.class);
  private final AtomicLong processedRecordCount = new AtomicLong();
  private final AtomicLong committedRecordCount = new AtomicLong();
  private final AtomicLong pendingRecordCount = new AtomicLong();
  private final Gauge processedRecords;
  private final Gauge committedRecords;
  private final Gauge pendingRecords;

  public MicrometerStreamProcessorMetrics(
      final MeterRegistry registry, final String clusterId, final int partitionId) {
    this.registry = Objects.requireNonNull(registry, "registry");
    final String cluster = Objects.requireNonNull(clusterId, "clusterId");
    processed = counter(cluster, partitionId, RecordAction.PROCESSED);
    skipped = counter(cluster, partitionId, RecordAction.SKIPPED);
    written = counter(cluster, partitionId, RecordAction.WRITTEN);
    final String partition = Integer.toString(partitionId);
    batchCommands =
        DistributionSummary.builder(BATCH_COMMANDS)
            .description("Commands executed in each successfully committed processing batch")
            .baseUnit("commands")
            .serviceLevelObjectives(MetricBuckets.COUNT)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    batchDuration =
        Timer.builder(BATCH_DURATION)
            .description("Engine execution time of successfully committed processing batches")
            .serviceLevelObjectives(MetricBuckets.LATENCY)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    commandLimitContinuations =
        continuationCounter(cluster, partition, ContinuationReason.COMMAND_LIMIT);
    recordLimitContinuations =
        continuationCounter(cluster, partition, ContinuationReason.RECORD_LIMIT);
    timeLimitContinuations = continuationCounter(cluster, partition, ContinuationReason.TIME_LIMIT);
    batchRetries =
        Counter.builder(BATCH_RETRIES)
            .description("Processing batches retried after exceeding the payload limit")
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    for (final BatchResultState state : BatchResultState.values()) {
      batchResults.put(
          state,
          Counter.builder(BATCH_RESULTS)
              .description("Successfully committed processing batches by result shape")
              .tag("cluster", cluster)
              .tag("partition", partition)
              .tag("state", state.tagValue())
              .register(registry));
    }
    postCommitDuration =
        Timer.builder(POST_COMMIT_DURATION)
            .description("Time spent running actions after the processing transaction commits")
            .serviceLevelObjectives(MetricBuckets.LATENCY)
            .tag("cluster", cluster)
            .tag("partition", partition)
            .register(registry);
    for (final Phase phase : Phase.values()) {
      phaseDurations.put(
          phase,
          Timer.builder(PHASE_DURATION)
              .description("Internal stream processing phase duration")
              .serviceLevelObjectives(MetricBuckets.LATENCY)
              .tag("cluster", cluster)
              .tag("partition", partition)
              .tag("phase", phase.tagValue())
              .register(registry));
    }
    processedRecords = gauge(cluster, partitionId, RecordState.PROCESSED, processedRecordCount);
    committedRecords = gauge(cluster, partitionId, RecordState.COMMITTED, committedRecordCount);
    pendingRecords = gauge(cluster, partitionId, RecordState.PENDING, pendingRecordCount);
  }

  @Override
  public void onProcessed(final List<LoggedEvent> writtenRecords) {
    processed.increment();
    if (!writtenRecords.isEmpty()) {
      written.increment(writtenRecords.size());
    }
  }

  @Override
  public void onBatchProcessed(
      final int commandCount,
      final int commandLimitContinuationCount,
      final int recordLimitContinuationCount,
      final int timeLimitContinuationCount,
      final long durationNanos) {
    batchCommands.record(Math.max(0, commandCount));
    batchDuration.record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
    if (commandLimitContinuationCount > 0) {
      commandLimitContinuations.increment(commandLimitContinuationCount);
    }
    if (recordLimitContinuationCount > 0) {
      recordLimitContinuations.increment(recordLimitContinuationCount);
    }
    if (timeLimitContinuationCount > 0) {
      timeLimitContinuations.increment(timeLimitContinuationCount);
    }
  }

  @Override
  public void onBatchRetried() {
    batchRetries.increment();
  }

  @Override
  public void onBatchResult(final BatchResultState state) {
    batchResults.get(state).increment();
  }

  @Override
  public void onPostCommitCompleted(final long durationNanos) {
    postCommitDuration.record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
  }

  @Override
  public void onPhaseCompleted(final Phase phase, final long durationNanos) {
    phaseDurations.get(phase).record(Math.max(0, durationNanos), TimeUnit.NANOSECONDS);
  }

  @Override
  public void onSkipped() {
    skipped.increment();
  }

  @Override
  public void onRecordPositions(final long processedPosition, final long committedPosition) {
    final long processedCount = Math.max(0, processedPosition);
    final long committedCount = Math.max(0, committedPosition);
    processedRecordCount.set(processedCount);
    committedRecordCount.set(committedCount);
    pendingRecordCount.set(Math.max(0, committedCount - processedCount));
  }

  @Override
  public void close() {
    registry.remove(processed);
    registry.remove(skipped);
    registry.remove(written);
    registry.remove(batchCommands);
    registry.remove(batchDuration);
    registry.remove(commandLimitContinuations);
    registry.remove(recordLimitContinuations);
    registry.remove(timeLimitContinuations);
    registry.remove(batchRetries);
    batchResults.values().forEach(registry::remove);
    registry.remove(postCommitDuration);
    phaseDurations.values().forEach(registry::remove);
    registry.remove(processedRecords);
    registry.remove(committedRecords);
    registry.remove(pendingRecords);
  }

  private Counter counter(
      final String clusterId, final int partitionId, final RecordAction action) {
    return Counter.builder(RECORDS)
        .description("The number of records processed by the stream processor")
        .tag("cluster", clusterId)
        .tag("partition", Integer.toString(partitionId))
        .tag("action", tagValue(action))
        .register(registry);
  }

  private Counter continuationCounter(
      final String clusterId, final String partition, final ContinuationReason reason) {
    return Counter.builder(BATCH_CONTINUATIONS)
        .description("Follow-up commands deferred to a later processing batch")
        .tag("cluster", clusterId)
        .tag("partition", partition)
        .tag("reason", tagValue(reason))
        .register(registry);
  }

  private Gauge gauge(
      final String clusterId,
      final int partitionId,
      final RecordState state,
      final AtomicLong value) {
    return Gauge.builder(RECORD_COUNT, value, AtomicLong::get)
        .description("Current number of processed, committed, or pending records")
        .tag("cluster", clusterId)
        .tag("partition", Integer.toString(partitionId))
        .tag("state", tagValue(state))
        .register(registry);
  }
}
