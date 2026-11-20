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

import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Observes stream processing on the partition actor. Committed outcome callbacks run only after the
 * processing transaction commits; retry callbacks run after the failed transaction rolls back,
 * while position callbacks publish the latest committed and consumed log high-water marks.
 */
public interface ProcessingMetrics extends AutoCloseable {

  ProcessingMetrics NOOP = new ProcessingMetrics() {};

  default void onRecovered() {}

  default void onProcessed(final List<LoggedEvent> writtenRecords) {}

  default void onBatchProcessed(
      final int commandCount,
      final int commandLimitContinuationCount,
      final int recordLimitContinuationCount,
      final int timeLimitContinuationCount,
      final long durationNanos) {}

  default void onBatchRetried() {}

  default void onBatchResult(final BatchResultState state) {}

  default void onPostCommitCompleted(final long durationNanos) {}

  default void onPhaseCompleted(final Phase phase, final long durationNanos) {}

  default void onSkipped() {}

  default void onRecordPositions(final long processedPosition, final long committedPosition) {}

  @Override
  default void close() {}

  static ProcessingMetrics combine(final ProcessingMetrics... metrics) {
    final List<ProcessingMetrics> delegates =
        Arrays.stream(metrics).map(metric -> Objects.requireNonNull(metric, "metric")).toList();
    return new ProcessingMetrics() {
      @Override
      public void onRecovered() {
        delegates.forEach(ProcessingMetrics::onRecovered);
      }

      @Override
      public void onProcessed(final List<LoggedEvent> writtenRecords) {
        delegates.forEach(metric -> metric.onProcessed(writtenRecords));
      }

      @Override
      public void onBatchProcessed(
          final int commandCount,
          final int commandLimitContinuationCount,
          final int recordLimitContinuationCount,
          final int timeLimitContinuationCount,
          final long durationNanos) {
        delegates.forEach(
            metric ->
                metric.onBatchProcessed(
                    commandCount,
                    commandLimitContinuationCount,
                    recordLimitContinuationCount,
                    timeLimitContinuationCount,
                    durationNanos));
      }

      @Override
      public void onBatchRetried() {
        delegates.forEach(ProcessingMetrics::onBatchRetried);
      }

      @Override
      public void onBatchResult(final BatchResultState state) {
        delegates.forEach(metric -> metric.onBatchResult(state));
      }

      @Override
      public void onPostCommitCompleted(final long durationNanos) {
        delegates.forEach(metric -> metric.onPostCommitCompleted(durationNanos));
      }

      @Override
      public void onPhaseCompleted(final Phase phase, final long durationNanos) {
        delegates.forEach(metric -> metric.onPhaseCompleted(phase, durationNanos));
      }

      @Override
      public void onSkipped() {
        delegates.forEach(ProcessingMetrics::onSkipped);
      }

      @Override
      public void onRecordPositions(final long processedPosition, final long committedPosition) {
        delegates.forEach(metric -> metric.onRecordPositions(processedPosition, committedPosition));
      }

      @Override
      public void close() {
        delegates.reversed().forEach(ProcessingMetrics::close);
      }
    };
  }

  enum BatchResultState {
    RECORDS("records"),
    RESPONSE_ONLY("response_only"),
    POST_COMMIT_ONLY("post_commit_only"),
    EMPTY("empty");

    private final String tagValue;

    BatchResultState(final String tagValue) {
      this.tagValue = tagValue;
    }

    public String tagValue() {
      return tagValue;
    }
  }

  enum Phase {
    QUEUE_WAIT("queue_wait"),
    RESULT_RAFT_COMMIT("result_raft_commit"),
    STATE_COMMIT("state_commit"),
    RESPONSE_DELIVERY("response_delivery");

    private final String tagValue;

    Phase(final String tagValue) {
      this.tagValue = tagValue;
    }

    public String tagValue() {
      return tagValue;
    }
  }

  @FunctionalInterface
  interface Factory {
    ProcessingMetrics create(int partitionId, TypedRecordProcessorContext context);

    static Factory noop() {
      return (partitionId, context) -> NOOP;
    }
  }
}
