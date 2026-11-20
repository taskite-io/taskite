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
package io.taskite.streamplatform.processing;

import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextFactory;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommandResponseObserver;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.command.CommandFailureObserver;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.logstream.LogBatchSizing;
import io.taskite.streamplatform.logstream.LogStreamReader;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

/**
 * Immutable wiring for a {@link StreamProcessor} instance. Bundles the collaborators the processor
 * and its state machines need, so leadership changes rebuild a processor by rebuilding this context
 * with a new {@link #mode()}.
 */
public record StreamProcessorContext(
    Configuration configuration, Processing processing, State state, Log log, Observers observers) {

  public StreamProcessorContext {
    Objects.requireNonNull(configuration, "configuration");
    Objects.requireNonNull(processing, "processing");
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(log, "log");
    Objects.requireNonNull(observers, "observers");
  }

  public record Configuration(Identity identity, Batching batching) {
    public Configuration {
      Objects.requireNonNull(identity, "identity");
      Objects.requireNonNull(batching, "batching");
    }
  }

  public record Identity(
      long clusterKey, int partitionId, String actorName, StreamProcessorMode mode) {}

  public long clusterKey() {
    return configuration.identity().clusterKey();
  }

  public record Batching(
      int maxCommandsInBatch, int maxRecordsInBatch, Duration maxBatchDuration, int maxBatchBytes) {
    public Batching {
      if (maxCommandsInBatch < 1) {
        throw new IllegalArgumentException("maxCommandsInBatch must be at least 1");
      }
      if (maxRecordsInBatch < 0) {
        throw new IllegalArgumentException("maxRecordsInBatch must not be negative");
      }
      if (maxBatchDuration == null || maxBatchDuration.isNegative()) {
        throw new IllegalArgumentException("maxBatchDuration must not be null or negative");
      }
      LogBatchSizing.validateMaxBatchBytes(maxBatchBytes);
    }
  }

  public record Processing(
      TypedEventRegistry eventRegistry,
      Function<TypedRecordProcessorContext, TypedRecordProcessors> typedRecordProcessorsFactory,
      TypedRecordProcessorContextFactory processorContextFactory) {}

  public record State(
      ProcessingStateStore storage,
      LongSupplier committedPositionSupplier,
      LongConsumer appliedPositionConsumer) {}

  public record Log(LogStreamReader reader, RecordAppender appender) {}

  public record Observers(
      CommandFailureObserver failureObserver,
      CommandResponseObserver commandResponseObserver,
      CommittedRecordObserver committedRecordObserver,
      ProcessingMetrics.Factory processingMetricsFactory) {}

  public int partitionId() {
    return configuration.identity().partitionId();
  }

  public String actorName() {
    return configuration.identity().actorName();
  }

  public StreamProcessorMode mode() {
    return configuration.identity().mode();
  }

  public TypedEventRegistry eventRegistry() {
    return processing.eventRegistry();
  }

  public Function<TypedRecordProcessorContext, TypedRecordProcessors>
      typedRecordProcessorsFactory() {
    return processing.typedRecordProcessorsFactory();
  }

  public TypedRecordProcessorContextFactory processorContextFactory() {
    return processing.processorContextFactory();
  }

  public ProcessingStateStore storage() {
    return state.storage();
  }

  public LongSupplier committedPositionSupplier() {
    return state.committedPositionSupplier();
  }

  public LongConsumer appliedPositionConsumer() {
    return state.appliedPositionConsumer();
  }

  public LogStreamReader reader() {
    return log.reader();
  }

  public RecordAppender appender() {
    return log.appender();
  }

  public CommandFailureObserver failureObserver() {
    return observers.failureObserver();
  }

  public CommandResponseObserver commandResponseObserver() {
    return observers.commandResponseObserver();
  }

  public CommittedRecordObserver committedRecordObserver() {
    return observers.committedRecordObserver();
  }

  public ProcessingMetrics.Factory processingMetricsFactory() {
    return observers.processingMetricsFactory();
  }

  public int maxCommandsInBatch() {
    return configuration.batching().maxCommandsInBatch();
  }

  public int maxRecordsInBatch() {
    return configuration.batching().maxRecordsInBatch();
  }

  public Duration maxBatchDuration() {
    return configuration.batching().maxBatchDuration();
  }

  public int maxBatchBytes() {
    return configuration.batching().maxBatchBytes();
  }
}
