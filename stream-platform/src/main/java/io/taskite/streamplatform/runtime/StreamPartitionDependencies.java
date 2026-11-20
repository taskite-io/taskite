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
package io.taskite.streamplatform.runtime;

import io.taskite.protocol.record.intent.Intent;
import io.taskite.snapshot.SnapshotLifecycleListener;
import io.taskite.snapshot.SnapshotMetrics;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.metrics.CommandDispatchMetrics;
import io.taskite.streamplatform.api.metrics.LogAppendMetrics;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContextFactory;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import java.util.Objects;
import java.util.function.Function;

/**
 * Domain-specific collaborators used by the shared stream raft-group runtime. Broker and PD provide
 * different registries/processors/state contexts while sharing the same lifecycle.
 */
public record StreamPartitionDependencies(
    long clusterKey,
    String actorNamePrefix,
    TypedEventRegistry eventRegistry,
    Function<TypedRecordProcessorContext, TypedRecordProcessors> typedRecordProcessorsFactory,
    TypedRecordProcessorContextFactory processorContextFactory,
    CommittedRecordObserver committedRecordObserver,
    SnapshotLifecycleListener snapshotLifecycleListener,
    ProcessingMetrics.Factory processingMetricsFactory,
    CommandDispatchMetrics.Factory commandDispatchMetricsFactory,
    LogAppendMetrics.Factory logAppendMetricsFactory,
    SnapshotMetrics.Factory snapshotMetricsFactory,
    Function<Integer, PartitionRequestLimiter<Intent>> requestLimiterFactory) {

  public StreamPartitionDependencies(
      final long clusterKey,
      final String actorNamePrefix,
      final TypedEventRegistry eventRegistry,
      final Function<TypedRecordProcessorContext, TypedRecordProcessors>
          typedRecordProcessorsFactory,
      final TypedRecordProcessorContextFactory processorContextFactory,
      final CommittedRecordObserver committedRecordObserver,
      final SnapshotLifecycleListener snapshotLifecycleListener,
      final ProcessingMetrics.Factory processingMetricsFactory,
      final CommandDispatchMetrics.Factory commandDispatchMetricsFactory,
      final LogAppendMetrics.Factory logAppendMetricsFactory,
      final SnapshotMetrics.Factory snapshotMetricsFactory) {
    this(
        clusterKey,
        actorNamePrefix,
        eventRegistry,
        typedRecordProcessorsFactory,
        processorContextFactory,
        committedRecordObserver,
        snapshotLifecycleListener,
        processingMetricsFactory,
        commandDispatchMetricsFactory,
        logAppendMetricsFactory,
        snapshotMetricsFactory,
        null);
  }

  public StreamPartitionDependencies(
      final long clusterKey,
      final String actorNamePrefix,
      final TypedEventRegistry eventRegistry,
      final Function<TypedRecordProcessorContext, TypedRecordProcessors>
          typedRecordProcessorsFactory,
      final TypedRecordProcessorContextFactory processorContextFactory,
      final CommittedRecordObserver committedRecordObserver) {
    this(
        clusterKey,
        actorNamePrefix,
        eventRegistry,
        typedRecordProcessorsFactory,
        processorContextFactory,
        committedRecordObserver,
        SnapshotLifecycleListener.noop(),
        ProcessingMetrics.Factory.noop(),
        CommandDispatchMetrics.Factory.noop(),
        LogAppendMetrics.Factory.noop(),
        SnapshotMetrics.Factory.noop());
  }

  public StreamPartitionDependencies(
      final long clusterKey,
      final String actorNamePrefix,
      final TypedEventRegistry eventRegistry,
      final Function<TypedRecordProcessorContext, TypedRecordProcessors>
          typedRecordProcessorsFactory,
      final TypedRecordProcessorContextFactory processorContextFactory,
      final CommittedRecordObserver committedRecordObserver,
      final SnapshotLifecycleListener snapshotLifecycleListener) {
    this(
        clusterKey,
        actorNamePrefix,
        eventRegistry,
        typedRecordProcessorsFactory,
        processorContextFactory,
        committedRecordObserver,
        snapshotLifecycleListener,
        ProcessingMetrics.Factory.noop(),
        CommandDispatchMetrics.Factory.noop(),
        LogAppendMetrics.Factory.noop(),
        SnapshotMetrics.Factory.noop());
  }

  public StreamPartitionDependencies(
      final long clusterKey,
      final String actorNamePrefix,
      final TypedEventRegistry eventRegistry,
      final Function<TypedRecordProcessorContext, TypedRecordProcessors>
          typedRecordProcessorsFactory,
      final TypedRecordProcessorContextFactory processorContextFactory,
      final CommittedRecordObserver committedRecordObserver,
      final SnapshotLifecycleListener snapshotLifecycleListener,
      final ProcessingMetrics.Factory processingMetricsFactory) {
    this(
        clusterKey,
        actorNamePrefix,
        eventRegistry,
        typedRecordProcessorsFactory,
        processorContextFactory,
        committedRecordObserver,
        snapshotLifecycleListener,
        processingMetricsFactory,
        CommandDispatchMetrics.Factory.noop(),
        LogAppendMetrics.Factory.noop(),
        SnapshotMetrics.Factory.noop());
  }

  public StreamPartitionDependencies(
      final long clusterKey,
      final String actorNamePrefix,
      final TypedEventRegistry eventRegistry,
      final Function<TypedRecordProcessorContext, TypedRecordProcessors>
          typedRecordProcessorsFactory,
      final TypedRecordProcessorContextFactory processorContextFactory,
      final CommittedRecordObserver committedRecordObserver,
      final SnapshotLifecycleListener snapshotLifecycleListener,
      final ProcessingMetrics.Factory processingMetricsFactory,
      final CommandDispatchMetrics.Factory commandDispatchMetricsFactory,
      final LogAppendMetrics.Factory logAppendMetricsFactory) {
    this(
        clusterKey,
        actorNamePrefix,
        eventRegistry,
        typedRecordProcessorsFactory,
        processorContextFactory,
        committedRecordObserver,
        snapshotLifecycleListener,
        processingMetricsFactory,
        commandDispatchMetricsFactory,
        logAppendMetricsFactory,
        SnapshotMetrics.Factory.noop());
  }

  public StreamPartitionDependencies {
    Objects.requireNonNull(actorNamePrefix, "actorNamePrefix");
    Objects.requireNonNull(eventRegistry, "eventRegistry");
    Objects.requireNonNull(typedRecordProcessorsFactory, "typedRecordProcessorsFactory");
    Objects.requireNonNull(processorContextFactory, "processorContextFactory");
    committedRecordObserver =
        committedRecordObserver == null ? CommittedRecordObserver.noop() : committedRecordObserver;
    snapshotLifecycleListener =
        snapshotLifecycleListener == null
            ? SnapshotLifecycleListener.noop()
            : snapshotLifecycleListener;
    processingMetricsFactory =
        processingMetricsFactory == null
            ? ProcessingMetrics.Factory.noop()
            : processingMetricsFactory;
    commandDispatchMetricsFactory =
        commandDispatchMetricsFactory == null
            ? CommandDispatchMetrics.Factory.noop()
            : commandDispatchMetricsFactory;
    logAppendMetricsFactory =
        logAppendMetricsFactory == null ? LogAppendMetrics.Factory.noop() : logAppendMetricsFactory;
    snapshotMetricsFactory =
        snapshotMetricsFactory == null ? SnapshotMetrics.Factory.noop() : snapshotMetricsFactory;
  }
}
