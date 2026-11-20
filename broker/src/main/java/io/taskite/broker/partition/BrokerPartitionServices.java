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
package io.taskite.broker.partition;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.cluster.raft.group.PartitionGroupDescriptor;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.engine.Engine;
import io.taskite.engine.metrics.MicrometerEngineMetrics;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.snapshot.SnapshotLifecycleListener;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.metrics.MicrometerCommandDispatchMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerLogAppendMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerSnapshotMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerStreamProcessorMetrics;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.streamplatform.runtime.StreamPartitionConfig;
import io.taskite.streamplatform.runtime.StreamPartitionDependencies;
import io.taskite.streamplatform.runtime.StreamPartitionFactory;
import java.nio.file.Path;

final class BrokerPartitionServices {
  private BrokerPartitionServices() {}

  static StreamPartitionDependencies create(
      final long clusterKey,
      final String clusterId,
      final int partitionId,
      final MeterRegistry meterRegistry,
      final BackpressureConfiguration backpressure) {
    return create(clusterKey, clusterId, partitionId, meterRegistry, backpressure, true);
  }

  static StreamPartitionDependencies create(
      final long clusterKey,
      final String clusterId,
      final int partitionId,
      final MeterRegistry meterRegistry,
      final BackpressureConfiguration backpressure,
      final boolean workerJobLeaseTokenEnabled) {
    return new StreamPartitionDependencies(
        clusterKey,
        "broker-" + clusterId + "-" + partitionId,
        Engine.eventRegistry(),
        Engine::createProcessors,
        (id, storage, keys, writers) ->
            Engine.createProcessorContext(id, storage, keys, writers, workerJobLeaseTokenEnabled),
        null,
        SnapshotLifecycleListener.noop(),
        (ignoredPartitionId, context) -> {
          final var state = context.getProcessingState(ProcessingState.class);
          return ProcessingMetrics.combine(
              new MicrometerStreamProcessorMetrics(
                  meterRegistry, clusterId, context.getPartitionId()),
              new MicrometerEngineMetrics(
                  meterRegistry,
                  clusterId,
                  context.getPartitionId(),
                  state.getIncidentState()::getIncidentCount));
        },
        metricsPartitionId ->
            new MicrometerCommandDispatchMetrics(meterRegistry, clusterId, metricsPartitionId),
        metricsPartitionId ->
            new MicrometerLogAppendMetrics(meterRegistry, clusterId, metricsPartitionId),
        metricsPartitionId ->
            new MicrometerSnapshotMetrics(meterRegistry, clusterId, metricsPartitionId),
        ignored -> new PartitionRequestLimiter<>(backpressure, BrokerBackpressure::isExempt));
  }

  static StreamPartition createGroup(
      final PartitionGroupDescriptor descriptor,
      final Path dataDirectory,
      final BrokerPartitionManagerConfig managerConfig,
      final JRaftServer jraftServer,
      final ActorScheduler scheduler,
      final MeterRegistry meterRegistry) {
    final var processing = managerConfig.processing();
    final StreamPartitionConfig config =
        StreamPartitionConfig.builder(
                descriptor.groupName(),
                descriptor.groupId(),
                managerConfig.localBrokerId(),
                dataDirectory)
            .initialTerm(descriptor.term())
            .initialRole(descriptor.role())
            .peers(descriptor.peers())
            .maxCommandsInBatch(processing.maxCommandsInBatch())
            .maxRecordsInBatch(processing.maxRecordsInBatch())
            .maxBatchDuration(processing.maxBatchDuration())
            .maxBatchBytes(processing.maxBatchBytes())
            .ownsScheduler(false)
            .snapshotPolicy(managerConfig.snapshot().policy())
            .electionTimeout(managerConfig.consensus().electionTimeout())
            .build();
    return StreamPartitionFactory.create(
        config,
        jraftServer,
        scheduler,
        create(
            managerConfig.clusterKey(),
            descriptor.groupName(),
            descriptor.groupId(),
            meterRegistry,
            managerConfig.backpressure(),
            managerConfig.engine().workerJobLeaseTokenEnabled()));
  }
}
