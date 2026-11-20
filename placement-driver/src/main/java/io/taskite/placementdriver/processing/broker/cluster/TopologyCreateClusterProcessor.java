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
package io.taskite.placementdriver.processing.broker.cluster;

import io.taskite.placementdriver.broker.cluster.BrokerClusterConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerClusterInvariantValidator;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class TopologyCreateClusterProcessor
    implements TypedRecordProcessor<ClusterConfigRecord> {
  private final Writers writers;
  private static final String INVALID_CONFIGURATION =
      "Expected to create cluster '%s' with broker count '%d', partition count '%d' and replicationFactor '%d', but configuration validation failed: %s";
  private static final String CONFIGURATION_CONFLICT =
      "CLUSTER_CONFIG_CONFLICT: Expected to create cluster '%s' with broker count '%d', partition count '%d' and replicationFactor '%d', but it already exists with broker count '%d', partition count '%d' and replicationFactor '%d'";
  private static final String CLUSTER_KEY_EXHAUSTED =
      "CLUSTER_LIMIT_EXCEEDED: Cannot create cluster '%s': all %d cluster key namespaces have been allocated";
  private final ProcessingState state;

  public TopologyCreateClusterProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<ClusterConfigRecord> record) {
    final ClusterConfigRecord requested = record.getValue();
    final String validationError =
        BrokerClusterInvariantValidator.validateBrokerClusterConfiguration(
            new BrokerClusterConfiguration(
                requested.getClusterId(),
                requested.getClusterSize(),
                requested.getPartitionSize(),
                requested.getReplicationFactor()));
    if (validationError != null) {
      final String reason =
          INVALID_CONFIGURATION.formatted(
              requested.getClusterId(),
              requested.getClusterSize(),
              requested.getPartitionSize(),
              requested.getReplicationFactor(),
              validationError);
      writers.rejection().appendRejection(RejectionType.INVALID_ARGUMENT, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_ARGUMENT, reason);
      return;
    }

    final var existing =
        state.getBrokerClusterConfigState().findClusterConfig(requested.getClusterId());
    if (existing.isPresent()) {
      if (sameConfiguration(existing.get(), requested)) {
        requested.setClusterKey(state.getBrokerClusterState().clusterKey(requested.getClusterId()));
        requested.setOutcome(ClusterConfigurationOutcome.EXISTS);
        writers
            .response()
            .writeEventOnCommand(
                record.getKey(), TopologyIntent.CLUSTER_CREATED, requested, record);
      } else {
        final String reason =
            CONFIGURATION_CONFLICT.formatted(
                requested.getClusterId(),
                requested.getClusterSize(),
                requested.getPartitionSize(),
                requested.getReplicationFactor(),
                existing.get().clusterSize(),
                existing.get().partitionSize(),
                existing.get().replicationFactor());
        writers.rejection().appendRejection(RejectionType.ALREADY_EXISTS, reason);
        writers.response().writeRejectionOnCommand(record, RejectionType.ALREADY_EXISTS, reason);
      }
      return;
    }
    final long clusterKey = state.getBrokerClusterState().latestClusterKey() + 1;
    if (clusterKey >= Protocol.MAXIMUM_CLUSTERS) {
      final String reason =
          CLUSTER_KEY_EXHAUSTED.formatted(requested.getClusterId(), Protocol.MAXIMUM_CLUSTERS - 1);
      writers.rejection().appendRejection(RejectionType.INVALID_STATE, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_STATE, reason);
      return;
    }
    requested.setClusterKey(clusterKey).setOutcome(ClusterConfigurationOutcome.CREATED);
    writers.state().appendFollowUpEvent(record.getKey(), TopologyIntent.CLUSTER_CREATED, requested);
    writers
        .response()
        .writeEventOnCommand(record.getKey(), TopologyIntent.CLUSTER_CREATED, requested, record);
  }

  private static boolean sameConfiguration(
      final BrokerClusterConfig existing, final ClusterConfigRecord requested) {
    return existing.clusterSize() == requested.getClusterSize()
        && existing.partitionSize() == requested.getPartitionSize()
        && existing.replicationFactor() == requested.getReplicationFactor();
  }
}
