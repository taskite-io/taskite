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

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class TopologyDeleteClusterProcessor
    implements TypedRecordProcessor<ClusterConfigRecord> {
  private final Writers writers;
  private static final String MISSING_CLUSTER =
      "Expected to delete a cluster, but clusterId is blank";
  private static final String ACTIVE_BROKERS =
      "CLUSTER_HAS_ACTIVE_BROKERS: Expected to delete cluster '%s', but all registered brokers must be OFFLINE before deletion";
  private static final String SUBSCRIPTION_REFERENCES =
      "CLUSTER_HAS_JOB_SUBSCRIPTION_REFERENCES: Expected to delete cluster '%s', but it is a persisted target of another cluster's job subscription distribution";
  private final ProcessingState state;

  public TopologyDeleteClusterProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<ClusterConfigRecord> record) {
    final String clusterId = record.getValue().getClusterId();
    if (clusterId.isBlank()) {
      reject(record, RejectionType.INVALID_ARGUMENT, MISSING_CLUSTER);
      return;
    }
    final boolean hasActiveBrokers =
        state.getBrokerRegistrationState().registrations(clusterId).stream()
            .anyMatch(registration -> registration.status() != BrokerStatus.OFFLINE);
    if (hasActiveBrokers) {
      reject(record, RejectionType.INVALID_STATE, ACTIVE_BROKERS.formatted(clusterId));
      return;
    }
    final boolean referencedByAnotherCluster =
        state.getCommandDistributionState().distributions().stream()
            .anyMatch(
                distribution ->
                    !clusterId.equals(distribution.getClusterId())
                        && state
                            .getCommandDistributionState()
                            .isTarget(distribution.getDistributionKey(), clusterId));
    if (referencedByAnotherCluster) {
      reject(record, RejectionType.INVALID_STATE, SUBSCRIPTION_REFERENCES.formatted(clusterId));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(record.getKey(), TopologyIntent.CLUSTER_DELETED, record.getValue());
    writers
        .response()
        .writeEventOnCommand(
            record.getKey(), TopologyIntent.CLUSTER_DELETED, record.getValue(), record);
  }

  private void reject(
      final TypedRecord<ClusterConfigRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
