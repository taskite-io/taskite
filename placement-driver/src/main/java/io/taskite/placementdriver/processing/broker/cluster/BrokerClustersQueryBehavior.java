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
import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClustersRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClustersRecord.Cluster;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;

public final class BrokerClustersQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;

  public BrokerClustersQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.BROKER_CLUSTERS;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    final var clusters =
        state.getBrokerClusterState().clusterIds().stream()
            .map(
                clusterId -> {
                  final var config = state.getBrokerClusterConfigState().clusterConfig(clusterId);
                  final var brokers = state.getBrokerRegistrationState().registrations(clusterId);
                  final int onlineBrokers =
                      (int)
                          brokers.stream()
                              .filter(broker -> broker.status() == BrokerStatus.ONLINE)
                              .count();
                  return new Cluster()
                      .setClusterId(clusterId)
                      .setClusterKey(state.getBrokerClusterState().clusterKey(clusterId))
                      .setClusterSize(config.clusterSize())
                      .setPartitionSize(config.partitionSize())
                      .setReplicationFactor(config.replicationFactor())
                      .setRegisteredBrokerCount(brokers.size())
                      .setOnlineBrokerCount(onlineBrokers);
                })
            .toList();
    return new BrokerClustersRecord().setCount(clusters.size()).setClusters(clusters);
  }
}
