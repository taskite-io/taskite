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

import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import java.util.List;

public final class BrokerClusterConfigurationQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to query configuration of cluster '%s', but it is not registered";

  public BrokerClusterConfigurationQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.BROKER_CLUSTER_CONFIGURATION;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    final String clusterId = query.getClusterId();
    final BrokerClusterConfig config =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(clusterId)
            .orElseThrow(
                () -> TopologyQueryRejection.notFound(CLUSTER_NOT_FOUND.formatted(clusterId)));
    final List<String> removedBrokers =
        state
            .getMemberChangeState()
            .configuration(MemberChangeScope.BROKER, clusterId)
            .map(change -> change.getRemovedMembers())
            .orElse(List.of());
    return new ClusterConfigRecord()
        .setClusterId(clusterId)
        .setClusterKey(state.getBrokerClusterState().clusterKey(clusterId))
        .setClusterSize(config.clusterSize())
        .setPartitionSize(config.partitionSize())
        .setReplicationFactor(config.replicationFactor())
        .setVoterRoster(config.voterRoster())
        .setRosterManaged(config.managedRoster())
        .setRemovedBrokers(removedBrokers);
  }
}
