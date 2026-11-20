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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class BrokerClusterDeletedApplier
    implements TypedEventApplier<TopologyIntent, ClusterConfigRecord> {
  private final MutableProcessingState state;

  public BrokerClusterDeletedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final ClusterConfigRecord value) {
    final String clusterId = value.getClusterId();
    state.getTimerSubscriptionState().deleteCluster(clusterId);
    state.getJobSubscriptionState().deleteCluster(clusterId);
    state.getJobSubscriptionMirrorState().deleteCluster(clusterId);
    state.getCommandDistributionState().deleteCluster(clusterId);
    state.getBrokerTopologyChangeState().deleteCluster(clusterId);
    state.getMemberChangeState().deleteResource(MemberChangeScope.BROKER, clusterId);
    state.getBrokerPartitionTopologyState().deleteCluster(clusterId);
    state.getBrokerRegistrationState().deleteCluster(clusterId);
    state.getWorkerRegistrationState().deleteCluster(clusterId);
    state.getBrokerClusterConfigState().delete(clusterId);
    state.getBrokerClusterState().delete(clusterId);
    state.getBrokerTopologyCoordinatorState().deleteCluster(clusterId);
  }
}
