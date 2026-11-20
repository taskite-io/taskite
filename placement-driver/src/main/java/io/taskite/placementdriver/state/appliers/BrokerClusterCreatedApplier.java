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

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.List;

public final class BrokerClusterCreatedApplier
    implements TypedEventApplier<TopologyIntent, ClusterConfigRecord> {
  private final MutableProcessingState state;
  private final BrokerVoterRosterBehavior voterRosterBehavior;

  public BrokerClusterCreatedApplier(final MutableProcessingState state) {
    this.state = state;
    voterRosterBehavior = new BrokerVoterRosterBehavior(state);
  }

  @Override
  public void applyState(final long key, final ClusterConfigRecord value) {
    final long timestamp = value.getRequestedAtEpochMillis();
    // Older leaders logged EXISTS replies as creation events. Replaying them must be a no-op.
    if (value.getOutcome() == ClusterConfigurationOutcome.EXISTS) {
      return;
    }
    state.getBrokerClusterState().remember(value.getClusterId(), value.getClusterKey());
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            value.getClusterId(),
            new BrokerClusterConfig(
                value.getClusterSize(),
                value.getPartitionSize(),
                value.getReplicationFactor(),
                List.of(),
                value.isRosterManaged() ? BrokerClusterConfig.MANAGED_ROSTER_EPOCH : 0L));
    voterRosterBehavior.reconcileVoterRoster(value.getClusterId(), timestamp);
    BrokerPartitionTopologyBehavior.rematerialize(state, value.getClusterId());
    state.getBrokerTopologyCoordinatorState().markCluster(value.getClusterId(), timestamp);
  }
}
