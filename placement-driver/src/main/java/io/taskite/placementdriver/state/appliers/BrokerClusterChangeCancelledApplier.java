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

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.Objects;

/** Cancels every non-terminal partition plan owned by one aggregate change. */
public final class BrokerClusterChangeCancelledApplier
    implements TypedEventApplier<TopologyIntent, ClusterConfigRecord> {
  private final MutableProcessingState state;

  public BrokerClusterChangeCancelledApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final ClusterConfigRecord value) {
    state
        .getBrokerTopologyChangeState()
        .clusterChangeOperation(value.getClusterId(), value.getChangeId())
        .ifPresent(
            operation -> {
              final PartitionChangeCancelledApplier planApplier =
                  new PartitionChangeCancelledApplier(state);
              operation.planChangeIds().stream()
                  .map(
                      planId ->
                          state
                              .getBrokerTopologyChangeState()
                              .currentDescendant(value.getClusterId(), planId)
                              .orElse(null))
                  .filter(Objects::nonNull)
                  .filter(plan -> !plan.isTerminal())
                  // A stopped execution may already have installed its reserved authority.
                  // Retain this recovery chain until its committed configuration is reconciled.
                  .filter(plan -> plan.execution().predecessor().isBlank())
                  .filter(plan -> plan.status() != BrokerPartitionChangeStatus.SUPERSEDING)
                  .forEach(
                      plan ->
                          planApplier.applyState(
                              key, BrokerPartitionChangeRecordMapper.fromPlan(plan)));
              state
                  .getBrokerTopologyChangeState()
                  .putClusterChangeOperation(
                      operation.withStatus(BrokerClusterChangeStatus.CANCELLED));
              BrokerRemovalFinalizer.finalizeRemovals(state, value.getClusterId());
            });
  }
}
