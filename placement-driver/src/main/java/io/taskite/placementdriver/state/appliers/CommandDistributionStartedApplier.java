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

import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.util.LinkedHashSet;
import java.util.Set;

public final class CommandDistributionStartedApplier
    implements TypedEventApplier<CommandDistributionIntent, CommandDistributionRecord<?>> {
  private final MutableProcessingState state;

  public CommandDistributionStartedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final CommandDistributionRecord<?> value) {
    final var distributionState = state.getCommandDistributionState();
    final boolean firstDistribution =
        distributionState.distribution(value.getDistributionKey()).isEmpty();
    distributionState.putDistribution(value);
    distributionState.putDedupeKey(
        CommandDistributionDedupe.dedupeKey(value), value.getDistributionKey());
    if (firstDistribution) {
      createTargets(value);
    }
  }

  private void createTargets(final CommandDistributionRecord<?> value) {
    for (final String clusterId : targetClusters(value)) {
      createPendingDistributions(value, clusterId);
    }
  }

  private Set<String> targetClusters(final CommandDistributionRecord<?> value) {
    final Set<String> targets = new LinkedHashSet<>();
    if (value.getValueType() == ValueType.JOB_SUBSCRIPTION_DELETION) {
      final long deploymentKey =
          ((JobSubscriptionDeletionRecord) value.getValue()).getSubscriptionKey();
      state.getCommandDistributionState().targets(deploymentKey).stream()
          .map(target -> target.getClusterId())
          .forEach(targets::add);
    } else {
      targets.add(value.getClusterId());
      state.getJobSubscriptionMirrorState().mirrorsFrom(value.getClusterId()).stream()
          .map(mirror -> mirror.getTargetClusterId())
          .sorted()
          .forEach(targets::add);
    }
    if (targets.isEmpty()) {
      targets.add(value.getClusterId());
    }
    return targets;
  }

  private void createPendingDistributions(
      final CommandDistributionRecord<?> value, final String clusterId) {
    if (value.getPartitionId() > 0) {
      final int partitionSize =
          Math.max(
              value.getPartitionId(),
              state.getBrokerClusterConfigState().clusterConfig(clusterId).partitionSize());
      state
          .getCommandDistributionState()
          .putTarget(value.getDistributionKey(), clusterId, partitionSize);
      state
          .getCommandDistributionState()
          .markPending(value.getDistributionKey(), clusterId, value.getPartitionId());
      return;
    }
    final int partitionSize =
        Math.max(1, state.getBrokerClusterConfigState().clusterConfig(clusterId).partitionSize());
    state
        .getCommandDistributionState()
        .putTarget(value.getDistributionKey(), clusterId, partitionSize);
    for (int partitionId = 1; partitionId <= partitionSize; partitionId++) {
      state
          .getCommandDistributionState()
          .markPending(value.getDistributionKey(), clusterId, partitionId);
    }
  }
}
