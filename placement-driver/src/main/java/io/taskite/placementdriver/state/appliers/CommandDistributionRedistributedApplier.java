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
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class CommandDistributionRedistributedApplier
    implements TypedEventApplier<CommandDistributionIntent, CommandDistributionRecord<?>> {
  private final MutableProcessingState state;

  public CommandDistributionRedistributedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final CommandDistributionRecord<?> value) {
    final var distributions = state.getCommandDistributionState();
    distributions.putDistribution(value);
    for (final var target : distributions.targets(key)) {
      distributions.resetTarget(
          key,
          target.getClusterId(),
          state.getBrokerClusterConfigState().clusterConfig(target.getClusterId()).partitionSize());
    }
  }
}
