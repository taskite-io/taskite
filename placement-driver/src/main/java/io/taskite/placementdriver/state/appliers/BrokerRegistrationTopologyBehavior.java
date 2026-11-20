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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;

/** Translates registration lifecycle changes into placement work; lease progress is excluded. */
final class BrokerRegistrationTopologyBehavior {
  private BrokerRegistrationTopologyBehavior() {}

  static boolean reconcile(
      final MutableProcessingState state,
      final BrokerRegistration previous,
      final String clusterId,
      final String brokerId,
      final long timestamp) {
    final var current =
        state
            .getBrokerRegistrationState()
            .registration(new BrokerRegistryKey(clusterId, brokerId))
            .orElse(null);
    final boolean changed =
        previous == null
            ? current != null
            : current == null
                || previous.status() != current.status()
                || !previous.incarnationId().equals(current.incarnationId())
                || !previous.endpoint().equals(current.endpoint());
    if (changed) {
      state.getBrokerTopologyCoordinatorState().markCluster(clusterId, timestamp);
    }
    return changed;
  }
}
