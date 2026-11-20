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
package io.taskite.dist.broker;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/** Readiness probe for rollout gates; process liveness remains independent of Raft availability. */
@Component("brokerReadiness")
@Profile("broker")
public final class BrokerReadinessHealthIndicator implements HealthIndicator {
  private final ObjectProvider<BrokerSpringLifecycle> lifecycle;

  public BrokerReadinessHealthIndicator(final ObjectProvider<BrokerSpringLifecycle> lifecycle) {
    this.lifecycle = lifecycle;
  }

  @Override
  public Health health() {
    final var current = lifecycle.getIfAvailable();
    return current != null && current.ready() ? Health.up().build() : Health.outOfService().build();
  }
}
