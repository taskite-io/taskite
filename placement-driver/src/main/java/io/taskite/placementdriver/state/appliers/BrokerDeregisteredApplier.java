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

import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;
import java.time.Instant;
import java.util.function.Function;
import java.util.function.Supplier;

public final class BrokerDeregisteredApplier
    implements TypedEventApplier<BrokerIntent, BrokerRegistrationRecord> {
  private final MutableProcessingState state;
  private final Function<BrokerRegistrationRecord, Instant> eventTime;
  private final BrokerVoterRosterBehavior voterRosterBehavior;

  public BrokerDeregisteredApplier(final MutableProcessingState state) {
    this(state, BrokerRegistrationEventTime::fromRecord);
  }

  public BrokerDeregisteredApplier(
      final MutableProcessingState state, final Supplier<Instant> nowSupplier) {
    this(state, ignored -> nowSupplier.get());
  }

  private BrokerDeregisteredApplier(
      final MutableProcessingState state,
      final Function<BrokerRegistrationRecord, Instant> eventTime) {
    this.state = state;
    this.eventTime = eventTime;
    voterRosterBehavior = new BrokerVoterRosterBehavior(state);
  }

  @Override
  public void applyState(final long key, final BrokerRegistrationRecord value) {
    final Instant now = eventTime.apply(value);
    final var previous =
        state
            .getBrokerRegistrationState()
            .registration(new BrokerRegistryKey(value.getClusterId(), value.getBrokerId()))
            .orElse(null);
    state
        .getBrokerRegistrationState()
        .markBrokerOffline(value.getClusterId(), value.getBrokerId(), now);
    BrokerRegistrationTopologyBehavior.reconcile(
        state, previous, value.getClusterId(), value.getBrokerId(), now.toEpochMilli());
    // T10 sticky replacement: a deregistered roster slot yields to an online spare (if any).
    voterRosterBehavior.reconcileVoterRoster(value.getClusterId(), now.toEpochMilli());
  }
}
