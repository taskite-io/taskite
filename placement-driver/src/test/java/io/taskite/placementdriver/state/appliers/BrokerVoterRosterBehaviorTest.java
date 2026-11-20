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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class BrokerVoterRosterBehaviorTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"REGISTER", "RENEW", "OFFLINE", "DRAINED"})
  void lifecycleEventFillsVacancyWithReturnedOnlineSpare(final String event) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = configured(storage);
      returnedSpare(state, "broker-4", "new-4");
      returnedSpare(state, "broker-5", "new-5");
      final var registrations = state.getBrokerRegistrationState();
      if (event.equals("REGISTER") || event.equals("RENEW")) {
        registrations.removeBroker("demo", "broker-3");
      }
      switch (event) {
        case "REGISTER" ->
            new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
                .applyState(1, record("broker-4", "new-4"));
        case "RENEW" ->
            new BrokerRenewedApplier(state, () -> Instant.EPOCH)
                .applyState(1, record("broker-4", "new-4"));
        case "OFFLINE" ->
            new BrokerMarkedOfflineApplier(state, () -> Instant.EPOCH)
                .applyState(1, record("broker-3", "old-3"));
        case "DRAINED" ->
            new BrokerDrainedApplier(state, () -> Instant.EPOCH)
                .applyState(1, record("broker-3", "old-3"));
        default -> throw new AssertionError(event);
      }
      assertEquals(List.of("broker-0", "broker-1", "broker-2", "broker-4"), roster(state));
      assertFalse(registrations.isVoterExcluded("demo", "broker-4"));
      assertTrue(registrations.isVoterExcluded("demo", "broker-5"));
      final var config = state.getBrokerClusterConfigState().clusterConfig("demo");
      assertEquals(4, config.clusterSize());
      assertEquals(8, config.partitionSize());
      assertEquals(3, config.replicationFactor());
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      assertEquals(List.of("broker-0", "broker-1", "broker-2", "broker-4"), roster(state));
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-4"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-5"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"old-4", "", "OFFLINE", "DRAINING", "FULL", "ORDINARY"})
  void preservesFencesAndPrefersOrdinarySpare(final String scenario) {
    try (var storage = new PartitionStorage(directory)) {
      final var state = configured(storage);
      returnedSpare(
          state, "broker-4", scenario.equals("old-4") || scenario.isEmpty() ? scenario : "new-4");
      final var registrations = state.getBrokerRegistrationState();
      if (scenario.equals("OFFLINE")) {
        registrations.markBrokerOffline("demo", "broker-4", Instant.EPOCH);
      } else if (scenario.equals("DRAINING")) {
        registrations.markBrokerDraining("demo", "broker-4", Instant.EPOCH);
      } else if (scenario.equals("ORDINARY")) {
        register(state, "broker-5", "new-5");
      }
      if (!scenario.equals("FULL")) {
        registrations.markBrokerOffline("demo", "broker-3", Instant.EPOCH);
      }
      new BrokerVoterRosterBehavior(state).reconcileVoterRoster("demo", 0);
      assertEquals(
          List.of(
              "broker-0",
              "broker-1",
              "broker-2",
              scenario.equals("ORDINARY") ? "broker-5" : "broker-3"),
          roster(state));
      assertTrue(registrations.isVoterExcluded("demo", "broker-4"));
    }
  }

  @Test
  void renewalAdmitsReturnedRosterMemberEvenWhenTheRosterDoesNotChange() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = configured(storage);
      final var registrations = state.getBrokerRegistrationState();
      registrations.excludeFromPlacement("demo", "broker-3");
      registrations.removeBroker("demo", "broker-3");
      register(state, "broker-3", "new-3");
      state.getBrokerTopologyCoordinatorState().deleteCluster("demo");
      new BrokerRenewedApplier(state, () -> Instant.EPOCH)
          .applyState(1, record("broker-3", "new-3"));
      assertEquals(List.of("broker-0", "broker-1", "broker-2", "broker-3"), roster(state));
      assertFalse(registrations.isVoterExcluded("demo", "broker-3"));
      assertTrue(state.getBrokerTopologyCoordinatorState().getTask("demo", 0).isPresent());
    }
  }

  @Test
  void returnedSpareCanFillWhenNoOrdinaryRegistrationExists() {
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(1, 1, 1, List.of("removed"), 1));
      returnedSpare(state, "broker-4", "new-4");
      new BrokerVoterRosterBehavior(state).reconcileVoterRoster("demo", 0);
      assertEquals(List.of("broker-4"), roster(state));
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-4"));
      new BrokerVoterRosterBehavior(state).reconcileVoterRoster("missing", 0);
    }
  }

  private static MutableProcessingState configured(final PartitionStorage storage) {
    final var state = state(storage);
    state.getBrokerClusterState().remember("demo", 1);
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo",
            new BrokerClusterConfig(
                4, 8, 3, List.of("broker-0", "broker-1", "broker-2", "broker-3"), 1));
    for (int broker = 0; broker < 4; broker++) {
      register(state, "broker-" + broker, "old-" + broker);
    }
    return state;
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static List<String> roster(final MutableProcessingState state) {
    return state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster();
  }

  private static void returnedSpare(
      final MutableProcessingState state, final String id, final String incarnation) {
    register(state, id, "old-" + id.substring("broker-".length()));
    state.getBrokerRegistrationState().excludeFromPlacement("demo", id);
    state.getBrokerRegistrationState().removeBroker("demo", id);
    register(state, id, incarnation);
  }

  private static void register(
      final MutableProcessingState state, final String id, final String incarnation) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            id,
            "127.0.0.1",
            26501,
            26502,
            "test",
            30_000,
            Instant.EPOCH,
            List.of(),
            incarnation);
  }

  private static BrokerRegistrationRecord record(final String id, final String incarnation) {
    return new BrokerRegistrationRecord()
        .setClusterId("demo")
        .setBrokerId(id)
        .setHost("127.0.0.1")
        .setPort(26501)
        .setRaftPort(26502)
        .setIncarnationId(incarnation);
  }
}
