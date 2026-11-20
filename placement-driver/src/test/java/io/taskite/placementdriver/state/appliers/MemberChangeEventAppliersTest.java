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

import io.taskite.db.impl.DbBytes;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
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

final class MemberChangeEventAppliersTest {
  @TempDir Path tempDir;

  @Test
  void cancellationRestoresSourceRosterAndBrokerStatus() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("cancel"))) {
      final MutableProcessingState state = state(storage);
      final var applier = new MemberChangePlannedApplier(state);
      final MemberChangeRecord planned = change("change-1", MemberChangeStatus.PENDING);

      applier.applyState(1, planned);
      assertEquals(
          List.of("broker-0"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      assertEquals(BrokerStatus.DRAINING, status(state, "broker-1"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));

      new MemberChangeCancelledApplier(state)
          .applyState(2, planned.copy().setStatus(MemberChangeStatus.CANCELLED));

      assertEquals(
          List.of("broker-0", "broker-1"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      assertEquals(BrokerStatus.ONLINE, status(state, "broker-1"));
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
      assertEquals(
          List.of("broker-0", "broker-1"),
          state
              .getMemberChangeState()
              .configuration(MemberChangeScope.BROKER, "demo")
              .orElseThrow()
              .getTargetVoters());
    }
  }

  @Test
  void cancellationRestoresRemovedSpareWithoutChangingVoterRoster() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("cancel-spare"))) {
      final var state = state(storage);
      register(state, "broker-2");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-9");
      final var applier = new MemberChangePlannedApplier(state);
      final var removal =
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("remove-spare")
              .setSourceVoters(List.of("broker-0", "broker-1"))
              .setTargetCount(2)
              .setTargetVoters(List.of("broker-0", "broker-1"))
              .setRemovedMembers(List.of("broker-2"))
              .setStatus(MemberChangeStatus.PENDING);
      applier.applyState(1, removal);
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-2"));
      assertEquals(BrokerStatus.DRAINING, status(state, "broker-2"));

      new MemberChangeCancelledApplier(state)
          .applyState(2, removal.copy().setStatus(MemberChangeStatus.CANCELLED));

      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-2"));
      assertEquals(BrokerStatus.ONLINE, status(state, "broker-2"));
      assertEquals(
          List.of("broker-0", "broker-1"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-9"));
      assertEquals(
          List.of("broker-2"),
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());
    }
  }

  @Test
  void explicitRemovalKeepsVoterExclusionButNewStartupCanBeLearnerAfterReopen() {
    final var directory = tempDir.resolve("exclusion");
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      new MemberChangeAdvancedApplier(state)
          .applyState(1, change("remove", MemberChangeStatus.COMPLETED));
      assertTrue(
          state.getBrokerRegistrationState().registrations("demo").stream()
              .noneMatch(registration -> registration.brokerId().equals("broker-1")));
      register(state, "broker-1", "restart-1");
      register(state, "broker-2");
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
    }
    try (PartitionStorage storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var applier = new MemberChangeAdvancedApplier(state);
      applier.applyState(
          2,
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("add-other")
              .setSourceVoters(List.of("broker-0"))
              .setTargetCount(2)
              .setTargetVoters(List.of("broker-0", "broker-2"))
              .setStatus(MemberChangeStatus.COMPLETED));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
      assertEquals(
          List.of("broker-1"),
          new BrokerTopologyCalculator(state)
              .calculateBrokerTopology("demo", Instant.EPOCH)
              .partitions()
              .getFirst()
              .learners());

      applier.applyState(
          3,
          new MemberChangeRecord()
              .setScope(MemberChangeScope.BROKER)
              .setResourceId("demo")
              .setChangeId("readd")
              .setSourceVoters(List.of("broker-0", "broker-2"))
              .setTargetCount(3)
              .setTargetVoters(List.of("broker-0", "broker-2", "broker-1"))
              .setStatus(MemberChangeStatus.COMPLETED));
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
      state.getBrokerRegistrationState().excludeFromPlacement("other", "broker-1");
      state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-9");
      state.getBrokerRegistrationState().deleteCluster("demo");
      assertFalse(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-9"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("other", "broker-1"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"startup-broker-1", "restart-1"})
  void onlyNewStartupRegainsLearnerEligibilityAfterRemoval(final String incarnation) {
    final var directory = tempDir.resolve(incarnation);
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      new MemberChangeAdvancedApplier(state)
          .applyState(1, change("remove", MemberChangeStatus.COMPLETED));
      register(state, "broker-1", incarnation);
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var eligible =
          BrokerTopologyPlanner.eligibleRegistrations(state.getBrokerRegistrationState(), "demo");
      assertEquals(
          !incarnation.equals("startup-broker-1"),
          eligible.stream().anyMatch(registration -> registration.brokerId().equals("broker-1")));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void legacyExclusionUpgradesWithoutBlockingNewStartup(final boolean hasPreviousRegistration) {
    final var directory = tempDir.resolve("legacy-" + hasPreviousRegistration);
    try (var storage = new PartitionStorage(directory)) {
      final var state = state(storage);
      final var store = new StateStoreProcessingStore(storage);
      final var key = new DbString();
      final var value = new DbBytes();
      final var exclusions =
          store.createColumnFamily(ColumnFamilies.PD_BROKER_PLACEMENT_EXCLUSIONS, key, value);
      key.wrapString("demo:broker-1");
      value.wrapBytes(new byte[] {1});
      exclusions.upsert(key, value);
      if (hasPreviousRegistration) {
        state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-1", Instant.EPOCH);
      } else {
        state.getBrokerRegistrationState().removeBroker("demo", "broker-1");
      }
      assertFalse(
          state.getBrokerRegistrationState().isLearnerExcluded("demo", "broker-1", "restart-1"));
      register(state, "broker-1", "restart-1");
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
      assertFalse(
          state.getBrokerRegistrationState().isLearnerExcluded("demo", "broker-1", "restart-1"));
      assertTrue(
          state
              .getBrokerRegistrationState()
              .isLearnerExcluded(
                  "demo", "broker-1", hasPreviousRegistration ? "startup-broker-1" : ""));
    }
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      assertFalse(
          state.getBrokerRegistrationState().isLearnerExcluded("demo", "broker-1", "restart-1"));
      assertTrue(state.getBrokerRegistrationState().isVoterExcluded("demo", "broker-1"));
    }
  }

  @Test
  void newStartupFillsVacantVoterSlotAndCanBeRemovedAgain() {
    try (var storage = new PartitionStorage(tempDir.resolve("voter-slot"))) {
      final var state = state(storage);
      final var registrations = state.getBrokerRegistrationState();
      registrations.excludeFromPlacement("demo", "broker-1");
      registrations.removeBroker("demo", "broker-1");
      state
          .getBrokerClusterConfigState()
          .updateVoterRoster("demo", List.of("broker-0", "broker-2"));
      register(state, "broker-2");
      registrations.markBrokerOffline("demo", "broker-0", Instant.EPOCH);
      new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
          .applyState(
              2,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("broker-1")
                  .setHost("127.0.0.1")
                  .setPort(26501)
                  .setRaftPort(26502)
                  .setIncarnationId("restart-1"));
      assertEquals(
          List.of("broker-1", "broker-2"),
          state.getBrokerClusterConfigState().clusterConfig("demo").voterRoster());
      assertFalse(registrations.isVoterExcluded("demo", "broker-1"));
      assertFalse(registrations.isLearnerExcluded("demo", "broker-1", "restart-1"));
      registrations.excludeFromPlacement("demo", "broker-1");
      assertTrue(registrations.isLearnerExcluded("demo", "broker-1", "restart-1"));
      assertFalse(registrations.isLearnerExcluded("demo", "broker-1", "restart-2"));
      assertFalse(registrations.isVoterExcluded("demo", "broker-2"));
    }
  }

  @Test
  void terminalRemovalRetainsOldIncarnationAfterRegistrationWasFinalized() {
    try (var storage = new PartitionStorage(tempDir.resolve("terminal-exclusion"))) {
      final var state = state(storage);
      final var applier = new MemberChangePlannedApplier(state);
      final var removal = change("remove", MemberChangeStatus.PENDING);
      applier.applyState(1, removal);
      // The last partition ACK finalizes the registration before the parent completion event.
      state.getBrokerRegistrationState().removeBroker("demo", "broker-1");
      new MemberChangeAdvancedApplier(state)
          .applyState(2, removal.copy().setStatus(MemberChangeStatus.COMPLETED));
      assertTrue(
          state
              .getBrokerRegistrationState()
              .isLearnerExcluded("demo", "broker-1", "startup-broker-1"));
      assertFalse(
          state.getBrokerRegistrationState().isLearnerExcluded("demo", "broker-1", "restart-1"));
    }
  }

  @Test
  void replayedOldTerminalEventDoesNotClearNewActiveChange() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("active-index"))) {
      final MutableProcessingState state = state(storage);
      final var applier = new MemberChangePlannedApplier(state);
      final MemberChangeRecord oldChange = change("old", MemberChangeStatus.PENDING);
      final MemberChangeRecord newChange = change("new", MemberChangeStatus.PENDING);

      applier.applyState(1, oldChange);
      new MemberChangeCancelledApplier(state)
          .applyState(2, oldChange.copy().setStatus(MemberChangeStatus.CANCELLED));
      applier.applyState(3, newChange);
      new MemberChangeCancelledApplier(state)
          .applyState(4, oldChange.copy().setStatus(MemberChangeStatus.CANCELLED));

      assertEquals(
          "new",
          state
              .getMemberChangeState()
              .active(MemberChangeScope.BROKER, "demo")
              .orElseThrow()
              .getChangeId());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    final MutableProcessingState state =
        new MutableProcessingState(new StateStoreProcessingStore(storage));
    state.getBrokerClusterState().remember("demo", 1);
    state
        .getBrokerClusterConfigState()
        .putClusterConfig(
            "demo", new BrokerClusterConfig(2, 1, 1, List.of("broker-0", "broker-1"), 1));
    register(state, "broker-0");
    register(state, "broker-1");
    return state;
  }

  private static void register(final MutableProcessingState state, final String brokerId) {
    register(state, brokerId, "startup-" + brokerId);
  }

  private static void register(
      final MutableProcessingState state, final String brokerId, final String incarnation) {
    state
        .getBrokerRegistrationState()
        .registerBroker(
            "demo",
            brokerId,
            "127.0.0.1",
            26501,
            26601,
            "test",
            30_000,
            Instant.EPOCH,
            List.of(),
            incarnation);
  }

  private static BrokerStatus status(final MutableProcessingState state, final String brokerId) {
    return state.getBrokerRegistrationState().registrations("demo").stream()
        .filter(registration -> brokerId.equals(registration.brokerId()))
        .findFirst()
        .orElseThrow()
        .status();
  }

  private static MemberChangeRecord change(final String changeId, final MemberChangeStatus status) {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.BROKER)
        .setResourceId("demo")
        .setChangeId(changeId)
        .setSourceVoters(List.of("broker-0", "broker-1"))
        .setTargetCount(1)
        .setTargetVoters(List.of("broker-0"))
        .setRemovedMembers(List.of("broker-1"))
        .setStatus(status);
  }
}
