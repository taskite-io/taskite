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
package io.taskite.placementdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.processing.broker.topology.BrokerTopologyQueryBehavior;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.appliers.BrokerClusterCreatedApplier;
import io.taskite.placementdriver.state.appliers.BrokerMarkedOfflineApplier;
import io.taskite.placementdriver.state.appliers.BrokerRegisteredApplier;
import io.taskite.placementdriver.state.appliers.BrokerRenewedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionAcknowledgedApplier;
import io.taskite.placementdriver.state.appliers.CommandDistributionStartedApplier;
import io.taskite.placementdriver.state.appliers.PlacementDriverTopologyUpdatedApplier;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PlacementDriverStateTest {
  @TempDir Path tempDir;

  private static final Instant T0 = Instant.parse("2024-01-01T00:00:00Z");
  private static final Instant T1 = Instant.parse("2024-01-01T00:01:00Z");

  @Test
  void mapsCurrentTopologyProtocolValuesToRecords() {
    assertEquals(TopologyIntent.QUERY, Intent.fromProtocolValue(ValueType.TOPOLOGY, (short) 12));
    assertEquals(TopologyIntent.QUERIED, Intent.fromProtocolValue(ValueType.TOPOLOGY, (short) 13));
    assertEquals(
        TopologyIntent.CLUSTER_CREATED, Intent.fromProtocolValue(ValueType.TOPOLOGY, (short) 1));
    assertEquals(TopologyIntent.UPDATE, Intent.fromProtocolValue(ValueType.TOPOLOGY, (short) 4));

    final TypedEventRegistry registry = PlacementDriverProcessors.createEventRegistry();
    final UnifiedRecordValue query = registry.create(ValueType.TOPOLOGY, TopologyIntent.QUERY);
    final UnifiedRecordValue queried = registry.create(ValueType.TOPOLOGY, TopologyIntent.QUERIED);
    final UnifiedRecordValue clusterCreated =
        registry.create(ValueType.TOPOLOGY, TopologyIntent.CLUSTER_CREATED);
    final UnifiedRecordValue update = registry.create(ValueType.TOPOLOGY, TopologyIntent.UPDATE);

    assertTrue(query instanceof TopologyQueryRecord);
    assertTrue(queried instanceof TopologyQueryRecord);
    assertTrue(clusterCreated instanceof ClusterConfigRecord);
    assertTrue(update instanceof MemberTopologyRecord);
  }

  @Test
  void restoresLegacyTopologyQueryTypesIntoCanonicalTypes() {
    assertEquals(QueryType.BROKER_CLUSTER_CONFIGURATION, restoreLegacyQueryType("CLUSTER_CONFIG"));
    assertEquals(QueryType.BROKER_TOPOLOGY, restoreLegacyQueryType("BROKERS"));
    assertEquals(QueryType.PLACEMENT_DRIVER_TOPOLOGY, restoreLegacyQueryType("MEMBERS"));
    assertEquals(QueryType.PLACEMENT_DRIVER_HEALTH, restoreLegacyQueryType("HEALTH"));
    assertEquals(
        QueryType.BROKER_CLUSTER_CHANGE, restoreLegacyQueryType("BROKER_CLUSTER_CHANGE_BATCH"));
    assertEquals(QueryType.BROKER_CLUSTER_CHANGE, restoreLegacyQueryType("BROKER_CLUSTER_CHANGES"));
  }

  @Test
  void appliesBrokerRegistrationAndRenewal() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-0"));

      List<BrokerRegistration> registrations =
          state.getBrokerRegistrationState().registrations("demo");
      assertEquals(1, registrations.size());
      assertEquals(BrokerStatus.ONLINE, registrations.get(0).status());
      assertEquals(T0, registrations.get(0).lastRenewal());
      assertEquals(45_000, registrations.get(0).renewalTimeoutMillis());

      new BrokerRenewedApplier(state, () -> T1).applyState(1, renewBroker("demo", "broker-0"));
      assertEquals(
          T1, state.getBrokerRegistrationState().registrations("demo").get(0).lastRenewal());
      assertEquals(
          45_000,
          state.getBrokerRegistrationState().registrations("demo").get(0).renewalTimeoutMillis());
    }
  }

  @Test
  void brokerLifecycleEventTimeIsDeterministicAcrossStateMachines() {
    final BrokerRegistrationRecord event =
        registerBroker("demo", "broker-0").setEventTimestampEpochMillis(T0.toEpochMilli());
    try (PartitionStorage firstStorage =
            new PartitionStorage(tempDir.resolve("deterministic-time-first"));
        PartitionStorage secondStorage =
            new PartitionStorage(tempDir.resolve("deterministic-time-second"))) {
      final MutableProcessingState first = state(firstStorage);
      final MutableProcessingState second = state(secondStorage);
      first.getBrokerClusterState().remember("demo", 1);
      second.getBrokerClusterState().remember("demo", 1);

      new BrokerRegisteredApplier(first).applyState(1, event);
      new BrokerRegisteredApplier(second).applyState(1, event);

      assertEquals(
          first.getBrokerRegistrationState().registrations("demo"),
          second.getBrokerRegistrationState().registrations("demo"));
      assertEquals(
          T0, first.getBrokerRegistrationState().registrations("demo").getFirst().lastRenewal());
    }
  }

  @Test
  void brokerTopologyReportsUnavailableInsteadOfNotFoundWhenNoBrokerIsOnline() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("offline-topology"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state).applyState(1, clusterConfig("demo", 1, 1, 1));
      new BrokerRegisteredApplier(state)
          .applyState(
              1,
              registerBroker("demo", "broker-0").setEventTimestampEpochMillis(T0.toEpochMilli()));
      new BrokerMarkedOfflineApplier(state)
          .applyState(
              1,
              new BrokerRegistrationRecord()
                  .setClusterId("demo")
                  .setBrokerId("broker-0")
                  .setEventTimestampEpochMillis(T1.toEpochMilli()));
      state.getBrokerPartitionTopologyState().deleteCluster("demo");

      final TopologyQueryRejection rejection =
          assertThrows(
              TopologyQueryRejection.class,
              () ->
                  new BrokerTopologyQueryBehavior(state)
                      .createResponseValue(
                          new TopologyQueryRecord()
                              .setClusterId("demo")
                              .setQueryType(QueryType.BROKER_TOPOLOGY)
                              .setRequestedAtEpochMillis(T1.toEpochMilli())));

      assertEquals(RejectionType.PROCESSING_ERROR, rejection.rejectionType());
    }
  }

  @Test
  void brokerTopologyPreservesBrokerPartitionObservations() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("broker-observations"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state).applyState(1, clusterConfig("demo", 3, 1, 1));
      new BrokerRegisteredApplier(state)
          .applyState(
              1,
              registerBroker("demo", "broker-0")
                  .setEventTimestampEpochMillis(T0.toEpochMilli())
                  .setPartitions(
                      List.of(
                          new BrokerRegistrationRecord.PartitionStateRecord()
                              .setPartitionId(1)
                              .setRole(MemberRole.LEADER)
                              .setTerm(7)
                              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
                              .setMatchIndex(123)
                              .setLag(4)
                              .setReadyForPromotion(true)
                              .setPriority(10)
                              .setLastOpId("op-1")
                              .setLastOpStatus(BrokerTopologyOperationStatus.APPLIED)
                              .setRecoveryEpoch(3))));

      final TopologyRecord topology =
          assertInstanceOf(
              TopologyRecord.class,
              new BrokerTopologyQueryBehavior(state)
                  .createResponseValue(
                      new TopologyQueryRecord()
                          .setClusterId("demo")
                          .setQueryType(QueryType.BROKER_TOPOLOGY)
                          .setRequestedAtEpochMillis(T1.toEpochMilli())));

      assertEquals(3, topology.getClusterSize());
      assertEquals(1, topology.getBrokers().size());
      final TopologyRecord.BrokerEndpointRecord broker = topology.getBrokers().getFirst();
      assertEquals("0.1.0-SNAPSHOT", broker.getVersion());
      assertEquals(T0.toEpochMilli(), broker.getRegisteredAtEpochMillis());
      assertEquals(T0.toEpochMilli(), broker.getLastRenewalEpochMillis());
      assertEquals(45_000, broker.getRenewalTimeoutMillis());
      assertEquals(RegistrationStatus.ONLINE, broker.getStatus());
      assertEquals(1, broker.getPartitions().size());
      final TopologyRecord.BrokerPartitionRecord partition = broker.getPartitions().getFirst();
      assertEquals(1, partition.getPartitionId());
      assertEquals(MemberRole.LEADER, partition.getRole());
      assertEquals(7, partition.getTerm());
      assertEquals(BrokerPartitionLifecycle.ACTIVE, partition.getLifecycleState());
      assertEquals(123, partition.getMatchIndex());
      assertEquals(4, partition.getLag());
      assertTrue(partition.isReadyForPromotion());
      assertEquals(10, partition.getPriority());
      assertEquals("op-1", partition.getLastOpId());
      assertEquals(BrokerTopologyOperationStatus.APPLIED, partition.getLastOpStatus());
      assertEquals(3, partition.getRecoveryEpoch());
    }
  }

  @Test
  void steadyStateRenewalInitialisesVoterRosterToClusterSize() {
    // Regression for the clusterSize-violation bug: on an already-running cluster only renewals
    // fire (no register/offline events), so the roster must be initialised from the renewal path.
    // Before the fix the roster stayed empty forever and voterRegistrations fell back to the legacy
    // "online capped at clusterSize" logic that let the voter union drift above clusterSize.
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state).applyState(1, clusterConfig("demo", 3, 5, 3));
      for (final String brokerId : List.of("broker-0", "broker-1", "broker-2", "broker-3")) {
        new BrokerRegisteredApplier(state, () -> T0)
            .applyState(1, registerBroker("demo", brokerId));
      }

      // A steady-state renewal (the only event a running broker sends) must maintain the roster.
      new BrokerRenewedApplier(state, () -> T1).applyState(1, renewBroker("demo", "broker-0"));

      final BrokerClusterConfig config =
          state.getBrokerClusterConfigState().findClusterConfig("demo").orElseThrow();
      assertEquals(3, config.voterRoster().size(), "roster must be capped at clusterSize");
      assertTrue(config.rosterEpoch() > 0, "roster epoch must advance once initialised");

      // With a stable roster of 3, the fourth broker can only ever be a learner (spare): the voter
      // union across all partitions never exceeds clusterSize.
      final List<BrokerRegistration> online =
          state.getBrokerRegistrationState().registrations("demo");
      final Set<String> voterUnion = new HashSet<>();
      for (int partitionId = 1; partitionId <= config.partitionSize(); partitionId++) {
        final var placement =
            BrokerTopologyPlanner.planPartition(
                "demo",
                config,
                partitionId,
                online,
                BrokerTopologyPlanner.voterRegistrations(config, online));
        voterUnion.addAll(placement.desiredVoters());
      }
      assertEquals(3, voterUnion.size(), "voter union must not exceed clusterSize");
    }
  }

  @Test
  void brokerRegistrationAutomaticallyFillsManagedRosterUpToClusterSize() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("managed-roster"))) {
      final MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(1, clusterConfig("demo", 2, 1, 2).setRosterManaged(true));

      for (final String brokerId : List.of("broker-0", "broker-1", "broker-2")) {
        new BrokerRegisteredApplier(state, () -> T0)
            .applyState(1, registerBroker("demo", brokerId));
      }

      final BrokerClusterConfig config =
          state.getBrokerClusterConfigState().findClusterConfig("demo").orElseThrow();
      assertEquals(List.of("broker-0", "broker-1"), config.voterRoster());
      assertEquals(
          List.of("broker-0"),
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology("demo", 1)
              .orElseThrow()
              .currentVoters());

      final List<BrokerRegistration> online =
          state.getBrokerRegistrationState().registrations("demo");
      final var desired =
          BrokerTopologyPlanner.planPartition(
              "demo", config, 1, online, BrokerTopologyPlanner.voterRegistrations(config, online));
      assertEquals(List.of("broker-0", "broker-1"), desired.desiredVoters());
      assertEquals(List.of("broker-2"), desired.learners());
    }
  }

  @Test
  void markBrokerOfflineUpdatesStatus() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-0"));

      new BrokerMarkedOfflineApplier(state, () -> T1)
          .applyState(1, renewBroker("demo", "broker-0"));

      assertEquals(
          BrokerStatus.OFFLINE,
          state.getBrokerRegistrationState().registrations("demo").get(0).status());
    }
  }

  @Test
  void registerBrokerClusterStoresTopologyAndClusterId() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state).applyState(1, clusterConfig("demo", 3, 5, 2));

      assertEquals(List.of("demo"), state.getBrokerClusterState().clusterIds());
      BrokerClusterConfig config =
          state.getBrokerClusterConfigState().findClusterConfig("demo").orElseThrow();
      assertEquals(3, config.clusterSize());
      assertEquals(5, config.partitionSize());
      assertEquals(2, config.replicationFactor());
    }
  }

  @Test
  void findTopologyConfigEmptyWhenUnknown() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      assertTrue(state.getBrokerClusterConfigState().findClusterConfig("missing").isEmpty());
      assertEquals(
          new BrokerClusterConfig(1, 1, 1),
          state.getBrokerClusterConfigState().clusterConfig("missing"));
    }
  }

  @Test
  void commandDistributionStartAndAcknowledgeUpdatePendingState() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      CommandDistributionRecord<?> record = commandDistribution(11, 2);

      new CommandDistributionStartedApplier(state).applyState(record.getDistributionKey(), record);

      assertEquals(
          record.getDistributionKey(),
          state
              .getCommandDistributionState()
              .distribution(record.getDistributionKey())
              .orElseThrow()
              .getDistributionKey());
      assertTrue(
          state
              .getCommandDistributionState()
              .isPending(
                  record.getDistributionKey(), record.getClusterId(), record.getPartitionId()));

      new CommandDistributionAcknowledgedApplier(state)
          .applyState(record.getDistributionKey(), record);

      assertFalse(
          state
              .getCommandDistributionState()
              .isPending(
                  record.getDistributionKey(), record.getClusterId(), record.getPartitionId()));
    }
  }

  @Test
  void registrationsAreSortedByBrokerId() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      MutableProcessingState state = state(storage);
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-2"));
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-0"));
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-1"));

      assertEquals(
          List.of("broker-0", "broker-1", "broker-2"),
          state.getBrokerRegistrationState().registrations("demo").stream()
              .map(BrokerRegistration::brokerId)
              .toList());
    }
  }

  @Test
  void businessStateSurvivesRestart() {
    Path runtime = tempDir.resolve("runtime");
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      MutableProcessingState state = state(storage);
      new BrokerClusterCreatedApplier(state).applyState(1, clusterConfig("demo", 3, 5, 2));
      new BrokerRegisteredApplier(state, () -> T0)
          .applyState(1, registerBroker("demo", "broker-0"));
      new PlacementDriverTopologyUpdatedApplier(state).applyState(1, placementDriverTopology());
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      MutableProcessingState reopened = state(storage);
      assertEquals(List.of("demo"), reopened.getBrokerClusterState().clusterIds());
      assertEquals(1, reopened.getBrokerRegistrationState().registrations("demo").size());
      assertEquals(
          45_000,
          reopened
              .getBrokerRegistrationState()
              .registrations("demo")
              .get(0)
              .renewalTimeoutMillis());
      MemberTopologyRecord topology =
          reopened.getPlacementDriverTopologyState().topology().orElseThrow();
      assertEquals("meta-1", topology.getLeaderId());
      assertEquals("127.0.0.1:29002", topology.getLeaderEndpoint());
      assertEquals(2, topology.getMembers().size());
    }
  }

  @Test
  void jobSubscriptionMirrorStateSurvivesRestart() throws Exception {
    final Path runtime = tempDir.resolve("mirror-restart");
    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final StateStoreProcessingStore store = new StateStoreProcessingStore(storage);
      final MutableProcessingState state = new MutableProcessingState(store);
      state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(9001)
                  .setSourceClusterId("region-a")
                  .setTargetClusterId("region-b")
                  .setCreatedAtEpochMillis(42));
      store.getCurrentTransaction().commit();
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      final JobSubscriptionMirrorRecord mirror =
          state(storage)
              .getJobSubscriptionMirrorState()
              .mirror("region-a", "region-b")
              .orElseThrow();
      assertEquals(9001, mirror.getMirrorKey());
      assertEquals(42, mirror.getCreatedAtEpochMillis());
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static QueryType restoreLegacyQueryType(final String queryType) {
    final byte[] json = ("{\"queryType\":\"" + queryType + "\"}").getBytes(StandardCharsets.UTF_8);
    final TopologyQueryRecord restored = new TopologyQueryRecord();
    restored.wrap(new UnsafeBuffer(json), 0, json.length);
    return restored.getQueryType();
  }

  private static BrokerRegistrationRecord registerBroker(String clusterId, String brokerId) {
    return new BrokerRegistrationRecord()
        .setClusterId(clusterId)
        .setBrokerId(brokerId)
        .setHost("127.0.0.1")
        .setPort(26501)
        .setRaftPort(26601)
        .setRenewalTimeoutMillis(45_000)
        .setVersion("0.1.0-SNAPSHOT");
  }

  private static BrokerRegistrationRecord renewBroker(String clusterId, String brokerId) {
    return new BrokerRegistrationRecord().setClusterId(clusterId).setBrokerId(brokerId);
  }

  private static ClusterConfigRecord clusterConfig(
      String clusterId, int clusterSize, int partitionSize, int replicationFactor) {
    return new ClusterConfigRecord()
        .setClusterKey(1)
        .setClusterId(clusterId)
        .setClusterSize(clusterSize)
        .setPartitionSize(partitionSize)
        .setReplicationFactor(replicationFactor);
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> commandDistribution(
      long distributionKey, int partitionId) {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(distributionKey)
        .setClusterId("demo")
        .setPartitionId(partitionId)
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setName("resource")
                .setJobType("process")
                .setVariables("content"));
  }

  private static MemberTopologyRecord placementDriverTopology() {
    return new MemberTopologyRecord()
        .setLocalMemberId("meta-1")
        .setLeaderId("meta-1")
        .setLeaderEndpoint("127.0.0.1:29002")
        .setTerm(3)
        .setMembers(
            List.of(
                new MemberTopologyRecord.MemberRecord()
                    .setMemberId("meta-0")
                    .setEndpoint("127.0.0.1:29001")
                    .setRole(MemberRole.FOLLOWER),
                new MemberTopologyRecord.MemberRecord()
                    .setMemberId("meta-1")
                    .setEndpoint("127.0.0.1:29002")
                    .setRole(MemberRole.LEADER)
                    .setSelf(true)));
  }
}
