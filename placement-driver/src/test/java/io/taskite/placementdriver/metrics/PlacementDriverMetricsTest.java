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
package io.taskite.placementdriver.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PlacementDriverMetricsTest {
  @TempDir Path tempDir;

  @Test
  void publishesJobSubscriptionMirrorAndDistributionProgress() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("resource-metrics"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getJobSubscriptionMirrorState()
          .put(
              new JobSubscriptionMirrorRecord()
                  .setMirrorKey(1)
                  .setSourceClusterId("demo")
                  .setTargetClusterId("backup"));
      state
          .getCommandDistributionState()
          .putDistribution(
              new CommandDistributionRecord<JobSubscriptionRecord>()
                  .setDistributionKey(101)
                  .setClusterId("demo")
                  .setValueType(ValueType.JOB_SUBSCRIPTION)
                  .setIntent(JobSubscriptionIntent.CREATE)
                  .setValue(
                      new JobSubscriptionRecord()
                          .setClusterId("demo")
                          .setTenantId("tenant")
                          .setName("orders")
                          .setJobType("process")
                          .setVariables("<xml />")));
      state.getCommandDistributionState().putTarget(101, "demo", 2);
      state.getCommandDistributionState().markPending(101, "demo", 2);
      state.getCommandDistributionState().putTarget(101, "backup", 1);

      try (PlacementDriverMetrics metrics = new PlacementDriverMetrics(registry, "meta-0")) {
        metrics.update(raftPartition(), state);

        assertEquals(1, gauge(registry, PlacementDriverMetrics.JOB_SUBSCRIPTION_MIRROR_RELATIONS));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.JOB_SUBSCRIPTION_DISTRIBUTION_TARGETS,
                "cluster",
                "demo",
                "status",
                "distributing"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.JOB_SUBSCRIPTION_DISTRIBUTION_TARGETS,
                "cluster",
                "backup",
                "status",
                "completed"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.JOB_SUBSCRIPTION_DISTRIBUTION_PENDING_PARTITIONS,
                "cluster",
                "demo"));
        assertEquals(
            0,
            gauge(
                registry,
                PlacementDriverMetrics.JOB_SUBSCRIPTION_DISTRIBUTION_PENDING_PARTITIONS,
                "cluster",
                "backup"));
      }
    }
  }

  @Test
  void publishesRaftTopologyAndPartitionQuorumHealth() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("metrics"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 42, () -> 7);
      state.getBrokerClusterState().remember("demo", 1);
      state.getBrokerClusterState().remember("analytics", 2);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(3, 1, 3));
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("analytics", new BrokerClusterConfig(2, 5, 1));
      register(state, "broker-0", "LEADER");
      register(state, "broker-1", "FOLLOWER");
      register(state, "broker-2", "FOLLOWER");
      state
          .getWorkerRegistrationState()
          .registerWorker(
              new WorkerRegistrationRecord()
                  .setClusterId("demo")
                  .setWorkerId("worker-1")
                  .setName("payments")
                  .setJobType("payment")
                  .setMaxJobsActive(32)
                  .setActiveJobs(3)
                  .setConcurrency(4)
                  .setClientVersion("0.1.0")
                  .setRenewalTimeoutMillis(30_000),
              "tenant-a",
              Instant.ofEpochSecond(10));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"), List.of(), "broker-0"),
                  PartitionStatus.ACTIVE,
                  Map.of()));

      final RaftPartition raftPartition = mock(RaftPartition.class);
      when(raftPartition.role()).thenReturn(RaftRole.LEADER);
      when(raftPartition.leaderId()).thenReturn("meta-0");
      when(raftPartition.term()).thenReturn(7L);
      when(raftPartition.committedIndex()).thenReturn(44L);
      when(raftPartition.lastAppliedIndex()).thenReturn(42L);
      when(raftPartition.lastIndex()).thenReturn(45L);
      when(raftPartition.members())
          .thenReturn(
              List.of(
                  new RaftMember("meta-0", RaftRole.LEADER),
                  new RaftMember("meta-1", RaftRole.FOLLOWER),
                  new RaftMember("meta-2", RaftRole.LEARNER)));

      try (PlacementDriverMetrics metrics = new PlacementDriverMetrics(registry, "meta-0")) {
        metrics.update(raftPartition, state);

        assertEquals(1, gauge(registry, PlacementDriverMetrics.CLUSTER_HEALTH));
        assertEquals(3, gauge(registry, PlacementDriverMetrics.RAFT_ROLE));
        assertEquals(2, gauge(registry, PlacementDriverMetrics.RAFT_COMMIT_LAG));
        assertEquals(42, gauge(registry, PlacementDriverMetrics.TOPOLOGY_VERSION));
        assertEquals(2, gauge(registry, PlacementDriverMetrics.MANAGED_CLUSTERS));
        assertEquals(
            1, gauge(registry, PlacementDriverMetrics.CLUSTER_PARTITIONS, "cluster", "demo"));
        assertEquals(
            5, gauge(registry, PlacementDriverMetrics.CLUSTER_PARTITIONS, "cluster", "analytics"));
        assertEquals(
            4, gauge(registry, PlacementDriverMetrics.CLUSTER_MEMBER_ROLE, "member", "meta-2"));
        assertEquals(
            3,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_ROLE,
                "cluster",
                "demo",
                "partition",
                "1",
                "broker",
                "broker-0"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_HEALTH,
                "cluster",
                "demo",
                "partition",
                "1"));
        assertEquals(
            1, gauge(registry, PlacementDriverMetrics.BROKER_CLUSTER_HEALTH, "cluster", "demo"));
        assertEquals(
            20,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_LAST_RENEWAL,
                "cluster",
                "demo",
                "broker",
                "broker-0",
                "status",
                "ONLINE",
                "host",
                "127.0.0.1",
                "command_port",
                "26501",
                "raft_port",
                "26601",
                "version",
                "test"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.WORKER_STATUS,
                "cluster",
                "demo",
                "tenant",
                "tenant-a",
                "name",
                "payments",
                "worker_id",
                "worker-1",
                "job_type",
                "payment",
                "client_version",
                "0.1.0"));
        assertEquals(
            3,
            gauge(
                registry,
                PlacementDriverMetrics.WORKER_ACTIVE_JOBS,
                "cluster",
                "demo",
                "tenant",
                "tenant-a",
                "name",
                "payments",
                "worker_id",
                "worker-1",
                "job_type",
                "payment",
                "client_version",
                "0.1.0"));
        assertEquals(
            32,
            gauge(
                registry,
                PlacementDriverMetrics.WORKER_MAX_JOBS_ACTIVE,
                "cluster",
                "demo",
                "tenant",
                "tenant-a",
                "name",
                "payments",
                "worker_id",
                "worker-1",
                "job_type",
                "payment",
                "client_version",
                "0.1.0"));
        assertEquals(
            10,
            gauge(
                registry,
                PlacementDriverMetrics.WORKER_LAST_RENEWAL,
                "cluster",
                "demo",
                "tenant",
                "tenant-a",
                "name",
                "payments",
                "worker_id",
                "worker-1",
                "job_type",
                "payment",
                "client_version",
                "0.1.0",
                "status",
                "ONLINE"));

        state
            .getBrokerRegistrationState()
            .markBrokerOffline("demo", "broker-1", Instant.ofEpochMilli(2));
        metrics.update(raftPartition, state);
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_HEALTH,
                "cluster",
                "demo",
                "partition",
                "1"));

        state
            .getBrokerRegistrationState()
            .markBrokerOffline("demo", "broker-2", Instant.ofEpochMilli(2));
        metrics.update(raftPartition, state);
        assertEquals(
            0,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_HEALTH,
                "cluster",
                "demo",
                "partition",
                "1"));
        assertNull(
            registry
                .find(PlacementDriverMetrics.BROKER_STATUS)
                .tags(
                    "pd_node",
                    "meta-0",
                    "cluster",
                    "demo",
                    "broker",
                    "broker-2",
                    "status",
                    "ONLINE")
                .gauge());
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_STATUS,
                "cluster",
                "demo",
                "broker",
                "broker-2",
                "status",
                "OFFLINE"));
        assertNull(
            registry
                .find(PlacementDriverMetrics.BROKER_LAST_RENEWAL)
                .tags(
                    "pd_node",
                    "meta-0",
                    "cluster",
                    "demo",
                    "broker",
                    "broker-2",
                    "status",
                    "ONLINE")
                .gauge());
        assertEquals(
            0.002,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_LAST_RENEWAL,
                "cluster",
                "demo",
                "broker",
                "broker-2",
                "status",
                "OFFLINE"));

        when(raftPartition.role()).thenReturn(RaftRole.FOLLOWER);
        metrics.update(raftPartition, state);
        assertNull(registry.find(PlacementDriverMetrics.WORKER_STATUS).gauge());
        assertNull(registry.find(PlacementDriverMetrics.WORKER_ACTIVE_JOBS).gauge());
      }

      assertNull(registry.find(PlacementDriverMetrics.CLUSTER_HEALTH).gauge());
      assertNull(registry.find(PlacementDriverMetrics.PARTITION_HEALTH).gauge());
      assertNull(registry.find(PlacementDriverMetrics.WORKER_STATUS).gauge());
    }
  }

  @Test
  void keepsPartitionHealthyWhileNewLearnersJoin() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("learner-health"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 42, () -> 7);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(6, 1, 3));
      register(state, "broker-0", "LEADER");
      register(state, "broker-1", "FOLLOWER");
      register(state, "broker-2", "FOLLOWER");
      register(state, "broker-3", "LEARNER");
      register(state, "broker-4", "LEARNER");
      register(state, "broker-5", "LEARNER");
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(
                      3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                  PartitionTopology.Membership.initial(
                      List.of("broker-0", "broker-1", "broker-2"),
                      List.of("broker-3", "broker-4", "broker-5"),
                      "broker-0"),
                  PartitionStatus.REBALANCING,
                  Map.of()));

      try (PlacementDriverMetrics metrics = new PlacementDriverMetrics(registry, "meta-0")) {
        metrics.update(raftPartition(), state);

        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_HEALTH,
                "cluster",
                "demo",
                "partition",
                "1"));
        assertEquals(
            1, gauge(registry, PlacementDriverMetrics.BROKER_CLUSTER_HEALTH, "cluster", "demo"));

        state
            .getBrokerPartitionTopologyState()
            .putPartitionTopology(
                new PartitionTopology(
                    new PartitionTopology.Identity("demo", 1),
                    new PartitionTopology.Placement(
                        3, List.of("broker-0", "broker-1", "broker-2"), "broker-0"),
                    PartitionTopology.Membership.initial(
                        List.of("broker-0", "broker-1", "broker-2"),
                        List.of("broker-3", "broker-4", "broker-5"),
                        "broker-0"),
                    PartitionStatus.CREATING,
                    Map.of()));
        metrics.update(raftPartition(), state);

        assertEquals(
            0,
            gauge(
                registry,
                PlacementDriverMetrics.PARTITION_HEALTH,
                "cluster",
                "demo",
                "partition",
                "1"));
      }
    }
  }

  @Test
  void publishesPlacementDriverAndBrokerChangeProgress() {
    final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("change-progress"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 42, () -> 7);
      state.getBrokerClusterState().remember("demo", 1);
      state
          .getBrokerClusterConfigState()
          .putClusterConfig("demo", new BrokerClusterConfig(4, 4, 3));

      final MemberChangeRecord placementDriverChange =
          memberChange(
                  MemberChangeScope.PLACEMENT_DRIVER,
                  PlacementDriverCluster.CLUSTER_ID,
                  "pd-add-1",
                  List.of("meta-0", "meta-1", "meta-2"),
                  List.of("meta-0", "meta-1", "meta-2", "meta-3"),
                  List.of(
                      memberOperation(
                          "add-learner-meta-3",
                          MemberChangeOperationType.MEMBER_ADD_LEARNER,
                          MemberChangeOperationStatus.COMPLETED),
                      memberOperation(
                          "promote-meta-3",
                          MemberChangeOperationType.MEMBER_PROMOTE_VOTER,
                          MemberChangeOperationStatus.PENDING)))
              .setRequestedAtEpochMillis(10_000);
      placementDriverChange.setNextOperationIndex(1);
      state.getMemberChangeState().put(placementDriverChange);
      state.getMemberChangeState().setActive(placementDriverChange);

      final String scalePlanId = "admin:member-change:broker-scale-1:1";
      final ChangePlan scalePlan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", scalePlanId, 1),
                  new ChangePlan.Versions(42, 43),
                  List.of(
                      TopologyOperation.join(1, "broker-3"),
                      TopologyOperation.promoteLearner(1, "broker-3"))),
              new ChangePlan.Progress(1, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(scalePlan);
      final MemberChangeRecord brokerChange =
          memberChange(
                  MemberChangeScope.BROKER,
                  "demo",
                  "broker-scale-1",
                  List.of("broker-0", "broker-1", "broker-2"),
                  List.of("broker-0", "broker-1", "broker-2", "broker-3"),
                  List.of(
                      memberOperation(
                          "join-broker-3",
                          MemberChangeOperationType.MEMBER_JOIN,
                          MemberChangeOperationStatus.IN_PROGRESS)))
              .setRequestedAtEpochMillis(20_000)
              .setChildPartitionChangeIds(List.of(scalePlanId));
      state.getMemberChangeState().put(brokerChange);
      state.getMemberChangeState().setActive(brokerChange);

      final String addPartitionPlanId = "admin:add-partition:add-partition-1:4";
      final ChangePlan addPartitionPlan =
          new ChangePlan(
              new ChangePlan.Definition(
                  new ChangePlan.Identity("demo", addPartitionPlanId, 4),
                  new ChangePlan.Versions(42, 43),
                  List.of(
                      TopologyOperation.add(4, "broker-0"), TopologyOperation.join(4, "broker-1"))),
              new ChangePlan.Progress(1, BrokerPartitionChangeStatus.IN_PROGRESS),
              ChangePlan.Deadline.NONE);
      state.getBrokerTopologyChangeState().putChangePlan(addPartitionPlan);
      state
          .getBrokerTopologyChangeState()
          .putClusterChangeOperation(
              new ClusterChangeOperation(
                  "demo",
                  "add-partition-1",
                  42,
                  4,
                  4,
                  3,
                  List.of("broker-0", "broker-1", "broker-2", "broker-3"),
                  List.of(),
                  List.of("broker-0", "broker-1", "broker-2", "broker-3"),
                  List.of(),
                  List.of(addPartitionPlanId),
                  BrokerClusterChangeStatus.IN_PROGRESS,
                  30_000,
                  40_000));

      try (PlacementDriverMetrics metrics = new PlacementDriverMetrics(registry, "meta-0")) {
        metrics.update(raftPartition(), state);

        assertEquals(1, gauge(registry, PlacementDriverMetrics.MANAGED_CLUSTERS));
        assertEquals(
            4, gauge(registry, PlacementDriverMetrics.CLUSTER_PARTITIONS, "cluster", "demo"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_ACTIVE,
                "change_type",
                "add_node",
                "status",
                "in_progress"));
        assertEquals(
            0.5,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_PROGRESS,
                "change_type",
                "add_node",
                "status",
                "in_progress"));
        assertEquals(
            10,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_STARTED_AT,
                "change_type",
                "add_node",
                "status",
                "in_progress"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_OPERATIONS,
                "change_type",
                "add_node",
                "operation_type",
                "member_add_learner",
                "status",
                "completed"));
        assertEquals(
            0.5,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_PROGRESS,
                "cluster",
                "demo",
                "change_type",
                "scale_up",
                "status",
                "in_progress"));
        assertEquals(
            20,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_STARTED_AT,
                "cluster",
                "demo",
                "change_type",
                "scale_up",
                "status",
                "in_progress"));
        assertEquals(
            1D / 3D,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_PROGRESS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            30,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_STARTED_AT,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_PARTITIONS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            1D / 3D,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_PARTITION_CHANGE_PROGRESS,
                "cluster",
                "demo",
                "partition",
                "4",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_OPERATIONS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "operation_type",
                "add",
                "status",
                "completed"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_OPERATIONS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "operation_type",
                "distribute_resources",
                "status",
                "pending"));

        final ChangePlan distributingPlan =
            addPartitionPlan.advanced().withStatus(BrokerPartitionChangeStatus.IN_PROGRESS);
        state.getBrokerTopologyChangeState().putChangePlan(distributingPlan);
        state
            .getBrokerTopologyChangeState()
            .putClusterChangeOperation(
                state
                    .getBrokerTopologyChangeState()
                    .activeClusterChange("demo")
                    .orElseThrow()
                    .withStatus(BrokerClusterChangeStatus.IN_PROGRESS));

        metrics.update(raftPartition(), state);

        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_ACTIVE,
                "cluster",
                "demo",
                "change_id",
                "add-partition-1",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            2D / 3D,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_PROGRESS,
                "cluster",
                "demo",
                "change_id",
                "add-partition-1",
                "change_type",
                "add_partition",
                "status",
                "in_progress"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_OPERATIONS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "operation_type",
                "distribute_resources",
                "status",
                "in_progress"));

        final MemberChangeRecord completedPlacementDriverChange =
            placementDriverChange
                .copy()
                .setStatus(MemberChangeStatus.COMPLETED)
                .setNextOperationIndex(2);
        completedPlacementDriverChange
            .getOperations()
            .forEach(operation -> operation.setStatus(MemberChangeOperationStatus.COMPLETED));
        state.getMemberChangeState().put(completedPlacementDriverChange);
        state
            .getMemberChangeState()
            .clearActive(MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverCluster.CLUSTER_ID);
        state
            .getBrokerTopologyChangeState()
            .putChangePlan(distributingPlan.withStatus(BrokerPartitionChangeStatus.COMPLETED));
        state
            .getBrokerTopologyChangeState()
            .putClusterChangeOperation(
                state
                    .getBrokerTopologyChangeState()
                    .activeClusterChange("demo")
                    .orElseThrow()
                    .withStatus(BrokerClusterChangeStatus.COMPLETED));

        metrics.update(raftPartition(), state);

        assertEquals(
            0,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_ACTIVE,
                "change_id",
                "pd-add-1",
                "status",
                "completed"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.MEMBER_CHANGE_PROGRESS,
                "change_id",
                "pd-add-1",
                "status",
                "completed"));
        assertEquals(
            0,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_ACTIVE,
                "cluster",
                "demo",
                "change_id",
                "add-partition-1",
                "change_type",
                "add_partition",
                "status",
                "completed"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_PROGRESS,
                "cluster",
                "demo",
                "change_id",
                "add-partition-1",
                "change_type",
                "add_partition",
                "status",
                "completed"));
        assertEquals(
            1,
            gauge(
                registry,
                PlacementDriverMetrics.BROKER_CHANGE_OPERATIONS,
                "cluster",
                "demo",
                "change_type",
                "add_partition",
                "operation_type",
                "distribute_resources",
                "status",
                "completed"));
        assertNull(
            registry
                .find(PlacementDriverMetrics.BROKER_CHANGE_PROGRESS)
                .tags(
                    "pd_node",
                    "meta-0",
                    "cluster",
                    "demo",
                    "change_id",
                    "add-partition-1",
                    "change_type",
                    "add_partition",
                    "status",
                    "in_progress")
                .gauge());
      }
    }
  }

  private static MemberChangeRecord memberChange(
      final MemberChangeScope scope,
      final String resourceId,
      final String changeId,
      final List<String> sourceVoters,
      final List<String> targetVoters,
      final List<MemberChangeOperation> operations) {
    return new MemberChangeRecord()
        .setScope(scope)
        .setResourceId(resourceId)
        .setChangeId(changeId)
        .setSourceVoters(sourceVoters)
        .setTargetVoters(targetVoters)
        .setTargetCount(targetVoters.size())
        .setStatus(MemberChangeStatus.IN_PROGRESS)
        .setOperations(operations);
  }

  private static MemberChangeOperation memberOperation(
      final String operationId,
      final MemberChangeOperationType type,
      final MemberChangeOperationStatus status) {
    return new MemberChangeOperation().setOperationId(operationId).setType(type).setStatus(status);
  }

  private static RaftPartition raftPartition() {
    final RaftPartition raftPartition = mock(RaftPartition.class);
    when(raftPartition.role()).thenReturn(RaftRole.LEADER);
    when(raftPartition.leaderId()).thenReturn("meta-0");
    when(raftPartition.term()).thenReturn(7L);
    when(raftPartition.committedIndex()).thenReturn(44L);
    when(raftPartition.lastAppliedIndex()).thenReturn(42L);
    when(raftPartition.lastIndex()).thenReturn(45L);
    when(raftPartition.members()).thenReturn(List.of(new RaftMember("meta-0", RaftRole.LEADER)));
    return raftPartition;
  }

  private static void register(
      final MutableProcessingState state, final String brokerId, final String role) {
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
            Instant.ofEpochSecond(20),
            List.of(
                new BrokerPartitionState(
                    1,
                    new BrokerPartitionState.Status(
                        MemberRole.parse(role), 7, BrokerPartitionLifecycle.ACTIVE, 10, 0),
                    new BrokerPartitionState.Replication(100, 0, false),
                    new BrokerPartitionState.Operation("", BrokerTopologyOperationStatus.NONE),
                    BrokerPartitionElectionState.UNKNOWN)));
  }

  private static double gauge(
      final SimpleMeterRegistry registry, final String name, final String... tags) {
    final String[] tagged = new String[tags.length + 2];
    tagged[0] = "pd_node";
    tagged[1] = "meta-0";
    System.arraycopy(tags, 0, tagged, 2, tags.length);
    return registry.get(name).tags(tagged).gauge().value();
  }
}
