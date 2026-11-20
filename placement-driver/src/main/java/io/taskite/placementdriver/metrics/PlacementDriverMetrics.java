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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes a bounded snapshot of placement-driver and Broker topology state.
 *
 * <p>{@link #update(RaftPartition, ProcessingState)} must run on the placement-driver actor. Gauge
 * callbacks only read atomics or immutable values owned by Micrometer, so Prometheus scrapes never
 * access actor-confined RocksDB state.
 */
public final class PlacementDriverMetrics implements AutoCloseable {
  public static final String CLUSTER_HEALTH = "taskite.pd.cluster.health";
  public static final String LEADER_AVAILABLE = "taskite.pd.cluster.leader.available";
  public static final String RAFT_ROLE = "taskite.pd.raft.role";
  public static final String RAFT_TERM = "taskite.pd.raft.term";
  public static final String RAFT_COMMIT_INDEX = "taskite.pd.raft.commit.index";
  public static final String RAFT_APPLIED_INDEX = "taskite.pd.raft.applied.index";
  public static final String RAFT_LAST_LOG_INDEX = "taskite.pd.raft.last.log.index";
  public static final String RAFT_COMMIT_LAG = "taskite.pd.raft.commit.lag";
  public static final String TOPOLOGY_VERSION = "taskite.pd.topology.version";
  public static final String MANAGED_CLUSTERS = "taskite.pd.managed.clusters";
  public static final String CLUSTER_PARTITIONS = "taskite.pd.cluster.partitions";
  public static final String CLUSTER_MEMBER_ROLE = "taskite.pd.cluster.member.role";
  public static final String MEMBER_CHANGE_ACTIVE = "taskite.pd.member.change.active";
  public static final String MEMBER_CHANGE_PROGRESS = "taskite.pd.member.change.progress";
  public static final String MEMBER_CHANGE_STARTED_AT =
      "taskite.pd.member.change.started.at.seconds";
  public static final String MEMBER_CHANGE_OPERATIONS = "taskite.pd.member.change.operations";
  public static final String BROKER_STATUS = "taskite.pd.broker.status";
  public static final String BROKER_LAST_RENEWAL = "taskite.pd.broker.last.renewal.at.seconds";
  public static final String BROKER_CLUSTER_HEALTH = "taskite.pd.broker.cluster.health";
  public static final String BROKER_CHANGE_ACTIVE = "taskite.pd.broker.change.active";
  public static final String BROKER_CHANGE_PROGRESS = "taskite.pd.broker.change.progress";
  public static final String BROKER_CHANGE_STARTED_AT =
      "taskite.pd.broker.change.started.at.seconds";
  public static final String BROKER_CHANGE_PARTITIONS = "taskite.pd.broker.change.partitions";
  public static final String BROKER_PARTITION_CHANGE_PROGRESS =
      "taskite.pd.broker.partition.change.progress";
  public static final String BROKER_CHANGE_OPERATIONS = "taskite.pd.broker.change.operations";
  public static final String PARTITION_HEALTH = "taskite.pd.partition.health";
  public static final String PARTITION_ROLE = "taskite.pd.partition.role";
  public static final String WORKER_STATUS = "taskite.pd.worker.status";
  public static final String WORKER_ACTIVE_JOBS = "taskite.pd.worker.active.jobs";
  public static final String WORKER_MAX_JOBS_ACTIVE = "taskite.pd.worker.max.jobs.active";
  public static final String WORKER_LAST_RENEWAL = "taskite.pd.worker.last.renewal.at.seconds";
  public static final String JOB_SUBSCRIPTION_MIRROR_RELATIONS =
      "taskite.pd.job.subscription.mirror.relations";
  public static final String JOB_SUBSCRIPTION_DISTRIBUTION_TARGETS =
      "taskite.pd.job.subscription.distribution.targets";
  public static final String JOB_SUBSCRIPTION_DISTRIBUTION_PENDING_PARTITIONS =
      "taskite.pd.job.subscription.distribution.pending.partitions";

  private final MeterRegistry registry;
  private final int changeHistoryLimit;
  private final AtomicLong clusterHealth = new AtomicLong();
  private final AtomicLong leaderAvailable = new AtomicLong();
  private final AtomicLong raftRole = new AtomicLong();
  private final AtomicLong raftTerm = new AtomicLong();
  private final AtomicLong raftCommitIndex = new AtomicLong();
  private final AtomicLong raftAppliedIndex = new AtomicLong();
  private final AtomicLong raftLastLogIndex = new AtomicLong();
  private final AtomicLong raftCommitLag = new AtomicLong();
  private final AtomicLong topologyVersion = new AtomicLong();
  private final AtomicLong managedClusters = new AtomicLong();
  private final AtomicLong jobSubscriptionMirrorRelations = new AtomicLong();
  private final List<Meter> fixedMeters = new ArrayList<>();
  private final MultiGauge clusterMemberRoles;
  private final MultiGauge clusterPartitions;
  private final MultiGauge memberChangeActive;
  private final MultiGauge memberChangeProgress;
  private final MultiGauge memberChangeStartedAt;
  private final MultiGauge memberChangeOperations;
  private final MultiGauge brokerStatuses;
  private final MultiGauge brokerLastRenewal;
  private final MultiGauge brokerClusterHealth;
  private final MultiGauge brokerChangeActive;
  private final MultiGauge brokerChangeProgress;
  private final MultiGauge brokerChangeStartedAt;
  private final MultiGauge brokerChangePartitions;
  private final MultiGauge brokerPartitionChangeProgress;
  private final MultiGauge brokerChangeOperations;
  private final MultiGauge partitionHealth;
  private final MultiGauge partitionRoles;
  private final MultiGauge workerStatuses;
  private final MultiGauge workerActiveJobs;
  private final MultiGauge workerMaxJobsActive;
  private final MultiGauge workerLastRenewal;
  private final MultiGauge jobSubscriptionDistributionTargets;
  private final MultiGauge jobSubscriptionDistributionPendingPartitions;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final BrokerElectionMetrics electionMetrics;
  private final BrokerPlanMetrics brokerPlanMetrics;

  public PlacementDriverMetrics(final MeterRegistry registry, final String nodeId) {
    this(registry, nodeId, PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT);
  }

  public PlacementDriverMetrics(
      final MeterRegistry registry, final String nodeId, final int changeHistoryLimit) {
    if (changeHistoryLimit <= 0) {
      throw new IllegalArgumentException("change history limit must be greater than zero");
    }
    this.registry = Objects.requireNonNull(registry, "registry");
    this.changeHistoryLimit = changeHistoryLimit;
    final String localNodeId = Objects.requireNonNull(nodeId, "nodeId");
    final Tags commonTags = Tags.of("pd_node", localNodeId);
    electionMetrics = new BrokerElectionMetrics(registry, commonTags);
    brokerPlanMetrics = new BrokerPlanMetrics(registry, commonTags);

    fixedMeters.add(
        gauge(
            CLUSTER_HEALTH,
            "Whether this placement-driver node observes an elected cluster leader.",
            commonTags,
            clusterHealth));
    fixedMeters.add(
        gauge(
            LEADER_AVAILABLE,
            "Whether the placement-driver Raft leader is known to this node.",
            commonTags,
            leaderAvailable));
    fixedMeters.add(
        gauge(
            RAFT_ROLE,
            "Local placement-driver Raft role: 0 unknown, 1 follower, 2 candidate, 3 leader, 4 learner.",
            commonTags,
            raftRole));
    fixedMeters.add(gauge(RAFT_TERM, "Current placement-driver Raft term.", commonTags, raftTerm));
    fixedMeters.add(
        gauge(
            RAFT_COMMIT_INDEX,
            "Highest committed placement-driver Raft log index.",
            commonTags,
            raftCommitIndex));
    fixedMeters.add(
        gauge(
            RAFT_APPLIED_INDEX,
            "Highest applied placement-driver Raft log index.",
            commonTags,
            raftAppliedIndex));
    fixedMeters.add(
        gauge(
            RAFT_LAST_LOG_INDEX,
            "Highest local placement-driver Raft log index.",
            commonTags,
            raftLastLogIndex));
    fixedMeters.add(
        gauge(
            RAFT_COMMIT_LAG,
            "Committed placement-driver Raft entries not yet applied locally.",
            commonTags,
            raftCommitLag));
    fixedMeters.add(
        gauge(
            TOPOLOGY_VERSION,
            "Version of the committed placement-driver topology state.",
            commonTags,
            topologyVersion));
    fixedMeters.add(
        gauge(
            MANAGED_CLUSTERS,
            "Number of Broker clusters managed by the placement-driver.",
            commonTags,
            managedClusters));
    fixedMeters.add(
        gauge(
            JOB_SUBSCRIPTION_MIRROR_RELATIONS,
            "Number of committed resource Mirror relations.",
            commonTags,
            jobSubscriptionMirrorRelations));

    clusterMemberRoles =
        multiGauge(
            CLUSTER_MEMBER_ROLE,
            "Committed placement-driver member role: 0 unknown, 1 follower, 2 candidate, 3 leader, 4 learner.",
            commonTags);
    clusterPartitions =
        multiGauge(
            CLUSTER_PARTITIONS,
            "Number of configured partitions in a managed Broker cluster.",
            commonTags);
    memberChangeActive =
        multiGauge(
            MEMBER_CHANGE_ACTIVE,
            "Whether a placement-driver membership change is active.",
            commonTags);
    memberChangeProgress =
        multiGauge(
            MEMBER_CHANGE_PROGRESS,
            "Completed fraction of a recent placement-driver membership change.",
            commonTags);
    memberChangeStartedAt =
        multiGauge(
            MEMBER_CHANGE_STARTED_AT,
            "Unix timestamp when a recent placement-driver membership change was requested.",
            commonTags);
    memberChangeOperations =
        multiGauge(
            MEMBER_CHANGE_OPERATIONS,
            "Placement-driver membership operations grouped by type and status.",
            commonTags);
    brokerStatuses =
        multiGauge(
            BROKER_STATUS,
            "Registered Broker status. The active status label has value 1.",
            commonTags);
    brokerLastRenewal =
        multiGauge(
            BROKER_LAST_RENEWAL,
            "Unix timestamp of the most recent Broker registration or renewal.",
            commonTags);
    workerStatuses =
        multiGauge(WORKER_STATUS, "Registered Worker status: 1 online, 0 offline.", commonTags);
    workerActiveJobs =
        multiGauge(
            WORKER_ACTIVE_JOBS, "Current active jobs reported by a registered Worker.", commonTags);
    workerMaxJobsActive =
        multiGauge(
            WORKER_MAX_JOBS_ACTIVE,
            "Maximum active jobs configured for a registered Worker.",
            commonTags);
    workerLastRenewal =
        multiGauge(
            WORKER_LAST_RENEWAL,
            "Unix timestamp of the most recent Worker registration or renewal.",
            commonTags);
    jobSubscriptionDistributionTargets =
        multiGauge(
            JOB_SUBSCRIPTION_DISTRIBUTION_TARGETS,
            "Job subscription distribution targets grouped by cluster and completion status.",
            commonTags);
    jobSubscriptionDistributionPendingPartitions =
        multiGauge(
            JOB_SUBSCRIPTION_DISTRIBUTION_PENDING_PARTITIONS,
            "Pending job subscription distribution partitions grouped by target cluster.",
            commonTags);
    brokerClusterHealth =
        multiGauge(
            BROKER_CLUSTER_HEALTH,
            "Whether every configured partition in a Broker cluster is healthy.",
            commonTags);
    brokerChangeActive =
        multiGauge(BROKER_CHANGE_ACTIVE, "Whether a Broker topology change is active.", commonTags);
    brokerChangeProgress =
        multiGauge(
            BROKER_CHANGE_PROGRESS,
            "Completed fraction of a recent Broker topology change.",
            commonTags);
    brokerChangeStartedAt =
        multiGauge(
            BROKER_CHANGE_STARTED_AT,
            "Unix timestamp when a recent Broker topology change was requested.",
            commonTags);
    brokerChangePartitions =
        multiGauge(
            BROKER_CHANGE_PARTITIONS,
            "Partition plans in an active Broker topology change grouped by status.",
            commonTags);
    brokerPartitionChangeProgress =
        multiGauge(
            BROKER_PARTITION_CHANGE_PROGRESS,
            "Completed fraction of an active Broker partition change.",
            commonTags);
    brokerChangeOperations =
        multiGauge(
            BROKER_CHANGE_OPERATIONS,
            "Low-level operations in an active Broker topology change grouped by type and status.",
            commonTags);
    partitionHealth =
        multiGauge(
            PARTITION_HEALTH,
            "Whether a Broker partition is serving with one leader and an active voter quorum.",
            commonTags);
    partitionRoles =
        multiGauge(
            PARTITION_ROLE,
            "Observed Broker replica role: 0 unknown, 1 follower, 2 candidate, 3 leader, 4 learner.",
            commonTags);
  }

  public synchronized void update(
      final RaftPartition raftPartition, final ProcessingState processingState) {
    if (closed.get()) {
      return;
    }
    electionMetrics.update(processingState);
    brokerPlanMetrics.update(processingState);

    final String leaderId = nullToEmpty(raftPartition.leaderId());
    leaderAvailable.set(leaderId.isBlank() ? 0 : 1);
    clusterHealth.set(leaderId.isBlank() ? 0 : 1);
    raftRole.set(roleValue(raftPartition.role()));
    raftTerm.set(Math.max(0, raftPartition.term()));
    raftCommitIndex.set(Math.max(0, raftPartition.committedIndex()));
    raftAppliedIndex.set(Math.max(0, raftPartition.lastAppliedIndex()));
    raftLastLogIndex.set(Math.max(0, raftPartition.lastIndex()));
    raftCommitLag.set(
        Math.max(0, raftPartition.committedIndex() - raftPartition.lastAppliedIndex()));
    topologyVersion.set(Math.max(0, processingState.getVersion()));

    clusterMemberRoles.register(memberRoleRows(raftPartition.members()), true);
    final List<String> clusterIds = processingState.getBrokerClusterState().clusterIds();
    managedClusters.set(clusterIds.size());
    jobSubscriptionMirrorRelations.set(
        processingState.getJobSubscriptionMirrorState().mirrors().size());

    final List<MultiGauge.Row<?>> clusterPartitionRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> memberChangeActiveRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> memberChangeProgressRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> memberChangeStartedAtRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> memberChangeOperationRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> statusRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerLastRenewalRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> clusterHealthRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerChangeActiveRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerChangeProgressRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerChangeStartedAtRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerChangePartitionRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerPartitionChangeProgressRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> brokerChangeOperationRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> partitionHealthRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> partitionRoleRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> workerStatusRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> workerActiveJobRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> workerMaxJobRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> workerLastRenewalRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> jobSubscriptionDistributionTargetRows = new ArrayList<>();
    final List<MultiGauge.Row<?>> jobSubscriptionDistributionPendingRows = new ArrayList<>();
    addJobSubscriptionDistributionRows(
        processingState,
        jobSubscriptionDistributionTargetRows,
        jobSubscriptionDistributionPendingRows);
    addPlacementDriverMemberChangeRows(
        processingState,
        changeHistoryLimit,
        memberChangeActiveRows,
        memberChangeProgressRows,
        memberChangeStartedAtRows,
        memberChangeOperationRows);
    if (raftPartition.role() == RaftRole.LEADER) {
      for (final WorkerRegistration registration :
          processingState.getWorkerRegistrationState().registrations()) {
        final Tags tags =
            Tags.of(
                "cluster",
                registration.clusterId(),
                "tenant",
                registration.tenantId(),
                "name",
                registration.name(),
                "worker_id",
                registration.workerId(),
                "job_type",
                registration.jobType(),
                "client_version",
                registration.clientVersion());
        workerStatusRows.add(
            MultiGauge.Row.of(tags, registration.status() == WorkerStatus.ONLINE ? 1 : 0));
        workerActiveJobRows.add(MultiGauge.Row.of(tags, registration.activeJobs()));
        workerMaxJobRows.add(MultiGauge.Row.of(tags, registration.maxJobsActive()));
        workerLastRenewalRows.add(
            MultiGauge.Row.of(
                tags.and("status", registration.status().name()),
                registration.lastRenewal().toEpochMilli() / 1000D));
      }
    }
    for (final String clusterId : clusterIds) {
      final List<BrokerRegistration> registrations =
          processingState.getBrokerRegistrationState().registrations(clusterId);
      final Map<String, BrokerRegistration> registrationsById = new LinkedHashMap<>();
      for (final BrokerRegistration registration : registrations) {
        registrationsById.put(registration.brokerId(), registration);
        statusRows.add(
            MultiGauge.Row.of(
                Tags.of(
                    "cluster",
                    clusterId,
                    "broker",
                    registration.brokerId(),
                    "status",
                    registration.status().name()),
                1));
        brokerLastRenewalRows.add(
            MultiGauge.Row.of(
                Tags.of(
                    "cluster",
                    clusterId,
                    "broker",
                    registration.brokerId(),
                    "status",
                    registration.status().name(),
                    "host",
                    nullToEmpty(registration.host()),
                    "command_port",
                    Integer.toString(registration.port()),
                    "raft_port",
                    Integer.toString(registration.raftPort()),
                    "version",
                    nullToEmpty(registration.version())),
                registration.lastRenewal().toEpochMilli() / 1000D));
      }

      final int partitionCount =
          processingState
              .getBrokerClusterConfigState()
              .findClusterConfig(clusterId)
              .map(config -> config.partitionSize())
              .orElse(0);
      clusterPartitionRows.add(MultiGauge.Row.of(Tags.of("cluster", clusterId), partitionCount));
      final Map<Integer, PartitionTopology> topologiesById = new LinkedHashMap<>();
      processingState
          .getBrokerPartitionTopologyState()
          .partitionTopologies(clusterId)
          .forEach(topology -> topologiesById.put(topology.partitionId(), topology));

      boolean allPartitionsHealthy = partitionCount > 0;
      for (int partitionId = 1; partitionId <= partitionCount; partitionId++) {
        final PartitionTopology topology = topologiesById.get(partitionId);
        final boolean healthy =
            topology != null && addPartitionRows(topology, registrationsById, partitionRoleRows);
        partitionHealthRows.add(
            MultiGauge.Row.of(
                Tags.of("cluster", clusterId, "partition", Integer.toString(partitionId)),
                healthy ? 1 : 0));
        allPartitionsHealthy &= healthy;
      }
      clusterHealthRows.add(
          MultiGauge.Row.of(Tags.of("cluster", clusterId), allPartitionsHealthy ? 1 : 0));
      addBrokerChangeRows(
          processingState,
          clusterId,
          changeHistoryLimit,
          brokerChangeActiveRows,
          brokerChangeProgressRows,
          brokerChangeStartedAtRows,
          brokerChangePartitionRows,
          brokerPartitionChangeProgressRows,
          brokerChangeOperationRows);
    }

    clusterPartitions.register(clusterPartitionRows, true);
    memberChangeActive.register(memberChangeActiveRows, true);
    memberChangeProgress.register(memberChangeProgressRows, true);
    memberChangeStartedAt.register(memberChangeStartedAtRows, true);
    memberChangeOperations.register(memberChangeOperationRows, true);
    brokerStatuses.register(statusRows, true);
    brokerLastRenewal.register(brokerLastRenewalRows, true);
    brokerClusterHealth.register(clusterHealthRows, true);
    brokerChangeActive.register(brokerChangeActiveRows, true);
    brokerChangeProgress.register(brokerChangeProgressRows, true);
    brokerChangeStartedAt.register(brokerChangeStartedAtRows, true);
    brokerChangePartitions.register(brokerChangePartitionRows, true);
    brokerPartitionChangeProgress.register(brokerPartitionChangeProgressRows, true);
    brokerChangeOperations.register(brokerChangeOperationRows, true);
    partitionHealth.register(partitionHealthRows, true);
    partitionRoles.register(partitionRoleRows, true);
    workerStatuses.register(workerStatusRows, true);
    workerActiveJobs.register(workerActiveJobRows, true);
    workerMaxJobsActive.register(workerMaxJobRows, true);
    workerLastRenewal.register(workerLastRenewalRows, true);
    jobSubscriptionDistributionTargets.register(jobSubscriptionDistributionTargetRows, true);
    jobSubscriptionDistributionPendingPartitions.register(
        jobSubscriptionDistributionPendingRows, true);
  }

  @Override
  public synchronized void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    electionMetrics.close();
    brokerPlanMetrics.close();
    clusterMemberRoles.register(List.of(), true);
    clusterPartitions.register(List.of(), true);
    memberChangeActive.register(List.of(), true);
    memberChangeProgress.register(List.of(), true);
    memberChangeStartedAt.register(List.of(), true);
    memberChangeOperations.register(List.of(), true);
    brokerStatuses.register(List.of(), true);
    brokerLastRenewal.register(List.of(), true);
    brokerClusterHealth.register(List.of(), true);
    brokerChangeActive.register(List.of(), true);
    brokerChangeProgress.register(List.of(), true);
    brokerChangeStartedAt.register(List.of(), true);
    brokerChangePartitions.register(List.of(), true);
    brokerPartitionChangeProgress.register(List.of(), true);
    brokerChangeOperations.register(List.of(), true);
    partitionHealth.register(List.of(), true);
    partitionRoles.register(List.of(), true);
    workerStatuses.register(List.of(), true);
    workerActiveJobs.register(List.of(), true);
    workerMaxJobsActive.register(List.of(), true);
    workerLastRenewal.register(List.of(), true);
    jobSubscriptionDistributionTargets.register(List.of(), true);
    jobSubscriptionDistributionPendingPartitions.register(List.of(), true);
    fixedMeters.forEach(registry::remove);
    fixedMeters.clear();
  }

  private static void addJobSubscriptionDistributionRows(
      final ProcessingState state,
      final List<MultiGauge.Row<?>> targetRows,
      final List<MultiGauge.Row<?>> pendingRows) {
    final Map<ResourceTargetKey, Integer> targetCounts = new LinkedHashMap<>();
    final Map<String, Integer> pendingCounts = new LinkedHashMap<>();
    final var distributionState = state.getCommandDistributionState();
    for (final var distribution : distributionState.distributions()) {
      for (final var target : distributionState.targets(distribution.getDistributionKey())) {
        final String clusterId = target.getClusterId();
        final JobSubscriptionDistributionStatus status =
            target.isCompleted()
                ? JobSubscriptionDistributionStatus.COMPLETED
                : JobSubscriptionDistributionStatus.DISTRIBUTING;
        targetCounts.merge(new ResourceTargetKey(clusterId, status), 1, Integer::sum);
        pendingCounts.merge(
            clusterId,
            distributionState
                .pendingPartitions(distribution.getDistributionKey(), clusterId)
                .size(),
            Integer::sum);
      }
    }
    targetCounts.forEach(
        (key, count) ->
            targetRows.add(
                MultiGauge.Row.of(
                    Tags.of("cluster", key.clusterId(), "status", metricValue(key.status().name())),
                    count)));
    pendingCounts.forEach(
        (clusterId, count) ->
            pendingRows.add(MultiGauge.Row.of(Tags.of("cluster", clusterId), count)));
  }

  private static void addPlacementDriverMemberChangeRows(
      final ProcessingState state,
      final int historyLimit,
      final List<MultiGauge.Row<?>> activeRows,
      final List<MultiGauge.Row<?>> progressRows,
      final List<MultiGauge.Row<?>> startedAtRows,
      final List<MultiGauge.Row<?>> operationRows) {
    final String activeChangeId =
        state
            .getMemberChangeState()
            .active(MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverCluster.CLUSTER_ID)
            .map(MemberChangeRecord::getChangeId)
            .orElse("");
    state
        .getMemberChangeState()
        .recentChanges(
            MemberChangeScope.PLACEMENT_DRIVER,
            PlacementDriverCluster.CLUSTER_ID,
            retainedChangeLimit(historyLimit))
        .stream()
        .forEach(
            change ->
                addMemberChangeRows(
                    change,
                    change.getChangeId().equals(activeChangeId),
                    true,
                    activeRows,
                    progressRows,
                    startedAtRows,
                    operationRows));
  }

  private static void addMemberChangeRows(
      final MemberChangeRecord change,
      final boolean active,
      final boolean placementDriver,
      final List<MultiGauge.Row<?>> activeRows,
      final List<MultiGauge.Row<?>> progressRows,
      final List<MultiGauge.Row<?>> startedAtRows,
      final List<MultiGauge.Row<?>> operationRows) {
    final String changeType = memberChangeType(change, placementDriver);
    final String status = metricValue(change.getStatus().name());
    final Tags tags =
        Tags.of("change_id", change.getChangeId(), "change_type", changeType, "status", status);
    activeRows.add(MultiGauge.Row.of(tags, active ? 1 : 0));
    progressRows.add(
        MultiGauge.Row.of(
            tags,
            progress(
                change.getNextOperationIndex(),
                change.getOperations().size(),
                change.getStatus() == MemberChangeStatus.COMPLETED)));
    addStartedAtRow(startedAtRows, tags, change.getRequestedAtEpochMillis());

    final Map<MemberOperationKey, Integer> counts = new LinkedHashMap<>();
    for (final MemberChangeOperation operation : change.getOperations()) {
      counts.merge(
          new MemberOperationKey(metricValue(operation.getType().name()), operation.getStatus()),
          1,
          Integer::sum);
    }
    counts.forEach(
        (key, count) ->
            operationRows.add(
                MultiGauge.Row.of(
                    Tags.of(
                        "change_id",
                        change.getChangeId(),
                        "change_type",
                        changeType,
                        "operation_type",
                        key.operationType(),
                        "status",
                        metricValue(key.status().name())),
                    count)));
  }

  private static void addBrokerChangeRows(
      final ProcessingState state,
      final String clusterId,
      final int historyLimit,
      final List<MultiGauge.Row<?>> activeRows,
      final List<MultiGauge.Row<?>> progressRows,
      final List<MultiGauge.Row<?>> startedAtRows,
      final List<MultiGauge.Row<?>> partitionRows,
      final List<MultiGauge.Row<?>> partitionProgressRows,
      final List<MultiGauge.Row<?>> operationRows) {
    final String activeMemberChangeId =
        state
            .getMemberChangeState()
            .active(MemberChangeScope.BROKER, clusterId)
            .map(MemberChangeRecord::getChangeId)
            .orElse("");
    final List<MemberChangeRecord> memberChanges =
        state
            .getMemberChangeState()
            .recentChanges(MemberChangeScope.BROKER, clusterId, retainedChangeLimit(historyLimit));
    memberChanges.stream()
        .forEach(
            memberChange ->
                addBrokerPlanRows(
                    clusterId,
                    memberChange.getChangeId(),
                    memberChangeType(memberChange, false),
                    MetricChangeStatus.from(memberChange.getStatus()),
                    memberChange.getRequestedAtEpochMillis(),
                    memberChange.getChangeId().equals(activeMemberChangeId),
                    changePlans(state, clusterId, memberChange.getChildPartitionChangeIds()),
                    activeRows,
                    progressRows,
                    startedAtRows,
                    partitionRows,
                    partitionProgressRows,
                    operationRows));

    final String activeClusterChangeId =
        state
            .getBrokerTopologyChangeState()
            .activeClusterChange(clusterId)
            .map(ClusterChangeOperation::changeId)
            .orElse("");
    state
        .getBrokerTopologyChangeState()
        .recentClusterChanges(clusterId, retainedChangeLimit(historyLimit))
        .stream()
        .forEach(
            clusterChange -> {
              final List<ChangePlan> plans =
                  changePlans(state, clusterId, clusterChange.planChangeIds());
              final List<ChangePlan> partitionAdditions =
                  plans.stream()
                      .filter(
                          plan ->
                              plan.operations().stream()
                                  .anyMatch(
                                      operation -> operation.type() == TopologyOperationType.ADD))
                      .toList();
              final boolean active = clusterChange.changeId().equals(activeClusterChangeId);
              if (!partitionAdditions.isEmpty()) {
                addBrokerPlanRows(
                    clusterId,
                    clusterChange.changeId(),
                    "add_partition",
                    aggregatePlanStatus(partitionAdditions),
                    clusterChange.startedAtEpochMillis(),
                    active,
                    partitionAdditions,
                    activeRows,
                    progressRows,
                    startedAtRows,
                    partitionRows,
                    partitionProgressRows,
                    operationRows);
              }

              final boolean representedByMemberChange =
                  state
                      .getMemberChangeState()
                      .get(MemberChangeScope.BROKER, clusterId, clusterChange.changeId())
                      .isPresent();
              final String operationType = clusterOperationType(plans);
              final List<ChangePlan> remainingPlans =
                  partitionAdditions.isEmpty()
                      ? plans
                      : plans.stream().filter(plan -> !partitionAdditions.contains(plan)).toList();
              if (!representedByMemberChange
                  && (!remainingPlans.isEmpty() || partitionAdditions.isEmpty())) {
                addBrokerPlanRows(
                    clusterId,
                    clusterChange.changeId(),
                    operationType,
                    MetricChangeStatus.from(clusterChange.status()),
                    clusterChange.startedAtEpochMillis(),
                    active,
                    remainingPlans,
                    activeRows,
                    progressRows,
                    startedAtRows,
                    partitionRows,
                    partitionProgressRows,
                    operationRows);
              }
            });
  }

  private static void addBrokerPlanRows(
      final String clusterId,
      final String changeId,
      final String changeType,
      final MetricChangeStatus status,
      final long startedAtEpochMillis,
      final boolean active,
      final List<ChangePlan> plans,
      final List<MultiGauge.Row<?>> activeRows,
      final List<MultiGauge.Row<?>> progressRows,
      final List<MultiGauge.Row<?>> startedAtRows,
      final List<MultiGauge.Row<?>> partitionRows,
      final List<MultiGauge.Row<?>> partitionProgressRows,
      final List<MultiGauge.Row<?>> operationRows) {
    final Tags changeTags =
        Tags.of(
            "cluster",
            clusterId,
            "change_id",
            changeId,
            "change_type",
            changeType,
            "status",
            metricValue(status.name()));
    activeRows.add(MultiGauge.Row.of(changeTags, active ? 1 : 0));
    addStartedAtRow(startedAtRows, changeTags, startedAtEpochMillis);
    final int operationCount =
        plans.stream()
            .mapToInt(
                plan ->
                    plan.operations().size() + (requiresJobSubscriptionDistribution(plan) ? 1 : 0))
            .sum();
    final int completedOperationCount =
        plans.stream()
            .mapToInt(
                plan ->
                    Math.min(plan.operations().size(), Math.max(0, plan.nextOperationIndex()))
                        + (requiresJobSubscriptionDistribution(plan)
                                && plan.status() == BrokerPartitionChangeStatus.COMPLETED
                            ? 1
                            : 0))
            .sum();
    progressRows.add(
        MultiGauge.Row.of(
            changeTags,
            progress(
                completedOperationCount, operationCount, status == MetricChangeStatus.COMPLETED)));

    final Map<BrokerPartitionChangeStatus, Integer> partitionCounts = new LinkedHashMap<>();
    final Map<BrokerOperationKey, Integer> operationCounts = new LinkedHashMap<>();
    for (final ChangePlan plan : plans) {
      final BrokerPartitionChangeStatus planStatus = plan.status();
      final boolean requiresJobSubscriptionDistribution = requiresJobSubscriptionDistribution(plan);
      final int partitionOperationCount =
          plan.operations().size() + (requiresJobSubscriptionDistribution ? 1 : 0);
      final int completedPartitionOperationCount =
          Math.min(plan.operations().size(), Math.max(0, plan.nextOperationIndex()))
              + (requiresJobSubscriptionDistribution
                      && planStatus == BrokerPartitionChangeStatus.COMPLETED
                  ? 1
                  : 0);
      partitionCounts.merge(planStatus, 1, Integer::sum);
      partitionProgressRows.add(
          MultiGauge.Row.of(
              Tags.of(
                  "cluster",
                  clusterId,
                  "change_id",
                  changeId,
                  "partition",
                  Integer.toString(plan.partitionId()),
                  "change_type",
                  changeType,
                  "status",
                  metricValue(planStatus.name())),
              progress(
                  completedPartitionOperationCount,
                  partitionOperationCount,
                  planStatus == BrokerPartitionChangeStatus.COMPLETED)));
      for (int index = 0; index < plan.operations().size(); index++) {
        final MetricChangeStatus operationStatus =
            index < plan.nextOperationIndex()
                ? MetricChangeStatus.COMPLETED
                : index == plan.nextOperationIndex()
                    ? MetricChangeStatus.from(planStatus)
                    : MetricChangeStatus.PENDING;
        operationCounts.merge(
            new BrokerOperationKey(
                metricValue(plan.operations().get(index).type().name()), operationStatus),
            1,
            Integer::sum);
      }
      if (requiresJobSubscriptionDistribution) {
        final MetricChangeStatus distributionStatus =
            switch (plan.status()) {
              case COMPLETED -> MetricChangeStatus.COMPLETED;
              case FAILED -> MetricChangeStatus.FAILED;
              case IN_PROGRESS ->
                  plan.hasMoreOperations()
                      ? MetricChangeStatus.PENDING
                      : MetricChangeStatus.IN_PROGRESS;
              default -> MetricChangeStatus.PENDING;
            };
        operationCounts.merge(
            new BrokerOperationKey("distribute_resources", distributionStatus), 1, Integer::sum);
      }
    }
    partitionCounts.forEach(
        (planStatus, count) ->
            partitionRows.add(
                MultiGauge.Row.of(
                    Tags.of(
                        "cluster",
                        clusterId,
                        "change_id",
                        changeId,
                        "change_type",
                        changeType,
                        "status",
                        metricValue(planStatus.name())),
                    count)));
    operationCounts.forEach(
        (key, count) ->
            operationRows.add(
                MultiGauge.Row.of(
                    Tags.of(
                        "cluster",
                        clusterId,
                        "change_id",
                        changeId,
                        "change_type",
                        changeType,
                        "operation_type",
                        key.operationType(),
                        "status",
                        metricValue(key.status().name())),
                    count)));
  }

  private static boolean requiresJobSubscriptionDistribution(final ChangePlan plan) {
    return plan.operations().stream()
        .anyMatch(operation -> operation.type() == TopologyOperationType.ADD);
  }

  private static List<ChangePlan> changePlans(
      final ProcessingState state, final String clusterId, final List<String> changeIds) {
    return changeIds.stream()
        .map(
            changeId ->
                state.getBrokerTopologyChangeState().changePlan(clusterId, changeId).orElse(null))
        .filter(Objects::nonNull)
        .toList();
  }

  private static String clusterOperationType(final List<ChangePlan> plans) {
    if (plans.stream().anyMatch(plan -> plan.changeId().contains(":rebalance:"))) {
      return "rebalance";
    }
    if (plans.stream().anyMatch(plan -> plan.changeId().contains(":quorum-recovery:"))) {
      return "quorum_recovery";
    }
    return "topology_change";
  }

  private static MetricChangeStatus aggregatePlanStatus(final List<ChangePlan> plans) {
    if (plans.stream().allMatch(ChangePlan::isTerminal)) {
      return plans.stream().allMatch(plan -> plan.status() == BrokerPartitionChangeStatus.COMPLETED)
          ? MetricChangeStatus.COMPLETED
          : MetricChangeStatus.FAILED;
    }
    if (plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.FAILED)) {
      return MetricChangeStatus.FAILING;
    }
    if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.WAITING_READY)) {
      return MetricChangeStatus.WAITING_READY;
    }
    if (plans.stream()
        .anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.WAITING_LEADER_CONVERGE)) {
      return MetricChangeStatus.WAITING_LEADER_CONVERGE;
    }
    if (plans.stream().anyMatch(plan -> plan.status() == BrokerPartitionChangeStatus.IN_PROGRESS)) {
      return MetricChangeStatus.IN_PROGRESS;
    }
    return MetricChangeStatus.PENDING;
  }

  private static String memberChangeType(
      final MemberChangeRecord change, final boolean placementDriver) {
    final Set<String> source = Set.copyOf(change.getSourceVoters());
    final Set<String> target = Set.copyOf(change.getTargetVoters());
    final boolean addsMember = target.stream().anyMatch(member -> !source.contains(member));
    final boolean removesMember = source.stream().anyMatch(member -> !target.contains(member));
    if (addsMember && removesMember) {
      return "replace";
    }
    if (addsMember) {
      return placementDriver ? "add_node" : "scale_up";
    }
    if (removesMember) {
      return placementDriver ? "remove_node" : "scale_down";
    }
    return "resize";
  }

  private static double progress(
      final int completed, final int total, final boolean completedChange) {
    if (total <= 0) {
      return completedChange ? 1 : 0;
    }
    return Math.min(1D, Math.max(0D, (double) completed / total));
  }

  private static int retainedChangeLimit(final int historyLimit) {
    return historyLimit == Integer.MAX_VALUE ? historyLimit : historyLimit + 1;
  }

  private static void addStartedAtRow(
      final List<MultiGauge.Row<?>> rows, final Tags tags, final long startedAtEpochMillis) {
    if (startedAtEpochMillis > 0) {
      rows.add(MultiGauge.Row.of(tags, startedAtEpochMillis / 1000D));
    }
  }

  private static String metricValue(final String value) {
    return value.toLowerCase(Locale.ROOT);
  }

  private boolean addPartitionRows(
      final PartitionTopology topology,
      final Map<String, BrokerRegistration> registrations,
      final List<MultiGauge.Row<?>> roleRows) {
    final List<String> assignedBrokers = new ArrayList<>(topology.currentVoters());
    topology.learners().stream()
        .filter(brokerId -> !assignedBrokers.contains(brokerId))
        .forEach(assignedBrokers::add);

    long highestVoterTerm = -1;
    final Map<String, BrokerPartitionState> voterStates = new LinkedHashMap<>();
    for (final String brokerId : assignedBrokers) {
      final BrokerRegistration registration = registrations.get(brokerId);
      final BrokerPartitionState state =
          registration == null ? null : partitionState(registration, topology.partitionId());
      final boolean voter = topology.currentVoters().contains(brokerId);
      final boolean replicaHealthy = isHealthyReplica(registration, state, voter);
      roleRows.add(
          MultiGauge.Row.of(
              partitionTags(topology, brokerId), observedRoleValue(registration, state)));
      if (voter && replicaHealthy) {
        voterStates.put(brokerId, state);
        highestVoterTerm = Math.max(highestVoterTerm, state.term());
      }
    }

    int votersAtHighestTerm = 0;
    int leadersAtHighestTerm = 0;
    for (final BrokerPartitionState state : voterStates.values()) {
      if (state.term() == highestVoterTerm) {
        votersAtHighestTerm++;
        if (state.role() == MemberRole.LEADER) {
          leadersAtHighestTerm++;
        }
      }
    }
    final int quorum = topology.currentVoters().size() / 2 + 1;
    return (topology.status() == PartitionStatus.ACTIVE
            || topology.status() == PartitionStatus.REBALANCING)
        && !topology.currentVoters().isEmpty()
        && leadersAtHighestTerm == 1
        && votersAtHighestTerm >= quorum;
  }

  private static List<MultiGauge.Row<?>> memberRoleRows(final List<RaftMember> members) {
    final List<MultiGauge.Row<?>> rows = new ArrayList<>();
    members.forEach(
        member ->
            rows.add(
                MultiGauge.Row.of(Tags.of("member", member.memberId()), roleValue(member.role()))));
    return rows;
  }

  private static BrokerPartitionState partitionState(
      final BrokerRegistration registration, final int partitionId) {
    return registration.partitions().stream()
        .filter(partition -> partition.partitionId() == partitionId)
        .findFirst()
        .orElse(null);
  }

  private static boolean isHealthyReplica(
      final BrokerRegistration registration,
      final BrokerPartitionState state,
      final boolean voter) {
    if (registration == null
        || registration.status() == BrokerStatus.OFFLINE
        || state == null
        || state.lifecycle() != BrokerPartitionLifecycle.ACTIVE) {
      return false;
    }
    return voter
        ? state.role() == MemberRole.LEADER || state.role() == MemberRole.FOLLOWER
        : state.role() == MemberRole.LEARNER;
  }

  private static int observedRoleValue(
      final BrokerRegistration registration, final BrokerPartitionState state) {
    if (registration == null || registration.status() == BrokerStatus.OFFLINE || state == null) {
      return 0;
    }
    try {
      return roleValue(RaftRole.valueOf(state.role().name()));
    } catch (final IllegalArgumentException ignored) {
      return 0;
    }
  }

  private static int roleValue(final RaftRole role) {
    if (role == null) {
      return 0;
    }
    return switch (role) {
      case FOLLOWER -> 1;
      case CANDIDATE -> 2;
      case LEADER -> 3;
      case LEARNER -> 4;
    };
  }

  private static Tags partitionTags(final PartitionTopology topology, final String brokerId) {
    return Tags.of(
        "cluster",
        topology.clusterId(),
        "partition",
        Integer.toString(topology.partitionId()),
        "broker",
        brokerId);
  }

  private Meter gauge(
      final String name, final String description, final Tags tags, final AtomicLong value) {
    return Gauge.builder(name, value, AtomicLong::get)
        .description(description)
        .tags(tags)
        .register(registry);
  }

  private MultiGauge multiGauge(final String name, final String description, final Tags tags) {
    return MultiGauge.builder(name).description(description).tags(tags).register(registry);
  }

  private static String nullToEmpty(final String value) {
    return value == null ? "" : value;
  }

  private record MemberOperationKey(String operationType, MemberChangeOperationStatus status) {}

  private record BrokerOperationKey(String operationType, MetricChangeStatus status) {}

  private record ResourceTargetKey(String clusterId, JobSubscriptionDistributionStatus status) {}

  private enum MetricChangeStatus {
    QUEUED,
    PENDING,
    WAITING_CATCH_UP,
    IN_PROGRESS,
    WAITING_READY,
    WAITING_LEADER_CONVERGE,
    SUPERSEDING,
    SUPERSEDED,
    CANCELLING,
    COMPLETED,
    FAILED,
    FAILING,
    CANCELLED;

    private static MetricChangeStatus from(final Enum<?> status) {
      return valueOf(status.name());
    }
  }
}
