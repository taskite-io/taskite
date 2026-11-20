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
package io.taskite.placementdriver.api.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RegistrationStatus;
import java.util.List;
import java.util.Map;

/**
 * Read-only HTTP response DTOs for the placement-driver admin/observability endpoints. Serialized
 * to JSON by Jackson; {@code null} fields are omitted so a single shape can serve both the richer
 * local view and the leaner peer view.
 */
public final class PlacementDriverHttpModel {
  private PlacementDriverHttpModel() {}

  public enum HealthStatus {
    UP
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record HealthResponse(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Endpoint endpointData,
      @JsonUnwrapped Consensus consensusData,
      @JsonUnwrapped Progress progressData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(
        HealthStatus status,
        String nodeId,
        MemberRole role,
        String leaderId,
        String leaderEndpoint) {}

    public HealthStatus status() {
      return identityData.status();
    }

    public String nodeId() {
      return identityData.nodeId();
    }

    public MemberRole role() {
      return identityData.role();
    }

    public String leaderId() {
      return identityData.leaderId();
    }

    public String leaderEndpoint() {
      return identityData.leaderEndpoint();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Endpoint(String host, int port, int grpcPort) {}

    public String host() {
      return endpointData.host();
    }

    public int port() {
      return endpointData.port();
    }

    public int grpcPort() {
      return endpointData.grpcPort();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Consensus(long term, int quorum, int peerCount) {}

    public long term() {
      return consensusData.term();
    }

    public int quorum() {
      return consensusData.quorum();
    }

    public int peerCount() {
      return consensusData.peerCount();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Progress(long commitIndex, long lastApplied, long lastLogIndex) {}

    public long commitIndex() {
      return progressData.commitIndex();
    }

    public long lastApplied() {
      return progressData.lastApplied();
    }

    public long lastLogIndex() {
      return progressData.lastLogIndex();
    }
  }

  public record PlacementDriverMemberView(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Endpoint endpointData,
      @JsonUnwrapped Registration registrationData,
      @JsonUnwrapped Progress progressData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String memberId, String endpoint, MemberRole role, boolean self) {}

    public String memberId() {
      return identityData.memberId();
    }

    public String endpoint() {
      return identityData.endpoint();
    }

    public MemberRole role() {
      return identityData.role();
    }

    public boolean self() {
      return identityData.self();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Endpoint(String host, int httpPort, int grpcPort, int raftPort) {}

    public String host() {
      return endpointData.host();
    }

    public int httpPort() {
      return endpointData.httpPort();
    }

    public int grpcPort() {
      return endpointData.grpcPort();
    }

    public int raftPort() {
      return endpointData.raftPort();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Registration(
        RegistrationStatus status, String incarnationId, long endpointEpoch) {}

    public RegistrationStatus status() {
      return registrationData.status();
    }

    public String incarnationId() {
      return registrationData.incarnationId();
    }

    public long endpointEpoch() {
      return registrationData.endpointEpoch();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Progress(
        long commitIndex, long lastApplied, long lastLogIndex, boolean readyForPromotion) {}

    public long commitIndex() {
      return progressData.commitIndex();
    }

    public long lastApplied() {
      return progressData.lastApplied();
    }

    public long lastLogIndex() {
      return progressData.lastLogIndex();
    }

    public boolean readyForPromotion() {
      return progressData.readyForPromotion();
    }

    public PlacementDriverMemberView(
        String memberId, String endpoint, MemberRole role, boolean self) {
      this(
          new Identity(memberId, endpoint, role, self),
          new Endpoint("", 0, 0, 0),
          new Registration(RegistrationStatus.UNKNOWN, "", 0),
          new Progress(0, 0, 0, false));
    }
  }

  public record PlacementDriverTopologyResponse(
      @JsonUnwrapped Leadership leadershipData, @JsonUnwrapped Members membersData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Leadership(
        String localMemberId, String leaderId, String leaderEndpoint, long term, int quorum) {}

    public String localMemberId() {
      return leadershipData.localMemberId();
    }

    public String leaderId() {
      return leadershipData.leaderId();
    }

    public String leaderEndpoint() {
      return leadershipData.leaderEndpoint();
    }

    public long term() {
      return leadershipData.term();
    }

    public int quorum() {
      return leadershipData.quorum();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Members(List<PlacementDriverMemberView> members) {}

    public List<PlacementDriverMemberView> members() {
      return membersData.members();
    }
  }

  public record BrokerPartitionView(
      @JsonUnwrapped State stateData,
      @JsonUnwrapped Replication replicationData,
      @JsonUnwrapped Operation operationData,
      @JsonUnwrapped Election electionData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record State(
        int partitionId, MemberRole role, long term, BrokerPartitionLifecycle lifecycleState) {}

    public int partitionId() {
      return stateData.partitionId();
    }

    public MemberRole role() {
      return stateData.role();
    }

    public long term() {
      return stateData.term();
    }

    public BrokerPartitionLifecycle lifecycleState() {
      return stateData.lifecycleState();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Replication(long matchIndex, long lag, boolean readyForPromotion) {}

    public long matchIndex() {
      return replicationData.matchIndex();
    }

    public long lag() {
      return replicationData.lag();
    }

    public boolean readyForPromotion() {
      return replicationData.readyForPromotion();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Operation(String lastOpId, BrokerTopologyOperationStatus lastOpStatus) {}

    public String lastOpId() {
      return operationData.lastOpId();
    }

    public BrokerTopologyOperationStatus lastOpStatus() {
      return operationData.lastOpStatus();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Election(
        int priority, long recoveryEpoch, BrokerPartitionElectionState election) {}

    public int priority() {
      return electionData.priority();
    }

    public long recoveryEpoch() {
      return electionData.recoveryEpoch();
    }

    public BrokerPartitionElectionState election() {
      return electionData.election();
    }

    public BrokerPartitionView(int partitionId, MemberRole role, long term) {
      this(
          new State(partitionId, role, term, BrokerPartitionLifecycle.ABSENT),
          new Replication(0, 0, false),
          new Operation("", BrokerTopologyOperationStatus.NONE),
          new Election(-1, 0, BrokerPartitionElectionState.UNKNOWN));
    }
  }

  public record BrokerView(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Endpoint endpointData,
      @JsonUnwrapped Lease leaseData,
      @JsonUnwrapped Report reportData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String brokerId, String version) {}

    public String brokerId() {
      return identityData.brokerId();
    }

    public String version() {
      return identityData.version();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Endpoint(String host, int port, int raftPort) {}

    public String host() {
      return endpointData.host();
    }

    public int port() {
      return endpointData.port();
    }

    public int raftPort() {
      return endpointData.raftPort();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Lease(String registeredAt, String lastRenewal, long renewalTimeoutMillis) {}

    public String registeredAt() {
      return leaseData.registeredAt();
    }

    public String lastRenewal() {
      return leaseData.lastRenewal();
    }

    public long renewalTimeoutMillis() {
      return leaseData.renewalTimeoutMillis();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Report(RegistrationStatus status, List<BrokerPartitionView> partitions) {}

    public RegistrationStatus status() {
      return reportData.status();
    }

    public List<BrokerPartitionView> partitions() {
      return reportData.partitions();
    }
  }

  public record WorkerView(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Execution executionData,
      @JsonUnwrapped Polling pollingData,
      @JsonUnwrapped Registration registrationData,
      @JsonUnwrapped Status statusData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(
        String clusterId, String tenantId, String workerId, String name, String jobType) {}

    public String clusterId() {
      return identityData.clusterId();
    }

    public String tenantId() {
      return identityData.tenantId();
    }

    public String workerId() {
      return identityData.workerId();
    }

    public String name() {
      return identityData.name();
    }

    public String jobType() {
      return identityData.jobType();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Execution(int maxJobsActive, int activeJobs, int concurrency) {}

    public int maxJobsActive() {
      return executionData.maxJobsActive();
    }

    public int activeJobs() {
      return executionData.activeJobs();
    }

    public int concurrency() {
      return executionData.concurrency();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Polling(
        long timeoutMillis,
        long requestTimeoutMillis,
        long pollIntervalMillis,
        List<String> fetchVariables) {}

    public long timeoutMillis() {
      return pollingData.timeoutMillis();
    }

    public long requestTimeoutMillis() {
      return pollingData.requestTimeoutMillis();
    }

    public long pollIntervalMillis() {
      return pollingData.pollIntervalMillis();
    }

    public List<String> fetchVariables() {
      return pollingData.fetchVariables();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Registration(
        String clientVersion,
        String registeredAt,
        String lastRenewal,
        String statusChangedAt,
        long renewalTimeoutMillis,
        String workerLeaseToken,
        long leaseDeadlineEpochMillis) {
      public Registration(
          String clientVersion,
          String registeredAt,
          String lastRenewal,
          String statusChangedAt,
          long renewalTimeoutMillis) {
        this(
            clientVersion,
            registeredAt,
            lastRenewal,
            statusChangedAt,
            renewalTimeoutMillis,
            "0",
            0);
      }
    }

    public String clientVersion() {
      return registrationData.clientVersion();
    }

    public String registeredAt() {
      return registrationData.registeredAt();
    }

    public String lastRenewal() {
      return registrationData.lastRenewal();
    }

    public String statusChangedAt() {
      return registrationData.statusChangedAt();
    }

    public long renewalTimeoutMillis() {
      return registrationData.renewalTimeoutMillis();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Status(WorkerStatus status) {}

    public WorkerStatus status() {
      return statusData.status();
    }
  }

  public record WorkerRegistryResponse(
      String generatedAt, int total, int online, int offline, List<WorkerView> workers) {}

  public record JobYieldView(
      String yieldId,
      String clusterId,
      String tenantId,
      String workerId,
      String workerName,
      String jobType,
      String workerLeaseToken,
      JobYieldReason reason,
      JobYieldStatus status,
      long createdAtEpochMillis,
      long completedAtEpochMillis,
      long releasedJobCount,
      int totalPartitions,
      int acknowledgedPartitions,
      WorkerView workerSnapshot,
      List<JobYieldPlan.Partition> partitions) {}

  public record JobYieldResponse(String generatedAt, int total, List<JobYieldView> yields) {}

  public record TopologyOperationView(
      String opId, TopologyOperationType type, String targetBroker, int priority) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PartitionPlacementView(
      @JsonUnwrapped Identity identityData, @JsonUnwrapped Membership membershipData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(int partitionId, PartitionStatus status) {}

    public int partitionId() {
      return identityData.partitionId();
    }

    public PartitionStatus status() {
      return identityData.status();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Membership(
        List<String> desiredVoters,
        List<String> currentVoters,
        List<String> learners,
        String desiredPrimary,
        Map<String, Integer> priorities) {}

    public List<String> desiredVoters() {
      return membershipData.desiredVoters();
    }

    public List<String> currentVoters() {
      return membershipData.currentVoters();
    }

    public List<String> learners() {
      return membershipData.learners();
    }

    public String desiredPrimary() {
      return membershipData.desiredPrimary();
    }

    public Map<String, Integer> priorities() {
      return membershipData.priorities();
    }
  }

  public record PartitionView(
      @JsonUnwrapped Identity identityData, @JsonUnwrapped Membership membershipData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(
        int partitionId,
        String leaderId,
        long term,
        long recoveryEpoch,
        PartitionStatus status,
        boolean serviceReady) {}

    public int partitionId() {
      return identityData.partitionId();
    }

    public String leaderId() {
      return identityData.leaderId();
    }

    public long term() {
      return identityData.term();
    }

    public long recoveryEpoch() {
      return identityData.recoveryEpoch();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Membership(List<String> replicas, List<String> learners) {}

    public List<String> replicas() {
      return membershipData.replicas();
    }

    public List<String> learners() {
      return membershipData.learners();
    }
  }

  public record TopologyResponse(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Configuration configurationData,
      @JsonUnwrapped Brokers brokersData,
      @JsonUnwrapped Partitions partitionsData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String clusterId, long version, String updatedAt) {}

    public String clusterId() {
      return identityData.clusterId();
    }

    public long version() {
      return identityData.version();
    }

    public String updatedAt() {
      return identityData.updatedAt();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Configuration(int clusterSize, int partitionSize, int replicationFactor) {}

    public int clusterSize() {
      return configurationData.clusterSize();
    }

    public int partitionSize() {
      return configurationData.partitionSize();
    }

    public int replicationFactor() {
      return configurationData.replicationFactor();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Brokers(List<BrokerView> brokers) {}

    public List<BrokerView> brokers() {
      return brokersData.brokers();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Partitions(List<PartitionView> partitions) {}

    public List<PartitionView> partitions() {
      return partitionsData.partitions();
    }
  }

  public record ClusterConfigResponse(
      String clusterId,
      long clusterKey,
      int clusterSize,
      int partitionSize,
      int replicationFactor,
      List<String> voterRoster) {}

  public record BrokerClusterSummary(
      @JsonUnwrapped Configuration configurationData, @JsonUnwrapped Counts countsData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Configuration(
        String clusterId,
        long clusterKey,
        int clusterSize,
        int partitionSize,
        int replicationFactor) {}

    public String clusterId() {
      return configurationData.clusterId();
    }

    public long clusterKey() {
      return configurationData.clusterKey();
    }

    public int clusterSize() {
      return configurationData.clusterSize();
    }

    public int partitionSize() {
      return configurationData.partitionSize();
    }

    public int replicationFactor() {
      return configurationData.replicationFactor();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Counts(int registeredBrokerCount, int onlineBrokerCount) {}

    public int registeredBrokerCount() {
      return countsData.registeredBrokerCount();
    }

    public int onlineBrokerCount() {
      return countsData.onlineBrokerCount();
    }
  }

  public record BrokerClustersResponse(int count, List<BrokerClusterSummary> clusters) {}

  public record CreateClusterResponse(
      String clusterId,
      long clusterKey,
      int clusterSize,
      int partitionSize,
      int replicationFactor,
      boolean created) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record JobSubscriptionMirrorView(
      long mirrorKey,
      String sourceClusterId,
      String targetClusterId,
      JobSubscriptionMirrorStatus status,
      Boolean created) {}

  public record JobSubscriptionMirrorListResponse(List<JobSubscriptionMirrorView> mirrors) {}

  public record JobSubscriptionDistributionTargetView(
      String clusterId,
      int expectedPartitions,
      int acknowledgedPartitions,
      List<Integer> pendingPartitions,
      JobSubscriptionDistributionStatus status,
      List<Integer> confirmedPartitionIds,
      Long mirrorKey) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record JobSubscriptionDistributionView(
      long distributionKey,
      Long subscriptionKey,
      Long deletionDistributionKey,
      String sourceClusterId,
      String clusterId,
      JobSubscriptionCreationType creationType,
      String valueType,
      JobSubscriptionDistributionStatus status,
      List<JobSubscriptionDistributionTargetView> targets,
      String name,
      String jobType,
      String tenantId,
      io.taskite.protocol.impl.record.value.placementdriver.commanddistribution
              .JobSubscriptionRecord
          subscription,
      long generation,
      long lastRedistributedAt) {}

  public record JobSubscriptionDistributionListResponse(
      int total, List<JobSubscriptionDistributionView> subscriptions) {}

  public record JobSubscriptionDistributionAcceptedResponse(long distributionKey) {}

  public record TimerSubscriptionCreatedResponse(long timerSubscriptionKey, long dueDate) {}

  public record TimerSubscriptionDeletedResponse(long timerSubscriptionKey) {}

  public record TimerSubscriptionView(
      long timerSubscriptionKey,
      String clusterId,
      String tenantId,
      String name,
      io.taskite.protocol.impl.record.value.common.TimerType type,
      String expression,
      List<String> jobTypes,
      String variables,
      String timeZone,
      long dueDate,
      int repetitions,
      long occurrence) {}

  public record TimerSubscriptionListResponse(
      int total, List<TimerSubscriptionView> subscriptions) {}

  public record JobSubscriptionRedistributionAcceptedResponse(
      long distributionKey,
      long generation,
      long lastRedistributedAt,
      JobSubscriptionDistributionStatus status) {}

  public record ClusterChangeAcceptedResponse(String changeId) {}

  public record PlacementDriverTargetConfiguration(int count, List<String> voters) {}

  public record PlacementDriverActualConfiguration(
      String leaderId, long term, List<String> voters, List<String> learners) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PlacementDriverConfigurationResponse(
      long epoch,
      PlacementDriverTargetConfiguration target,
      PlacementDriverActualConfiguration actual,
      String activeChangeId) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PlacementDriverMemberChangeAcceptedResponse(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Epochs epochsData,
      @JsonUnwrapped Target targetData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String changeId, MemberChangeStatus status) {}

    public String changeId() {
      return identityData.changeId();
    }

    public MemberChangeStatus status() {
      return identityData.status();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Epochs(Long sourceEpoch, Long targetEpoch) {}

    public Long sourceEpoch() {
      return epochsData.sourceEpoch();
    }

    public Long targetEpoch() {
      return epochsData.targetEpoch();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Target(Integer targetCount, List<String> targetVoters) {}

    public Integer targetCount() {
      return targetData.targetCount();
    }

    public List<String> targetVoters() {
      return targetData.targetVoters();
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PlacementDriverMemberOperationView(
      MemberChangeOperationType type,
      String memberId,
      MemberChangeOperationStatus status,
      Long barrierIndex,
      String error) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PlacementDriverMemberChangeStatusResponse(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Epochs epochsData,
      @JsonUnwrapped Target targetData,
      @JsonUnwrapped Actual actualData,
      @JsonUnwrapped Execution executionData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String changeId, MemberChangeStatus status, String error) {}

    public String changeId() {
      return identityData.changeId();
    }

    public MemberChangeStatus status() {
      return identityData.status();
    }

    public String error() {
      return identityData.error();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Epochs(long sourceEpoch, long targetEpoch) {}

    public long sourceEpoch() {
      return epochsData.sourceEpoch();
    }

    public long targetEpoch() {
      return epochsData.targetEpoch();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Target(int targetCount, List<String> targetVoters) {}

    public int targetCount() {
      return targetData.targetCount();
    }

    public List<String> targetVoters() {
      return targetData.targetVoters();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Actual(List<String> actualVoters, List<String> actualLearners) {}

    public List<String> actualVoters() {
      return actualData.actualVoters();
    }

    public List<String> actualLearners() {
      return actualData.actualLearners();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Execution(
        PlacementDriverMemberOperationView currentOperation,
        List<PlacementDriverMemberOperationView> operations,
        List<PlacementDriverMemberOperationView> plannedChanges,
        List<PlacementDriverMemberOperationView> pendingChanges) {}

    public PlacementDriverMemberOperationView currentOperation() {
      return executionData.currentOperation();
    }

    public List<PlacementDriverMemberOperationView> operations() {
      return executionData.operations();
    }

    public List<PlacementDriverMemberOperationView> plannedChanges() {
      return executionData.plannedChanges();
    }

    public List<PlacementDriverMemberOperationView> pendingChanges() {
      return executionData.pendingChanges();
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record PendingChangeProgressView(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Progress progressData,
      @JsonUnwrapped Deadline deadlineData,
      @JsonUnwrapped Execution executionData) {
    public PendingChangeProgressView(
        final Identity identity, final Progress progress, final Deadline deadline) {
      this(
          identity,
          progress,
          deadline,
          new Execution(0, 0, "", "", "", PlanBlockedReason.NONE, null));
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(
        int partitionId, ChangeStatus status, String changeId, boolean terminal) {
      public Identity(final int partitionId, final ChangeStatus status) {
        this(partitionId, status, "", false);
      }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Execution(
        long executionGeneration,
        long authorityIndex,
        String rootChangeId,
        String supersedes,
        String supersededBy,
        PlanBlockedReason blockedReason,
        String blockedSince) {}

    public int partitionId() {
      return identityData.partitionId();
    }

    public ChangeStatus status() {
      return identityData.status();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Progress(
        int nextOperationIndex,
        int operationCount,
        TopologyOperationView currentOperation,
        List<TopologyOperationView> remainingOperations) {}

    public int nextOperationIndex() {
      return progressData.nextOperationIndex();
    }

    public int operationCount() {
      return progressData.operationCount();
    }

    public TopologyOperationView currentOperation() {
      return progressData.currentOperation();
    }

    public List<TopologyOperationView> remainingOperations() {
      return progressData.remainingOperations();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Deadline(String startedAt, String deadline) {}

    public String startedAt() {
      return deadlineData.startedAt();
    }

    public String deadline() {
      return deadlineData.deadline();
    }
  }

  public record ClusterChangeStatusResponse(
      @JsonUnwrapped Identity identityData,
      @JsonUnwrapped Topologies topologiesData,
      @JsonUnwrapped Changes changesData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(
        String changeId,
        ChangeStatus status,
        ChangeType changeType,
        boolean cancellable,
        ChangeCancellationReason cancellationReason) {}

    public String changeId() {
      return identityData.changeId();
    }

    public ChangeStatus status() {
      return identityData.status();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Topologies(
        List<PartitionPlacementView> currentTopology,
        List<PartitionPlacementView> expectedTopology) {}

    public List<PartitionPlacementView> currentTopology() {
      return topologiesData.currentTopology();
    }

    public List<PartitionPlacementView> expectedTopology() {
      return topologiesData.expectedTopology();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Changes(
        List<TopologyOperationView> plannedChanges,
        List<PendingChangeProgressView> pendingChanges,
        List<PendingChangeProgressView> executionHistory) {
      public Changes(
          final List<TopologyOperationView> plannedChanges,
          final List<PendingChangeProgressView> pendingChanges) {
        this(plannedChanges, pendingChanges, List.of());
      }
    }

    public List<TopologyOperationView> plannedChanges() {
      return changesData.plannedChanges();
    }

    public List<PendingChangeProgressView> pendingChanges() {
      return changesData.pendingChanges();
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record RecoverQuorumResponse(
      @JsonUnwrapped Identity identityData, @JsonUnwrapped Outcome outcomeData) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Identity(String clusterId, String changeId, int partitionId, String survivor) {}

    public String clusterId() {
      return identityData.clusterId();
    }

    public String changeId() {
      return identityData.changeId();
    }

    public int partitionId() {
      return identityData.partitionId();
    }

    public String survivor() {
      return identityData.survivor();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Outcome(long recoveryEpoch, ChangeStatus status) {}

    public long recoveryEpoch() {
      return outcomeData.recoveryEpoch();
    }

    public ChangeStatus status() {
      return outcomeData.status();
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record AdminOperationErrorResponse(
      String error, String message, String leaderEndpoint, List<String> pendingChangeIds) {
    public AdminOperationErrorResponse(
        final String error, final String message, final String leaderEndpoint) {
      this(error, message, leaderEndpoint, null);
    }
  }

  /**
   * Response of {@code DELETE /api/v1/broker/clusters/{clusterId}/changes/{changeId}}: outcome of
   * cancelling one change by id.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record CancelChangeResponse(String clusterId, String changeId, ChangeStatus status) {}

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record ErrorResponse(String error, String clusterId) {}
}
