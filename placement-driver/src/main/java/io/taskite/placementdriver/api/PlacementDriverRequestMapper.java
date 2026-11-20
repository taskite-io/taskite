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
package io.taskite.placementdriver.api;

import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.ClusterConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateClusterRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionMirrorRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.PlacementDriverConfigurationPatch;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RecoverQuorumRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RedistributeJobSubscriptionRequest;
import io.taskite.placementdriver.common.mapper.BrokerPartitionStateMapper;
import io.taskite.placementdriver.protocol.BrokerPartitionState;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.runtime.PlacementDriverNodeSnapshot;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRedistributeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord.PartitionStateRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.agrona.concurrent.UnsafeBuffer;

public final class PlacementDriverRequestMapper {
  private static final String CONFIGURATION_PATCH = "CONFIGURATION_PATCH";
  private static final String REBALANCE = "REBALANCE";
  private static final String QUORUM_RECOVERY = "QUORUM_RECOVERY";
  private static final String PLACEMENT_DRIVER_RESOURCE_ID = PlacementDriverCluster.CLUSTER_ID;

  private final TypedEventRegistry eventRegistry;
  private final Clock clock;

  public PlacementDriverRequestMapper(final TypedEventRegistry eventRegistry) {
    this(eventRegistry, Clock.systemUTC());
  }

  PlacementDriverRequestMapper(final TypedEventRegistry eventRegistry, final Clock clock) {
    this.eventRegistry = Objects.requireNonNull(eventRegistry, "eventRegistry");
    this.clock = Objects.requireNonNull(clock, "clock");
  }

  public LoggedEvent toPlacementDriverCommand(final PlacementDriverApiRequest<?> request) {
    Objects.requireNonNull(request, "request");
    final RecordMetadata metadata =
        new RecordMetadata()
            .requestId(UUID.randomUUID().toString())
            .recordVersion(LoggedEvent.DEFAULT_RECORD_VERSION)
            .recordType(RecordType.COMMAND)
            .valueType(request.valueType())
            .intent(request.intent());
    return LoggedEvent.ofRecord(
        request.key() > 0
            ? request.key()
            : Protocol.encodeKey(Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, 1, 0),
        metadata,
        request.value());
  }

  public LoggedEvent toPlacementDriverCommand(final byte[] requestBytes) {
    Objects.requireNonNull(requestBytes, "requestBytes");
    final ExecuteCommandRequest request = new ExecuteCommandRequest();
    request.wrap(new UnsafeBuffer(requestBytes), 0, requestBytes.length);

    final UnifiedRecordValue value =
        eventRegistry.create(request.getValueType(), request.getIntent());
    value.wrap(request.getValue(), 0, request.getValue().capacity());
    if (value instanceof final TopologyQueryRecord query
        && query.getRequestedAtEpochMillis() <= 0) {
      query.setRequestedAtEpochMillis(clock.millis());
    }
    final long key =
        request.getKey() > 0
            ? request.getKey()
            : Protocol.encodeKey(
                Protocol.PLACEMENT_DRIVER_CLUSTER_KEY, request.getPartitionId(), 0);
    final RecordMetadata metadata =
        new RecordMetadata()
            .requestId(UUID.randomUUID().toString())
            .recordVersion(LoggedEvent.DEFAULT_RECORD_VERSION)
            .recordType(RecordType.COMMAND)
            .valueType(request.getValueType())
            .intent(request.getIntent());
    return LoggedEvent.ofRecord(key, metadata, value);
  }

  public PlacementDriverApiRequest<TopologyQueryRecord> toGetClusterConfigRequest(
      final GetClusterConfigRequest request) {
    Objects.requireNonNull(request, "request");
    return toQueryRequest(QueryType.BROKER_CLUSTER_CONFIGURATION, request.getClusterId(), "");
  }

  public PlacementDriverApiRequest<TopologyQueryRecord> toGetBrokerTopologyRequest(
      final GetBrokerTopologyRequest request) {
    Objects.requireNonNull(request, "request");
    return toQueryRequest(QueryType.BROKER_TOPOLOGY, request.getClusterId(), "");
  }

  /**
   * Maps a public typed distribution request to the internal PD command-distribution state machine.
   */
  public PlacementDriverApiRequest<CommandDistributionRecord<?>> toDistributeCommandRequest(
      final DistributeCommandRequest request) {
    Objects.requireNonNull(request, "request");
    final CommandDistributionRequestRecord<?> command =
        switch (request.getCommandCase()) {
          case CREATEJOBSUBSCRIPTION ->
              new CommandDistributionRequestRecord<JobSubscriptionRecord>()
                  .setValueType(ValueType.JOB_SUBSCRIPTION)
                  .setIntent(JobSubscriptionIntent.CREATE)
                  .setValue(
                      new JobSubscriptionRecord()
                          .setClusterId(request.getClusterId())
                          .setTenantId(request.getTenantId())
                          .setName(request.getCreateJobSubscription().getName())
                          .setJobType(request.getCreateJobSubscription().getJobType())
                          .setVariables(request.getCreateJobSubscription().getVariables())
                          .setRetries(
                              request.getCreateJobSubscription().hasRetries()
                                  ? request.getCreateJobSubscription().getRetries()
                                  : 3)
                          .setPriority(
                              request.getCreateJobSubscription().hasPriority()
                                  ? request.getCreateJobSubscription().getPriority()
                                  : 50));
          case DELETEJOBSUBSCRIPTION ->
              new CommandDistributionRequestRecord<JobSubscriptionDeletionRecord>()
                  .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
                  .setIntent(JobSubscriptionIntent.DELETE)
                  .setValue(
                      new JobSubscriptionDeletionRecord()
                          .setClusterId(request.getClusterId())
                          .setTenantId(request.getTenantId())
                          .setSubscriptionKey(
                              request.getDeleteJobSubscription().getSubscriptionKey()));
          case COMMAND_NOT_SET ->
              throw new IllegalArgumentException("distribution command must be set");
        };
    return new PlacementDriverApiRequest<>(
        ValueType.COMMAND_DISTRIBUTION,
        CommandDistributionIntent.START,
        command,
        CommandDistributionRecord::new);
  }

  public PlacementDriverApiRequest<TimerSubscriptionRecord> toCreateTimerSubscriptionRequest(
      final CreateTimerSubscriptionRequest request) {
    final var timer =
        new TimerSubscriptionRecord()
            .setClusterId(request.getClusterId())
            .setTenantId(request.getTenantId())
            .setName(request.getName())
            .setType(
                switch (request.getType()) {
                  case DURATION -> TimerType.DURATION;
                  case CYCLE -> TimerType.CYCLE;
                  case CRON -> TimerType.CRON;
                  default -> TimerType.UNSPECIFIED;
                })
            .setExpression(request.getExpression())
            .setJobTypes(request.getJobTypesList())
            .setVariables(request.getVariables());
    return new PlacementDriverApiRequest<>(
        ValueType.TIMER, TimerIntent.CREATE, timer, TimerSubscriptionRecord::new);
  }

  public PlacementDriverApiRequest<CommandDistributionRecord<?>> toCreateJobSubscriptionRequest(
      final PlacementDriverHttpRequestModel.CreateJobSubscriptionRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("request body is required");
    }
    final var subscription =
        io.taskite.placementdriver.protocol.CreateJobSubscriptionRequest.newBuilder()
            .setName(request.name() == null ? "" : request.name())
            .setJobType(request.jobType() == null ? "" : request.jobType())
            .setVariables(request.variables() == null ? "" : request.variables());
    if (request.retries() != null) {
      subscription.setRetries(request.retries());
    }
    if (request.priority() != null) {
      subscription.setPriority(request.priority());
    }
    return toDistributeCommandRequest(
        DistributeCommandRequest.newBuilder()
            .setClusterId(request.clusterId() == null ? "" : request.clusterId())
            .setTenantId(request.tenantId() == null ? "" : request.tenantId())
            .setCreateJobSubscription(subscription)
            .build());
  }

  public PlacementDriverApiRequest<TimerSubscriptionRecord> toCreateTimerSubscriptionRequest(
      final PlacementDriverHttpRequestModel.CreateTimerSubscriptionRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("request body is required");
    }
    if (request.jobTypes() != null && request.jobTypes().stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("jobTypes must contain nonblank strings");
    }
    return toCreateTimerSubscriptionRequest(
        CreateTimerSubscriptionRequest.newBuilder()
            .setClusterId(request.clusterId() == null ? "" : request.clusterId())
            .setTenantId(request.tenantId() == null ? "" : request.tenantId())
            .setName(request.name() == null ? "" : request.name())
            .setType(
                request.type() == null
                    ? io.taskite.placementdriver.protocol.TimerSubscriptionType
                        .TIMER_TYPE_UNSPECIFIED
                    : io.taskite.placementdriver.protocol.TimerSubscriptionType.valueOf(
                        request.type()))
            .setExpression(request.expression() == null ? "" : request.expression())
            .addAllJobTypes(request.jobTypes() == null ? List.of() : request.jobTypes())
            .setVariables(
                request.variables() == null || request.variables().isBlank()
                    ? "{}"
                    : request.variables())
            .build());
  }

  public PlacementDriverApiRequest<TimerSubscriptionRecord> toDeleteTimerSubscriptionRequest(
      final DeleteTimerSubscriptionRequest request) {
    if (request.getTimerSubscriptionKey() <= 0) {
      throw new IllegalArgumentException("timerSubscriptionKey must be positive");
    }
    return new PlacementDriverApiRequest<>(
        request.getTimerSubscriptionKey(),
        ValueType.TIMER,
        TimerIntent.CANCEL,
        new TimerSubscriptionRecord()
            .setClusterId(request.getClusterId())
            .setTenantId(request.getTenantId()),
        TimerSubscriptionRecord::new);
  }

  public PlacementDriverApiRequest<CommandDistributionRecord<?>> toDeleteJobSubscriptionRequest(
      final String subscriptionKey, final String clusterId, final String tenantId) {
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId is required");
    }
    return toDistributeCommandRequest(
        DistributeCommandRequest.newBuilder()
            .setClusterId(clusterId == null ? "" : clusterId)
            .setTenantId(tenantId == null ? "" : tenantId)
            .setDeleteJobSubscription(
                io.taskite.placementdriver.protocol.DeleteJobSubscriptionRequest.newBuilder()
                    .setSubscriptionKey(positiveLong(subscriptionKey, "subscriptionKey")))
            .build());
  }

  public PlacementDriverApiRequest<TimerSubscriptionRecord> toDeleteTimerSubscriptionRequest(
      final String timerSubscriptionKey, final String clusterId, final String tenantId) {
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId is required");
    }
    return toDeleteTimerSubscriptionRequest(
        DeleteTimerSubscriptionRequest.newBuilder()
            .setTimerSubscriptionKey(positiveLong(timerSubscriptionKey, "timerSubscriptionKey"))
            .setClusterId(clusterId)
            .setTenantId(tenantId == null ? "" : tenantId)
            .build());
  }

  public RecoverQuorumRequest normalizeRecoverQuorumRequest(final RecoverQuorumRequest request) {
    return request == null ? new RecoverQuorumRequest(null, null, null) : request;
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toCreateClusterRequest(
      final CreateClusterRequest request) {
    final ClusterConfigRecord command =
        new ClusterConfigRecord()
            .setClusterId(request == null ? "" : request.clusterId())
            .setClusterSize(
                request == null || request.clusterSize() == null ? 0 : request.clusterSize())
            .setPartitionSize(
                request == null || request.partitionSize() == null ? 0 : request.partitionSize())
            .setReplicationFactor(
                request == null || request.replicationFactor() == null
                    ? 0
                    : request.replicationFactor())
            .setRosterManaged(true);
    return configRequest(TopologyIntent.CREATE_CLUSTER, command);
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toDeleteClusterRequest(
      final String clusterId) {
    return configRequest(
        TopologyIntent.DELETE_CLUSTER, new ClusterConfigRecord().setClusterId(clusterId));
  }

  public PlacementDriverApiRequest<JobSubscriptionMirrorRecord>
      toCreateJobSubscriptionMirrorRequest(final CreateJobSubscriptionMirrorRequest request) {
    final JobSubscriptionMirrorRecord command =
        new JobSubscriptionMirrorRecord()
            .setSourceClusterId(request == null ? "" : request.sourceClusterId())
            .setTargetClusterId(request == null ? "" : request.targetClusterId())
            .setCreatedAtEpochMillis(clock.millis());
    return new PlacementDriverApiRequest<>(
        ValueType.JOB_SUBSCRIPTION_MIRROR,
        JobSubscriptionMirrorIntent.CREATE,
        command,
        JobSubscriptionMirrorRecord::new);
  }

  public PlacementDriverApiRequest<JobSubscriptionMirrorRecord>
      toDeleteJobSubscriptionMirrorRequest(final String mirrorKey) {
    return new PlacementDriverApiRequest<>(
        ValueType.JOB_SUBSCRIPTION_MIRROR,
        JobSubscriptionMirrorIntent.DELETE,
        new JobSubscriptionMirrorRecord().setMirrorKey(positiveLong(mirrorKey, "mirrorKey")),
        JobSubscriptionMirrorRecord::new);
  }

  public PlacementDriverApiRequest<JobSubscriptionMirrorQueryRecord>
      toJobSubscriptionMirrorQueryRequest(
          final String mirrorKey, final String sourceClusterId, final String targetClusterId) {
    return new PlacementDriverApiRequest<>(
        ValueType.JOB_SUBSCRIPTION_MIRROR,
        JobSubscriptionMirrorIntent.QUERY,
        new JobSubscriptionMirrorQueryRecord()
            .setMirrorKey(
                mirrorKey == null || mirrorKey.isBlank() ? 0 : positiveLong(mirrorKey, "mirrorKey"))
            .setSourceClusterId(sourceClusterId)
            .setTargetClusterId(targetClusterId),
        JobSubscriptionMirrorQueryRecord::new);
  }

  public PlacementDriverApiRequest<CommandDistributionQueryRecord>
      toJobSubscriptionDistributionQueryRequest(final String distributionKey) {
    return new PlacementDriverApiRequest<>(
        ValueType.COMMAND_DISTRIBUTION,
        CommandDistributionIntent.QUERY,
        new CommandDistributionQueryRecord()
            .setDistributionKey(positiveLong(distributionKey, "distributionKey")),
        CommandDistributionQueryRecord::new);
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toPatchClusterRequest(
      final String clusterId, final ClusterConfigurationPatch patch) {
    final ClusterConfigRecord command = adminCommand(clusterId, CONFIGURATION_PATCH);
    if (patch != null && patch.brokers() != null) {
      command
          .setRequestedBrokerAdditions(
              patch.brokers().add() == null ? List.of() : patch.brokers().add())
          .setRequestedBrokerRemovals(
              patch.brokers().remove() == null ? List.of() : patch.brokers().remove())
          .setRequestedBrokerCount(patch.brokers().count());
    }
    if (patch != null && patch.partitions() != null) {
      command
          .setRequestedPartitionCount(patch.partitions().count())
          .setRequestedReplicationFactor(patch.partitions().replicationFactor());
    }
    return configRequest(TopologyIntent.CONFIGURE_CLUSTER, command);
  }

  public PlacementDriverApiRequest<CommandDistributionListRecord>
      toJobSubscriptionDistributionListRequest(
          final String jobType, final String distributionKey, final String clusterId) {
    final var query =
        new CommandDistributionListRecord()
            .setJobType(jobType)
            .setDistributionKey(
                distributionKey == null || distributionKey.isBlank()
                    ? 0
                    : positiveLong(distributionKey, "distributionKey"))
            .setClusterId(clusterId);
    return new PlacementDriverApiRequest<>(
        ValueType.COMMAND_DISTRIBUTION,
        CommandDistributionIntent.LIST,
        query,
        CommandDistributionListRecord::new);
  }

  public PlacementDriverApiRequest<CommandDistributionRecord<?>>
      toRedistributeJobSubscriptionRequest(
          final String distributionKey, final RedistributeJobSubscriptionRequest request) {
    if (request == null
        || request.expectedGeneration() == null
        || !request.expectedGeneration().matches("[0-9]+")) {
      throw new IllegalArgumentException(
          "expectedGeneration must be a non-negative int64 decimal string");
    }
    final long generation;
    try {
      generation = Long.parseLong(request.expectedGeneration());
    } catch (final NumberFormatException error) {
      throw new IllegalArgumentException("expectedGeneration exceeds int64 range", error);
    }
    return new PlacementDriverApiRequest<>(
        ValueType.COMMAND_DISTRIBUTION,
        CommandDistributionIntent.REDISTRIBUTE,
        new CommandDistributionRedistributeRecord()
            .setDistributionKey(positiveLong(distributionKey, "distributionKey"))
            .setExpectedGeneration(generation)
            .setRequestedAtEpochMillis(clock.millis()),
        CommandDistributionRecord::new);
  }

  public PlacementDriverApiRequest<MemberChangeRecord> toBrokerMemberChangeRequest(
      final String clusterId, final ClusterConfigurationPatch patch) {
    final var brokers = patch == null ? null : patch.brokers();
    final MemberChangeRecord command =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.BROKER)
            .setResourceId(clusterId)
            .setChangeId(UUID.randomUUID().toString())
            .setRequestedAdditions(
                brokers == null || brokers.add() == null ? List.of() : brokers.add())
            .setRequestedRemovals(
                brokers == null || brokers.remove() == null ? List.of() : brokers.remove())
            .setRequestedCount(brokers == null ? null : brokers.count())
            .setRequestedAtEpochMillis(clock.millis());
    return new PlacementDriverApiRequest<>(
        ValueType.MEMBER_CHANGE, MemberChangeIntent.REQUEST, command, MemberChangeRecord::new);
  }

  public PlacementDriverApiRequest<MemberChangeRecord> toPlacementDriverMemberChangeRequest(
      final PlacementDriverConfigurationPatch patch) {
    final MemberChangeRecord command =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.PLACEMENT_DRIVER)
            .setResourceId(PLACEMENT_DRIVER_RESOURCE_ID)
            .setChangeId(UUID.randomUUID().toString())
            .setRequestedAdditions(patch == null || patch.add() == null ? List.of() : patch.add())
            .setRequestedRemovals(
                patch == null || patch.remove() == null ? List.of() : patch.remove())
            .setRequestedCount(patch == null ? null : patch.count())
            .setRequestedAtEpochMillis(clock.millis());
    return new PlacementDriverApiRequest<>(
        ValueType.MEMBER_CHANGE, MemberChangeIntent.REQUEST, command, MemberChangeRecord::new);
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toRebalanceClusterRequest(
      final String clusterId) {
    return configRequest(TopologyIntent.CONFIGURE_CLUSTER, adminCommand(clusterId, REBALANCE));
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toRecoverQuorumRequest(
      final String clusterId, final String partitionId, final RecoverQuorumRequest request) {
    final RecoverQuorumRequest normalized = normalizeRecoverQuorumRequest(request);
    final Integer parsedPartitionId;
    try {
      parsedPartitionId = Integer.valueOf(partitionId);
    } catch (final NumberFormatException error) {
      throw new IllegalArgumentException("INVALID_PARTITION_ID", error);
    }
    return configRequest(
        TopologyIntent.CONFIGURE_CLUSTER,
        adminCommand(clusterId, QUORUM_RECOVERY)
            .setRequestedPartitionId(parsedPartitionId)
            .setConfirmation(normalized.confirm())
            .setFencingProof(normalized.fencingProof())
            .setSurvivorBrokerId(normalized.survivor()));
  }

  public PlacementDriverApiRequest<ClusterConfigRecord> toCancelClusterChangeRequest(
      final String clusterId, final String changeId) {
    return configRequest(
        TopologyIntent.CANCEL_CLUSTER_CHANGE,
        new ClusterConfigRecord().setClusterId(clusterId).setChangeId(changeId));
  }

  public PlacementDriverApiRequest<TopologyQueryRecord> toQueryRequest(
      final QueryType queryType, final String clusterId, final String changeId) {
    return toQueryRequest(queryType, clusterId, changeId, "");
  }

  public PlacementDriverApiRequest<TopologyQueryRecord> toPlacementDriverQueryRequest(
      final QueryType queryType, final PlacementDriverNodeSnapshot snapshot) {
    final TopologyQueryRecord query =
        queryRecord(queryType, "", "")
            .setRequestingMemberId(snapshot.memberId())
            .setRequestingHost(snapshot.host())
            .setRequestingHttpPort(snapshot.httpPort())
            .setRequestingGrpcPort(snapshot.grpcPort())
            .setRequestingCommitIndex(snapshot.commitIndex())
            .setRequestingLastApplied(snapshot.lastApplied())
            .setRequestingLastLogIndex(snapshot.lastLogIndex());
    return queryRequest(query);
  }

  public PlacementDriverApiRequest<BrokerRegistrationRecord> toRegisterBrokerRequest(
      final RegisterBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return brokerRegistrationRequest(
        BrokerIntent.REGISTER,
        new BrokerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setBrokerId(request.getBrokerId())
            .setHost(request.getHost())
            .setPort(request.getPort())
            .setRaftPort(request.getRaftPort())
            .setVersion(request.getVersion())
            .setIncarnationId(request.getIncarnationId())
            .setRenewalTimeoutMillis(request.getRenewalTimeoutMillis())
            .setPartitions(partitions(request.getPartitionsList())));
  }

  public PlacementDriverApiRequest<BrokerRegistrationRecord> toRenewBrokerRequest(
      final RenewBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return brokerRegistrationRequest(
        BrokerIntent.RENEW,
        new BrokerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setBrokerId(request.getBrokerId())
            .setIncarnationId(request.getIncarnationId())
            .setPreservePartitionObservations(request.getPreservePartitionObservations())
            .setPartitions(partitions(request.getPartitionsList())));
  }

  public PlacementDriverApiRequest<BrokerRegistrationRecord> toDeregisterBrokerRequest(
      final DeregisterBrokerRequest request) {
    Objects.requireNonNull(request, "request");
    return brokerRegistrationRequest(
        BrokerIntent.DEREGISTER,
        new BrokerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setBrokerId(request.getBrokerId())
            .setIncarnationId(request.getIncarnationId()));
  }

  public PlacementDriverApiRequest<WorkerRegistrationRecord> toRegisterWorkerRequest(
      final RegisterWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return workerRegistrationRequest(
        WorkerIntent.REGISTER,
        new WorkerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setTenantIds(request.getTenantIdsList())
            .setWorkerId(request.getWorkerId())
            .setName(request.getName())
            .setJobType(request.getJobType())
            .setMaxJobsActive(request.getMaxJobsActive())
            .setActiveJobs(request.getActiveJobs())
            .setConcurrency(request.getConcurrency())
            .setTimeoutMillis(request.getTimeoutMillis())
            .setRequestTimeoutMillis(request.getRequestTimeoutMillis())
            .setPollIntervalMillis(request.getPollIntervalMillis())
            .setFetchVariables(request.getFetchVariablesList())
            .setClientVersion(request.getClientVersion())
            .setRenewalTimeoutMillis(request.getRenewalTimeoutMillis()));
  }

  public PlacementDriverApiRequest<WorkerRegistrationRecord> toRenewWorkerRequest(
      final RenewWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return workerRegistrationRequest(
        WorkerIntent.RENEW,
        new WorkerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setTenantIds(request.getTenantIdsList())
            .setWorkerId(request.getWorkerId())
            .setWorkerLeaseToken(request.getWorkerLeaseToken())
            .setName(request.getName())
            .setJobType(request.getJobType())
            .setMaxJobsActive(request.getMaxJobsActive())
            .setActiveJobs(request.getActiveJobs())
            .setConcurrency(request.getConcurrency())
            .setTimeoutMillis(request.getTimeoutMillis())
            .setRequestTimeoutMillis(request.getRequestTimeoutMillis())
            .setPollIntervalMillis(request.getPollIntervalMillis())
            .setFetchVariables(request.getFetchVariablesList())
            .setClientVersion(request.getClientVersion())
            .setRenewalTimeoutMillis(request.getRenewalTimeoutMillis()));
  }

  public PlacementDriverApiRequest<WorkerRegistrationRecord> toDeregisterWorkerRequest(
      final DeregisterWorkerRequest request) {
    Objects.requireNonNull(request, "request");
    return workerRegistrationRequest(
        WorkerIntent.DEREGISTER,
        new WorkerRegistrationRecord()
            .setClusterId(request.getClusterId())
            .setTenantIds(request.getTenantIdsList())
            .setWorkerId(request.getWorkerId())
            .setWorkerLeaseToken(request.getWorkerLeaseToken())
            .setName(request.getName()));
  }

  private PlacementDriverApiRequest<TopologyQueryRecord> toQueryRequest(
      final QueryType queryType,
      final String clusterId,
      final String changeId,
      final String requestingMemberId) {
    return queryRequest(
        queryRecord(queryType, clusterId, changeId).setRequestingMemberId(requestingMemberId));
  }

  private TopologyQueryRecord queryRecord(
      final QueryType queryType, final String clusterId, final String changeId) {
    return new TopologyQueryRecord()
        .setQueryType(queryType)
        .setClusterId(clusterId)
        .setChangeId(changeId)
        .setRequestedAtEpochMillis(clock.millis());
  }

  private static PlacementDriverApiRequest<TopologyQueryRecord> queryRequest(
      final TopologyQueryRecord query) {
    return new PlacementDriverApiRequest<>(
        ValueType.TOPOLOGY, TopologyIntent.QUERY, query, TopologyQueryRecord::new);
  }

  private ClusterConfigRecord adminCommand(final String clusterId, final String operation) {
    return new ClusterConfigRecord()
        .setClusterId(clusterId)
        .setAdminOperation(operation)
        .setChangeId(UUID.randomUUID().toString())
        .setRequestedAtEpochMillis(clock.millis())
        .setRosterManaged(true);
  }

  private static PlacementDriverApiRequest<ClusterConfigRecord> configRequest(
      final TopologyIntent intent, final ClusterConfigRecord command) {
    return new PlacementDriverApiRequest<>(
        ValueType.TOPOLOGY, intent, command, ClusterConfigRecord::new);
  }

  private static PlacementDriverApiRequest<BrokerRegistrationRecord> brokerRegistrationRequest(
      final BrokerIntent intent, final BrokerRegistrationRecord command) {
    return new PlacementDriverApiRequest<>(
        ValueType.BROKER_REGISTRATION, intent, command, BrokerRegistrationRecord::new);
  }

  private static PlacementDriverApiRequest<WorkerRegistrationRecord> workerRegistrationRequest(
      final WorkerIntent intent, final WorkerRegistrationRecord command) {
    return new PlacementDriverApiRequest<>(
        ValueType.WORKER_REGISTRATION, intent, command, WorkerRegistrationRecord::new);
  }

  private static List<PartitionStateRecord> partitions(
      final List<BrokerPartitionState> partitions) {
    return partitions.stream()
        .map(
            partition ->
                new PartitionStateRecord()
                    .setPartitionId(partition.getPartitionId())
                    .setRole(MemberRole.parse(partition.getRole()))
                    .setTerm(partition.getTerm())
                    .setLifecycleState(
                        BrokerPartitionLifecycle.parse(partition.getLifecycleState()))
                    .setMatchIndex(partition.getMatchIndex())
                    .setLag(partition.getLag())
                    .setReadyForPromotion(partition.getReadyForPromotion())
                    .setPriority(partition.getPriority())
                    .setLastOpId(partition.getLastOperationId())
                    .setLastOpStatus(
                        BrokerTopologyOperationStatus.parse(partition.getLastOperationStatus()))
                    .setRecoveryEpoch(partition.getRecoveryEpoch())
                    .setElection(BrokerPartitionStateMapper.fromProto(partition.getElection())))
        .toList();
  }

  private static long positiveLong(final String value, final String field) {
    try {
      final long parsed = Long.parseLong(value);
      if (parsed <= 0) {
        throw new IllegalArgumentException(field + " must be positive");
      }
      return parsed;
    } catch (final NumberFormatException error) {
      throw new IllegalArgumentException(field + " must be a positive integer", error);
    }
  }
}
