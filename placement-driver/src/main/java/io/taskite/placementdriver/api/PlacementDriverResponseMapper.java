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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.taskite.placementdriver.api.model.PlacementDriverHttpModel;
import io.taskite.placementdriver.common.mapper.BrokerPartitionStateMapper;
import io.taskite.placementdriver.common.mapper.PlacementDriverTopologyMapper;
import io.taskite.placementdriver.protocol.Broker;
import io.taskite.placementdriver.protocol.BrokerPartitionState;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.DeregisterWorkerResponse;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.Partition;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.RenewWorkerResponse;
import io.taskite.placementdriver.runtime.PlacementDriverNotLeaderException;
import io.taskite.placementdriver.runtime.PlacementDriverRejectionException;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionListRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOutcome;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeResponseRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClusterChangeStatusRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerClustersRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.ExecuteCommandResponseEncoder;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorCreationOutcome;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.MemberRole;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.agrona.concurrent.UnsafeBuffer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;

public final class PlacementDriverResponseMapper {
  private static final long HTTP_TIMEOUT_MILLIS = 12_000L;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public DeferredResult<ResponseEntity<Object>> toHttpResponse(
      final ActorFuture<PlacementDriverEndpointResult> future) {
    final DeferredResult<ResponseEntity<Object>> deferred =
        new DeferredResult<>(HTTP_TIMEOUT_MILLIS);
    deferred.onTimeout(
        () ->
            deferred.setErrorResult(
                ResponseEntity.status(503)
                    .body(
                        new PlacementDriverHttpModel.AdminOperationErrorResponse(
                            "TIMEOUT", "Admin request timed out before completion", null))));
    future.onComplete(
        (result, error) -> {
          if (error != null) {
            deferred.setErrorResult(error);
          } else {
            deferred.setResult(ResponseEntity.status(result.status()).body(result.body()));
          }
        });
    return deferred;
  }

  public byte[] toCommandResponse(final LoggedEvent record) {
    final ExecuteCommandResponse response =
        new ExecuteCommandResponse()
            .setPartitionId(ExecuteCommandResponseEncoder.partitionIdNullValue())
            .setKey(record.key())
            .setRecordType(record.metadata().getRecordType())
            .setValueType(record.metadata().getValueType())
            .setIntent(record.metadata().getIntent())
            .setRejectionType(record.metadata().getRejectionType());

    final UnsafeBuffer value = new UnsafeBuffer(new byte[record.value().getLength()]);
    record.value().write(value, 0);
    response.setValue(value, 0, value.capacity());
    final byte[] rejectionReason =
        record.metadata().getRejectionReason() == null
            ? new byte[0]
            : record.metadata().getRejectionReason().getBytes(StandardCharsets.UTF_8);
    response.setRejectionReason(new UnsafeBuffer(rejectionReason), 0, rejectionReason.length);

    final UnsafeBuffer encoded = new UnsafeBuffer(new byte[response.getLength()]);
    response.write(encoded, 0);
    return Arrays.copyOf(encoded.byteArray(), response.getLength());
  }

  public RegisterBrokerResponse toRegisterBrokerResponse(
      final PlacementDriverApiResponse<BrokerRegistrationRecord> response) {
    final BrokerRegistrationRecord record = response.value();
    return RegisterBrokerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .setBrokerId(record.getBrokerId())
        .build();
  }

  public RenewBrokerResponse toRenewBrokerResponse(
      final PlacementDriverApiResponse<BrokerRegistrationRecord> response) {
    final BrokerRegistrationRecord record = response.value();
    return RenewBrokerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .setBrokerId(record.getBrokerId())
        .build();
  }

  public DeregisterBrokerResponse toDeregisterBrokerResponse(
      final PlacementDriverApiResponse<BrokerRegistrationRecord> response) {
    final BrokerRegistrationRecord record = response.value();
    return DeregisterBrokerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .setBrokerId(record.getBrokerId())
        .build();
  }

  public RegisterWorkerResponse toRegisterWorkerResponse(
      final PlacementDriverApiResponse<WorkerRegistrationRecord> response) {
    final WorkerRegistrationRecord record = response.value();
    return RegisterWorkerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .addAllTenantIds(record.getTenantIds())
        .setWorkerId(record.getWorkerId())
        .setWorkerLeaseToken(record.getWorkerLeaseToken())
        .setLeaseDeadlineEpochMillis(record.getLeaseDeadlineEpochMillis())
        .setName(record.getName())
        .build();
  }

  public RenewWorkerResponse toRenewWorkerResponse(
      final PlacementDriverApiResponse<WorkerRegistrationRecord> response) {
    final WorkerRegistrationRecord record = response.value();
    return RenewWorkerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .addAllTenantIds(record.getTenantIds())
        .setWorkerId(record.getWorkerId())
        .setWorkerLeaseToken(record.getWorkerLeaseToken())
        .setLeaseDeadlineEpochMillis(record.getLeaseDeadlineEpochMillis())
        .setName(record.getName())
        .build();
  }

  public DeregisterWorkerResponse toDeregisterWorkerResponse(
      final PlacementDriverApiResponse<WorkerRegistrationRecord> response) {
    final WorkerRegistrationRecord record = response.value();
    return DeregisterWorkerResponse.newBuilder()
        .setClusterId(record.getClusterId())
        .addAllTenantIds(record.getTenantIds())
        .setWorkerId(record.getWorkerId())
        .setName(record.getName())
        .build();
  }

  public GetClusterConfigResponse toGetClusterConfigResponse(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final ClusterConfigRecord config =
        readResponseValue(response.value(), ClusterConfigRecord.class);
    return GetClusterConfigResponse.newBuilder()
        .setClusterId(config.getClusterId())
        .setClusterKey(config.getClusterKey())
        .setClusterSize(config.getClusterSize())
        .setPartitionSize(config.getPartitionSize())
        .setReplicationFactor(config.getReplicationFactor())
        .addAllRemovedBrokers(config.getRemovedBrokers())
        .build();
  }

  public BrokerTopology toBrokerTopology(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    return toBrokerTopology(readResponseValue(response.value(), TopologyRecord.class));
  }

  public BrokerTopology toBrokerTopology(final TopologyRecord topology) {
    final BrokerTopology.Builder result =
        BrokerTopology.newBuilder()
            .setClusterId(topology.getClusterId())
            .setVersion(topology.getVersion())
            .setClusterSize(topology.getClusterSize())
            .setPartitionSize(topology.getPartitionSize())
            .setReplicationFactor(topology.getReplicationFactor())
            .setUpdatedAtEpochMillis(topology.getUpdatedAtEpochMillis());
    topology.getBrokers().forEach(broker -> result.addBrokers(toBroker(broker)));
    topology.getPartitions().forEach(partition -> result.addPartitions(toPartition(partition)));
    return result.build();
  }

  public BrokerTopology toBrokerTopology(final ClusterTopology topology) {
    final BrokerTopology.Builder result =
        BrokerTopology.newBuilder()
            .setClusterId(topology.clusterId())
            .setVersion(topology.version())
            .setClusterSize(topology.brokers().size())
            .setPartitionSize(topology.partitionSize())
            .setReplicationFactor(topology.replicationFactor())
            .setUpdatedAtEpochMillis(topology.updatedAt().toEpochMilli());
    topology
        .brokers()
        .values()
        .forEach(
            broker ->
                result.addBrokers(
                    Broker.newBuilder()
                        .setBrokerId(broker.brokerId())
                        .setHost(broker.host())
                        .setPort(broker.port())
                        .setRaftPort(broker.raftPort())));
    topology
        .partitions()
        .forEach(
            partition ->
                result.addPartitions(
                    Partition.newBuilder()
                        .setPartitionId(partition.partitionId())
                        .setLeaderId(partition.leaderId())
                        .setServiceReady(partition.serviceReady())
                        .addAllReplicas(partition.replicas())
                        .addAllLearners(partition.learners())
                        .setTerm(partition.term())
                        .setStatus(partition.status().name())
                        .putAllPriorities(partition.priorities())
                        .setRecoveryEpoch(partition.recoveryEpoch())));
    return result.build();
  }

  public PlacementDriverTopology toPlacementDriverTopology(final MemberTopologyRecord topology) {
    return PlacementDriverTopologyMapper.toProto(topology);
  }

  /** Returns the stable distribution key after PD accepts the typed distribution request. */
  public DistributeCommandResponse toDistributeCommandResponse(
      final PlacementDriverApiResponse<CommandDistributionRecord<?>> response) {
    final long distributionKey =
        response.value().getDistributionKey() > 0
            ? response.value().getDistributionKey()
            : response.key();
    return DistributeCommandResponse.newBuilder().setDistributionKey(distributionKey).build();
  }

  public CreateTimerSubscriptionResponse toCreateTimerSubscriptionResponse(
      final PlacementDriverApiResponse<TimerSubscriptionRecord> response) {
    return CreateTimerSubscriptionResponse.newBuilder()
        .setTimerSubscriptionKey(response.key())
        .setDueDate(response.value().getDueDate())
        .build();
  }

  public PlacementDriverEndpointResult toCreateJobSubscriptionResult(
      final PlacementDriverApiResponse<CommandDistributionRecord<?>> response) {
    return new PlacementDriverEndpointResult(
        202,
        new PlacementDriverHttpModel.JobSubscriptionDistributionAcceptedResponse(
            toDistributeCommandResponse(response).getDistributionKey()));
  }

  public PlacementDriverEndpointResult toCreateTimerSubscriptionResult(
      final PlacementDriverApiResponse<TimerSubscriptionRecord> response) {
    return new PlacementDriverEndpointResult(
        201,
        new PlacementDriverHttpModel.TimerSubscriptionCreatedResponse(
            response.key(), response.value().getDueDate()));
  }

  public PlacementDriverEndpointResult toDeleteJobSubscriptionResult(
      final PlacementDriverApiResponse<CommandDistributionRecord<?>> response) {
    return new PlacementDriverEndpointResult(
        202,
        new PlacementDriverHttpModel.JobSubscriptionDistributionAcceptedResponse(
            toDistributeCommandResponse(response).getDistributionKey()));
  }

  public PlacementDriverEndpointResult toDeleteTimerSubscriptionResult(
      final PlacementDriverApiResponse<TimerSubscriptionRecord> response) {
    return new PlacementDriverEndpointResult(
        200, new PlacementDriverHttpModel.TimerSubscriptionDeletedResponse(response.key()));
  }

  public PlacementDriverEndpointResult toTimerSubscriptionsResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final var timers = readResponseValue(response.value(), TimerSubscriptionListRecord.class);
    final var subscriptions =
        timers.getSubscriptions().entrySet().stream()
            .sorted(java.util.Map.Entry.comparingByKey())
            .map(
                entry -> {
                  final var timer = entry.getValue();
                  return new PlacementDriverHttpModel.TimerSubscriptionView(
                      entry.getKey(),
                      timer.getClusterId(),
                      timer.getTenantId(),
                      timer.getName(),
                      timer.getType(),
                      timer.getExpression(),
                      timer.getJobTypes(),
                      timer.getVariables(),
                      timer.getTimeZone(),
                      timer.getDueDate(),
                      timer.getRepetitions(),
                      timer.getOccurrence());
                })
            .toList();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.TimerSubscriptionListResponse(
            subscriptions.size(), subscriptions));
  }

  public DeleteTimerSubscriptionResponse toDeleteTimerSubscriptionResponse(
      final PlacementDriverApiResponse<TimerSubscriptionRecord> response) {
    return DeleteTimerSubscriptionResponse.newBuilder()
        .setTimerSubscriptionKey(response.key())
        .build();
  }

  public Status toGrpcStatus(final Throwable error) {
    final Throwable cause = unwrap(error);
    if (isBackpressure(cause)) {
      final Status status = Status.fromThrowable(cause);
      return Status.RESOURCE_EXHAUSTED
          .withDescription(
              status.getCode() == Status.Code.RESOURCE_EXHAUSTED
                  ? status.getDescription()
                  : cause.getMessage())
          .withCause(cause);
    }
    if (cause instanceof StatusRuntimeException || cause instanceof StatusException) {
      return Status.fromThrowable(cause);
    }
    if (cause instanceof PlacementDriverNotLeaderException notLeader) {
      return Status.FAILED_PRECONDITION.withDescription(
          "NOT_LEADER:" + (notLeader.leaderEndpoint() == null ? "" : notLeader.leaderEndpoint()));
    }
    if (cause instanceof PlacementDriverRejectionException rejection) {
      final Status status =
          switch (rejection.type()) {
            case INVALID_ARGUMENT -> Status.INVALID_ARGUMENT;
            case NOT_FOUND -> Status.NOT_FOUND;
            case ALREADY_EXISTS -> Status.ALREADY_EXISTS;
            case INVALID_STATE, STALE_WORKER_SESSION, FEATURE_DISABLED ->
                Status.FAILED_PRECONDITION;
            case EXCEEDED_BATCH_RECORD_SIZE -> Status.RESOURCE_EXHAUSTED;
            case WORKER_SESSION_NOT_READY -> Status.UNAVAILABLE;
            case PROCESSING_ERROR, NULL_VAL, SBE_UNKNOWN -> Status.INTERNAL;
          };
      return status.withDescription(rejection.getMessage()).withCause(cause);
    }
    if (cause instanceof IllegalArgumentException) {
      return Status.INVALID_ARGUMENT.withDescription(messageOf(cause)).withCause(cause);
    }
    return Status.UNAVAILABLE.withDescription(messageOf(cause)).withCause(cause);
  }

  private static Broker toBroker(final TopologyRecord.BrokerEndpointRecord broker) {
    final Broker.Builder result =
        Broker.newBuilder()
            .setBrokerId(broker.getBrokerId())
            .setHost(broker.getHost())
            .setPort(broker.getPort())
            .setRaftPort(broker.getRaftPort())
            .setVersion(broker.getVersion())
            .setIncarnationId(broker.getIncarnationId())
            .setRegisteredAtEpochMillis(broker.getRegisteredAtEpochMillis())
            .setLastRenewalEpochMillis(broker.getLastRenewalEpochMillis())
            .setRenewalTimeoutMillis(broker.getRenewalTimeoutMillis())
            .setStatus(broker.getStatus().name());
    broker
        .getPartitions()
        .forEach(
            partition ->
                result.addPartitions(
                    BrokerPartitionState.newBuilder()
                        .setPartitionId(partition.getPartitionId())
                        .setRole(partition.getRole().name())
                        .setTerm(partition.getTerm())
                        .setLifecycleState(partition.getLifecycleState().name())
                        .setMatchIndex(partition.getMatchIndex())
                        .setLag(partition.getLag())
                        .setReadyForPromotion(partition.isReadyForPromotion())
                        .setPriority(partition.getPriority())
                        .setLastOperationId(partition.getLastOpId())
                        .setLastOperationStatus(partition.getLastOpStatus().name())
                        .setRecoveryEpoch(partition.getRecoveryEpoch())
                        .setElection(BrokerPartitionStateMapper.toProto(partition.getElection()))));
    return result.build();
  }

  private static Partition toPartition(final TopologyRecord.PartitionRecord partition) {
    return Partition.newBuilder()
        .setPartitionId(partition.getPartitionId())
        .setLeaderId(partition.getLeaderId())
        .setServiceReady(partition.isServiceReady())
        .addAllReplicas(partition.getReplicas())
        .addAllLearners(partition.getLearners())
        .setTerm(partition.getTerm())
        .setStatus(partition.getStatus() == null ? "" : partition.getStatus().name())
        .putAllPriorities(partition.getPriorities())
        .setRecoveryEpoch(partition.getRecoveryEpoch())
        .build();
  }

  public PlacementDriverEndpointResult toHealthResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final TopologyQueryRecord query = response.value();
    return toHealthResult(
        readResponseValue(query, MemberTopologyRecord.class),
        query.getRequestingHost(),
        query.getRequestingHttpPort(),
        query.getRequestingGrpcPort(),
        query.getRequestingCommitIndex(),
        query.getRequestingLastApplied(),
        query.getRequestingLastLogIndex());
  }

  private PlacementDriverEndpointResult toHealthResult(
      final MemberTopologyRecord topology,
      final String host,
      final int httpPort,
      final int grpcPort,
      final long commitIndex,
      final long lastApplied,
      final long lastLogIndex) {
    final MemberRole role =
        topology.getMembers().stream()
            .filter(MemberTopologyRecord.MemberRecord::isSelf)
            .map(MemberTopologyRecord.MemberRecord::getRole)
            .findFirst()
            .orElse(MemberRole.UNKNOWN);
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.HealthResponse(
            new PlacementDriverHttpModel.HealthResponse.Identity(
                PlacementDriverHttpModel.HealthStatus.UP,
                topology.getLocalMemberId(),
                role,
                topology.getLeaderId(),
                topology.getLeaderEndpoint()),
            new PlacementDriverHttpModel.HealthResponse.Endpoint(host, httpPort, grpcPort),
            new PlacementDriverHttpModel.HealthResponse.Consensus(
                topology.getTerm(), quorum(topology), topology.getMembers().size()),
            new PlacementDriverHttpModel.HealthResponse.Progress(
                commitIndex, lastApplied, lastLogIndex)));
  }

  public PlacementDriverEndpointResult toPlacementDriverTopologyResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final MemberTopologyRecord topology =
        readResponseValue(response.value(), MemberTopologyRecord.class);
    final List<PlacementDriverHttpModel.PlacementDriverMemberView> members =
        topology.getMembers().stream()
            .map(
                member ->
                    new PlacementDriverHttpModel.PlacementDriverMemberView(
                        new PlacementDriverHttpModel.PlacementDriverMemberView.Identity(
                            member.getMemberId(),
                            member.getEndpoint(),
                            member.getRole(),
                            member.isSelf()),
                        new PlacementDriverHttpModel.PlacementDriverMemberView.Endpoint(
                            member.getHost(),
                            member.getHttpPort(),
                            member.getGrpcPort(),
                            member.getRaftPort()),
                        new PlacementDriverHttpModel.PlacementDriverMemberView.Registration(
                            member.getStatus(),
                            member.getIncarnationId(),
                            member.getEndpointEpoch()),
                        new PlacementDriverHttpModel.PlacementDriverMemberView.Progress(
                            member.getCommitIndex(),
                            member.getLastApplied(),
                            member.getLastLogIndex(),
                            member.isReadyForPromotion())))
            .toList();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.PlacementDriverTopologyResponse(
            new PlacementDriverHttpModel.PlacementDriverTopologyResponse.Leadership(
                topology.getLocalMemberId(),
                topology.getLeaderId(),
                topology.getLeaderEndpoint(),
                topology.getTerm(),
                quorum(topology)),
            new PlacementDriverHttpModel.PlacementDriverTopologyResponse.Members(members)));
  }

  public PlacementDriverEndpointResult toPlacementDriverConfigurationResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final MemberConfigurationRecord configuration =
        readResponseValue(response.value(), MemberConfigurationRecord.class);
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.PlacementDriverConfigurationResponse(
            configuration.getEpoch(),
            new PlacementDriverHttpModel.PlacementDriverTargetConfiguration(
                configuration.getTargetCount(), configuration.getTargetVoters()),
            new PlacementDriverHttpModel.PlacementDriverActualConfiguration(
                configuration.getLeaderId(),
                configuration.getTerm(),
                configuration.getActualVoters(),
                configuration.getActualLearners()),
            configuration.getActiveChangeId().isBlank()
                ? null
                : configuration.getActiveChangeId()));
  }

  public PlacementDriverEndpointResult toClusterConfigurationResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final ClusterConfigRecord config =
        readResponseValue(response.value(), ClusterConfigRecord.class);
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.ClusterConfigResponse(
            config.getClusterId(),
            config.getClusterKey(),
            config.getClusterSize(),
            config.getPartitionSize(),
            config.getReplicationFactor(),
            config.getVoterRoster()));
  }

  public PlacementDriverEndpointResult toBrokerClustersResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final BrokerClustersRecord clusters =
        readResponseValue(response.value(), BrokerClustersRecord.class);
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.BrokerClustersResponse(
            clusters.getCount(),
            clusters.getClusters().stream()
                .map(
                    cluster ->
                        new PlacementDriverHttpModel.BrokerClusterSummary(
                            new PlacementDriverHttpModel.BrokerClusterSummary.Configuration(
                                cluster.getClusterId(),
                                cluster.getClusterKey(),
                                cluster.getClusterSize(),
                                cluster.getPartitionSize(),
                                cluster.getReplicationFactor()),
                            new PlacementDriverHttpModel.BrokerClusterSummary.Counts(
                                cluster.getRegisteredBrokerCount(),
                                cluster.getOnlineBrokerCount())))
                .toList()));
  }

  public PlacementDriverEndpointResult toBrokerTopologyResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final TopologyRecord topology = readResponseValue(response.value(), TopologyRecord.class);
    final List<PlacementDriverHttpModel.BrokerView> brokers =
        topology.getBrokers().stream()
            .map(
                broker ->
                    new PlacementDriverHttpModel.BrokerView(
                        new PlacementDriverHttpModel.BrokerView.Identity(
                            broker.getBrokerId(), broker.getVersion()),
                        new PlacementDriverHttpModel.BrokerView.Endpoint(
                            broker.getHost(), broker.getPort(), broker.getRaftPort()),
                        new PlacementDriverHttpModel.BrokerView.Lease(
                            formatEpochMillis(broker.getRegisteredAtEpochMillis()),
                            formatEpochMillis(broker.getLastRenewalEpochMillis()),
                            broker.getRenewalTimeoutMillis()),
                        new PlacementDriverHttpModel.BrokerView.Report(
                            broker.getStatus(),
                            broker.getPartitions().stream()
                                .map(
                                    partition ->
                                        new PlacementDriverHttpModel.BrokerPartitionView(
                                            new PlacementDriverHttpModel.BrokerPartitionView.State(
                                                partition.getPartitionId(),
                                                partition.getRole(),
                                                partition.getTerm(),
                                                partition.getLifecycleState()),
                                            new PlacementDriverHttpModel.BrokerPartitionView
                                                .Replication(
                                                partition.getMatchIndex(),
                                                partition.getLag(),
                                                partition.isReadyForPromotion()),
                                            new PlacementDriverHttpModel.BrokerPartitionView
                                                .Operation(
                                                partition.getLastOpId(),
                                                partition.getLastOpStatus()),
                                            new PlacementDriverHttpModel.BrokerPartitionView
                                                .Election(
                                                partition.getPriority(),
                                                partition.getRecoveryEpoch(),
                                                partition.getElection())))
                                .toList())))
            .toList();
    final List<PlacementDriverHttpModel.PartitionView> partitions =
        topology.getPartitions().stream()
            .map(
                partition ->
                    new PlacementDriverHttpModel.PartitionView(
                        new PlacementDriverHttpModel.PartitionView.Identity(
                            partition.getPartitionId(),
                            partition.getLeaderId(),
                            partition.getTerm(),
                            partition.getRecoveryEpoch(),
                            partition.getStatus(),
                            partition.isServiceReady()),
                        new PlacementDriverHttpModel.PartitionView.Membership(
                            partition.getReplicas(), partition.getLearners())))
            .toList();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.TopologyResponse(
            new PlacementDriverHttpModel.TopologyResponse.Identity(
                topology.getClusterId(),
                topology.getVersion(),
                Instant.ofEpochMilli(topology.getUpdatedAtEpochMillis()).toString()),
            new PlacementDriverHttpModel.TopologyResponse.Configuration(
                topology.getClusterSize() > 0
                    ? topology.getClusterSize()
                    : topology.getBrokers().size(),
                topology.getPartitionSize(),
                topology.getReplicationFactor()),
            new PlacementDriverHttpModel.TopologyResponse.Brokers(brokers),
            new PlacementDriverHttpModel.TopologyResponse.Partitions(partitions)));
  }

  public PlacementDriverEndpointResult toWorkerRegistryResult(
      final List<WorkerRegistration> registrations) {
    final List<PlacementDriverHttpModel.WorkerView> workers =
        registrations.stream()
            .map(
                worker ->
                    new PlacementDriverHttpModel.WorkerView(
                        new PlacementDriverHttpModel.WorkerView.Identity(
                            worker.clusterId(),
                            worker.tenantId(),
                            worker.workerId(),
                            worker.name(),
                            worker.jobType()),
                        new PlacementDriverHttpModel.WorkerView.Execution(
                            worker.maxJobsActive(), worker.activeJobs(), worker.concurrency()),
                        new PlacementDriverHttpModel.WorkerView.Polling(
                            worker.timeoutMillis(),
                            worker.requestTimeoutMillis(),
                            worker.pollIntervalMillis(),
                            worker.fetchVariables()),
                        new PlacementDriverHttpModel.WorkerView.Registration(
                            worker.clientVersion(),
                            worker.registeredAt().toString(),
                            worker.lastRenewal().toString(),
                            worker.statusChangedAt().toString(),
                            worker.renewalTimeoutMillis(),
                            Long.toString(worker.workerLeaseToken()),
                            worker.leaseDeadlineEpochMillis()),
                        new PlacementDriverHttpModel.WorkerView.Status(worker.status())))
            .toList();
    final int online =
        (int)
            registrations.stream().filter(worker -> worker.status() == WorkerStatus.ONLINE).count();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.WorkerRegistryResponse(
            Instant.now().toString(),
            registrations.size(),
            online,
            registrations.size() - online,
            workers));
  }

  private static String formatEpochMillis(final long epochMillis) {
    return epochMillis > 0 ? Instant.ofEpochMilli(epochMillis).toString() : null;
  }

  public PlacementDriverEndpointResult toJobYieldResult(List<JobYieldPlan> plans, boolean detail) {
    final var views =
        plans.stream()
            .map(
                plan -> {
                  final var snapshot = plan.workerSnapshot();
                  final var workerView =
                      new PlacementDriverHttpModel.WorkerView(
                          new PlacementDriverHttpModel.WorkerView.Identity(
                              plan.clusterId(),
                              plan.tenantId(),
                              plan.workerId(),
                              snapshot.getName(),
                              snapshot.getJobType()),
                          new PlacementDriverHttpModel.WorkerView.Execution(
                              snapshot.getMaxJobsActive(),
                              snapshot.getActiveJobs(),
                              snapshot.getConcurrency()),
                          new PlacementDriverHttpModel.WorkerView.Polling(
                              snapshot.getTimeoutMillis(),
                              snapshot.getRequestTimeoutMillis(),
                              snapshot.getPollIntervalMillis(),
                              snapshot.getFetchVariables()),
                          new PlacementDriverHttpModel.WorkerView.Registration(
                              snapshot.getClientVersion(),
                              formatEpochMillis(snapshot.getRegisteredAtEpochMillis()),
                              formatEpochMillis(snapshot.getExpectedLastRenewalEpochMillis()),
                              formatEpochMillis(plan.createdAtEpochMillis()),
                              snapshot.getRenewalTimeoutMillis(),
                              Long.toString(plan.workerLeaseToken()),
                              snapshot.getLeaseDeadlineEpochMillis()),
                          new PlacementDriverHttpModel.WorkerView.Status(WorkerStatus.OFFLINE));
                  final int acknowledged =
                      (int)
                          plan.partitions().stream()
                              .filter(target -> target.status() == JobYieldStatus.COMPLETED)
                              .count();
                  return new PlacementDriverHttpModel.JobYieldView(
                      Long.toString(plan.yieldId()),
                      plan.clusterId(),
                      plan.tenantId(),
                      plan.workerId(),
                      snapshot.getName(),
                      snapshot.getJobType(),
                      Long.toString(plan.workerLeaseToken()),
                      plan.reason(),
                      plan.completedAtEpochMillis() > 0
                          ? JobYieldStatus.COMPLETED
                          : JobYieldStatus.PENDING,
                      plan.createdAtEpochMillis(),
                      plan.completedAtEpochMillis(),
                      plan.partitions().stream()
                          .mapToLong(JobYieldPlan.Partition::releasedJobCount)
                          .sum(),
                      plan.partitions().size(),
                      acknowledged,
                      workerView,
                      plan.partitions());
                })
            .toList();
    return new PlacementDriverEndpointResult(
        200,
        detail
            ? views.getFirst()
            : new PlacementDriverHttpModel.JobYieldResponse(
                Instant.now().toString(), views.size(), views));
  }

  public PlacementDriverEndpointResult toClusterChangeResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final BrokerClusterChangeStatusRecord change =
        readResponseValue(response.value(), BrokerClusterChangeStatusRecord.class);
    final List<PlacementDriverHttpModel.PartitionPlacementView> currentTopology =
        change.getCurrentTopology().stream()
            .map(PlacementDriverResponseMapper::partitionPlacement)
            .toList();
    final List<PlacementDriverHttpModel.TopologyOperationView> plannedChanges =
        change.getPlannedChanges().stream()
            .map(PlacementDriverResponseMapper::topologyOperation)
            .toList();
    final var pending =
        change.getPartitionChanges().stream()
            .map(PlacementDriverResponseMapper::partitionChangeProgress)
            .toList();
    final var history =
        change.getExecutionHistory().stream()
            .map(PlacementDriverResponseMapper::partitionChangeProgress)
            .toList();
    final List<PlacementDriverHttpModel.PartitionPlacementView> expectedTopology =
        change.getExpectedTopology().stream()
            .map(PlacementDriverResponseMapper::partitionPlacement)
            .toList();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.ClusterChangeStatusResponse(
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Identity(
                change.getChangeId(),
                change.getStatus(),
                change.getChangeType(),
                change.isCancellable(),
                change.getCancellationReason()),
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Topologies(
                currentTopology, expectedTopology),
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Changes(
                plannedChanges, pending, history)));
  }

  private static PlacementDriverHttpModel.PendingChangeProgressView partitionChangeProgress(
      final BrokerClusterChangeStatusRecord.PartitionChange partition) {
    return new PlacementDriverHttpModel.PendingChangeProgressView(
        new PlacementDriverHttpModel.PendingChangeProgressView.Identity(
            partition.partitionId(),
            partition.status(),
            partition.changeId(),
            partition.terminal()),
        new PlacementDriverHttpModel.PendingChangeProgressView.Progress(
            partition.nextOperationIndex(),
            partition.operationCount(),
            partition.currentOperation() == null
                ? null
                : topologyOperation(partition.currentOperation()),
            partition.remainingOperations().stream()
                .map(PlacementDriverResponseMapper::topologyOperation)
                .toList()),
        new PlacementDriverHttpModel.PendingChangeProgressView.Deadline(
            partition.startedAt(), partition.deadline()),
        new PlacementDriverHttpModel.PendingChangeProgressView.Execution(
            partition.executionGeneration(),
            partition.authorityIndex(),
            partition.rootChangeId(),
            partition.supersedes(),
            partition.supersededBy(),
            partition.blockedReason(),
            partition.blockedSince()));
  }

  public PlacementDriverEndpointResult toCreateClusterResult(
      final PlacementDriverApiResponse<ClusterConfigRecord> response) {
    final ClusterConfigRecord value = response.value();
    return new PlacementDriverEndpointResult(
        value.getOutcome() == ClusterConfigurationOutcome.EXISTS ? 200 : 201,
        new PlacementDriverHttpModel.CreateClusterResponse(
            value.getClusterId(),
            value.getClusterKey(),
            value.getClusterSize(),
            value.getPartitionSize(),
            value.getReplicationFactor(),
            value.getOutcome() != ClusterConfigurationOutcome.EXISTS));
  }

  public PlacementDriverEndpointResult toCreateJobSubscriptionMirrorResult(
      final PlacementDriverApiResponse<JobSubscriptionMirrorRecord> response) {
    final JobSubscriptionMirrorRecord mirror = response.value();
    final boolean created = mirror.getOutcome() != JobSubscriptionMirrorCreationOutcome.EXISTS;
    return new PlacementDriverEndpointResult(
        created ? 201 : 200, jobSubscriptionMirrorView(mirror, created));
  }

  public PlacementDriverEndpointResult toJobSubscriptionMirrorQueryResult(
      final PlacementDriverApiResponse<JobSubscriptionMirrorQueryRecord> response) {
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.JobSubscriptionMirrorListResponse(
            response.value().getMirrors().stream()
                .map(mirror -> jobSubscriptionMirrorView(mirror, null))
                .toList()));
  }

  public PlacementDriverEndpointResult toJobSubscriptionMirrorResult(
      final PlacementDriverApiResponse<JobSubscriptionMirrorQueryRecord> response) {
    return new PlacementDriverEndpointResult(
        200, jobSubscriptionMirrorView(response.value().getMirrors().getFirst(), null));
  }

  public PlacementDriverEndpointResult toDeleteJobSubscriptionMirrorResult(
      final PlacementDriverApiResponse<JobSubscriptionMirrorRecord> response) {
    return new PlacementDriverEndpointResult(204, null);
  }

  public PlacementDriverEndpointResult toJobSubscriptionDistributionResult(
      final PlacementDriverApiResponse<CommandDistributionQueryRecord> response) {
    return new PlacementDriverEndpointResult(
        200, jobSubscriptionDistributionView(response.value()));
  }

  public PlacementDriverEndpointResult toJobSubscriptionDistributionListResult(
      final PlacementDriverApiResponse<CommandDistributionListRecord> response) {
    final var value = response.value();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.JobSubscriptionDistributionListResponse(
            value.getTotal(),
            value.getSubscriptions().stream()
                .map(PlacementDriverResponseMapper::jobSubscriptionDistributionView)
                .toList()));
  }

  public PlacementDriverEndpointResult toRedistributeJobSubscriptionResult(
      final PlacementDriverApiResponse<CommandDistributionRecord<?>> response) {
    final var value = response.value();
    return new PlacementDriverEndpointResult(
        202,
        new PlacementDriverHttpModel.JobSubscriptionRedistributionAcceptedResponse(
            value.getDistributionKey(),
            value.getGeneration(),
            value.getLastRedistributedAt(),
            JobSubscriptionDistributionStatus.DISTRIBUTING));
  }

  private static PlacementDriverHttpModel.JobSubscriptionDistributionView
      jobSubscriptionDistributionView(final CommandDistributionQueryRecord distribution) {
    return new PlacementDriverHttpModel.JobSubscriptionDistributionView(
        distribution.getDistributionKey(),
        distribution.getSubscriptionKey() > 0 ? distribution.getSubscriptionKey() : null,
        distribution.getDeletionDistributionKey() > 0
            ? distribution.getDeletionDistributionKey()
            : null,
        distribution.getOriginClusterId(),
        distribution.getClusterId(),
        distribution.getCreationType(),
        distribution.getValueType(),
        distribution.getStatus(),
        distribution.getTargets().stream()
            .map(
                target ->
                    new PlacementDriverHttpModel.JobSubscriptionDistributionTargetView(
                        target.getClusterId(),
                        target.getExpectedPartitions(),
                        target.getAcknowledgedPartitions(),
                        target.getPendingPartitions(),
                        target.getStatus(),
                        target.getConfirmedPartitionIds(),
                        target.getMirrorKey() > 0 ? target.getMirrorKey() : null))
            .toList(),
        distribution.getName(),
        distribution.getJobType(),
        distribution.getTenantId(),
        distribution.getSubscription(),
        distribution.getGeneration(),
        distribution.getLastRedistributedAt());
  }

  public PlacementDriverEndpointResult toConfigurationChangeResult(
      final PlacementDriverApiResponse<ClusterConfigRecord> response) {
    final ClusterConfigRecord value = response.value();
    if (value.getOutcome() == ClusterConfigurationOutcome.NO_CHANGES) {
      return new PlacementDriverEndpointResult(
          200, new PlacementDriverHttpModel.ClusterChangeAcceptedResponse("NO_CHANGES"));
    }
    if (value.getOutcome() == ClusterConfigurationOutcome.RECOVERY_PLANNED) {
      return new PlacementDriverEndpointResult(
          202,
          new PlacementDriverHttpModel.RecoverQuorumResponse(
              new PlacementDriverHttpModel.RecoverQuorumResponse.Identity(
                  value.getClusterId(),
                  value.getChangeId(),
                  value.getRequestedPartitionId() == null ? 0 : value.getRequestedPartitionId(),
                  value.getSurvivorBrokerId()),
              new PlacementDriverHttpModel.RecoverQuorumResponse.Outcome(
                  value.getSourceTopologyVersion() + 1, ChangeStatus.PENDING)));
    }
    return new PlacementDriverEndpointResult(
        202, new PlacementDriverHttpModel.ClusterChangeAcceptedResponse(value.getChangeId()));
  }

  public PlacementDriverEndpointResult toMemberChangeResult(
      final PlacementDriverApiResponse<MemberChangeRecord> response) {
    final MemberChangeRecord value = response.value();
    return new PlacementDriverEndpointResult(
        value.getOutcome() == MemberChangeOutcome.NO_CHANGES ? 200 : 202,
        new PlacementDriverHttpModel.ClusterChangeAcceptedResponse(
            value.getOutcome() == MemberChangeOutcome.NO_CHANGES
                ? MemberChangeOutcome.NO_CHANGES.name()
                : value.getChangeId()));
  }

  public PlacementDriverEndpointResult toPlacementDriverMemberChangeResult(
      final PlacementDriverApiResponse<MemberChangeRecord> response) {
    final MemberChangeRecord value = response.value();
    final boolean noChanges = value.getOutcome() == MemberChangeOutcome.NO_CHANGES;
    return new PlacementDriverEndpointResult(
        noChanges ? 200 : 202,
        new PlacementDriverHttpModel.PlacementDriverMemberChangeAcceptedResponse(
            new PlacementDriverHttpModel.PlacementDriverMemberChangeAcceptedResponse.Identity(
                noChanges ? MemberChangeOutcome.NO_CHANGES.name() : value.getChangeId(),
                value.getStatus()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeAcceptedResponse.Epochs(
                noChanges ? null : value.getSourceEpoch(),
                noChanges ? null : value.getTargetEpoch()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeAcceptedResponse.Target(
                noChanges ? null : value.getTargetCount(),
                noChanges ? null : value.getTargetVoters())));
  }

  public PlacementDriverEndpointResult toPlacementDriverMemberChangeStatusResult(
      final PlacementDriverApiResponse<TopologyQueryRecord> response) {
    final MemberChangeResponseRecord result =
        readResponseValue(response.value(), MemberChangeResponseRecord.class);
    final MemberChangeRecord change = result.getChange();
    final List<PlacementDriverHttpModel.PlacementDriverMemberOperationView> plannedChanges =
        change.getOperations().stream()
            .map(PlacementDriverResponseMapper::memberOperation)
            .toList();
    final List<PlacementDriverHttpModel.PlacementDriverMemberOperationView> pendingChanges =
        change.isTerminal() || change.getNextOperationIndex() >= change.getOperations().size()
            ? List.of()
            : change
                .getOperations()
                .subList(change.getNextOperationIndex(), change.getOperations().size())
                .stream()
                .map(PlacementDriverResponseMapper::memberOperation)
                .toList();
    return new PlacementDriverEndpointResult(
        200,
        new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse(
            new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.Identity(
                change.getChangeId(),
                change.getStatus(),
                change.getError().isBlank() ? null : change.getError()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.Epochs(
                change.getSourceEpoch(), change.getTargetEpoch()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.Target(
                change.getTargetCount(), change.getTargetVoters()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.Actual(
                result.getActualVoters(), result.getActualLearners()),
            new PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.Execution(
                pendingChanges.isEmpty() ? null : pendingChanges.getFirst(),
                plannedChanges,
                plannedChanges,
                pendingChanges)));
  }

  public PlacementDriverEndpointResult toDeleteClusterResult(
      final PlacementDriverApiResponse<ClusterConfigRecord> response) {
    return new PlacementDriverEndpointResult(204, null);
  }

  public PlacementDriverEndpointResult toCancelChangeResult(
      final PlacementDriverApiResponse<ClusterConfigRecord> response) {
    final ClusterConfigRecord value = response.value();
    return new PlacementDriverEndpointResult(
        202,
        new PlacementDriverHttpModel.CancelChangeResponse(
            value.getClusterId(), value.getChangeId(), ChangeStatus.CANCELLED));
  }

  public PlacementDriverEndpointResult toHttpError(final Throwable error, final String clusterId) {
    final Throwable cause = unwrap(error);
    if (isBackpressure(cause)) {
      return new PlacementDriverEndpointResult(
          429,
          new PlacementDriverHttpModel.AdminOperationErrorResponse(
              "BACKPRESSURE", cause.getMessage(), null));
    }
    if (cause instanceof PlacementDriverRejectionException rejection) {
      final int status =
          switch (rejection.type()) {
            case INVALID_ARGUMENT -> 400;
            case NOT_FOUND -> 404;
            case ALREADY_EXISTS, INVALID_STATE, STALE_WORKER_SESSION, FEATURE_DISABLED -> 409;
            case WORKER_SESSION_NOT_READY -> 503;
            case PROCESSING_ERROR, EXCEEDED_BATCH_RECORD_SIZE, NULL_VAL, SBE_UNKNOWN -> 503;
          };
      return new PlacementDriverEndpointResult(
          status,
          new PlacementDriverHttpModel.AdminOperationErrorResponse(
              rejectionCode(rejection),
              rejection.getMessage(),
              null,
              rejectionPendingChangeIds(rejection)));
    }
    if (cause instanceof PlacementDriverNotLeaderException notLeader) {
      return new PlacementDriverEndpointResult(
          503,
          new PlacementDriverHttpModel.AdminOperationErrorResponse(
              "NOT_LEADER", messageOf(cause), notLeader.leaderEndpoint()));
    }
    if (cause instanceof IllegalArgumentException) {
      return new PlacementDriverEndpointResult(
          400,
          new PlacementDriverHttpModel.AdminOperationErrorResponse(
              messageOf(cause), messageOf(cause), null));
    }
    return new PlacementDriverEndpointResult(
        503,
        new PlacementDriverHttpModel.AdminOperationErrorResponse(
            "REQUEST_FAILED", messageOf(cause), null));
  }

  private static boolean isBackpressure(final Throwable cause) {
    if (cause instanceof BackpressureException) {
      return true;
    }
    final Status status = Status.fromThrowable(cause);
    return status.getCode() == Status.Code.RESOURCE_EXHAUSTED
        && status.getDescription() != null
        && status.getDescription().startsWith("BACKPRESSURE:");
  }

  private static <T> T readResponseValue(
      final TopologyQueryRecord response, final Class<T> responseType) {
    try {
      return MAPPER.readValue(response.getResponseValue(), responseType);
    } catch (final JsonProcessingException error) {
      throw new IllegalStateException("failed to map query response", error);
    }
  }

  private static int parsePort(final String value) {
    try {
      return Integer.parseInt(value);
    } catch (final NumberFormatException ignored) {
      return 0;
    }
  }

  private static String rejectionCode(final PlacementDriverRejectionException rejection) {
    final String reason = rejection.reason();
    if (reason != null) {
      final String code = reason.lines().findFirst().orElse("").split(":", 2)[0].trim();
      if (code.matches("[A-Z][A-Z0-9_]*")) {
        return code;
      }
    }
    return rejection.type().name();
  }

  private static List<String> rejectionPendingChangeIds(
      final PlacementDriverRejectionException rejection) {
    final String reason = rejection.reason();
    if (reason == null || !reason.startsWith("TOPOLOGY_CHANGE_IN_PROGRESS:")) {
      return null;
    }
    // The first line retains the comma-separated machine-readable ids; subsequent lines explain
    // the rejected operation. Legacy reasons contain only the first line.
    final String changeIds =
        reason.substring(reason.indexOf(':') + 1).lines().findFirst().orElse("");
    return changeIds.isBlank()
        ? List.of()
        : Arrays.stream(changeIds.split(","))
            .map(String::trim)
            .filter(id -> !id.isEmpty())
            .toList();
  }

  private static PlacementDriverHttpModel.TopologyOperationView topologyOperation(
      final BrokerClusterChangeStatusRecord.Operation operation) {
    return new PlacementDriverHttpModel.TopologyOperationView(
        operation.opId(), operation.type(), operation.targetBroker(), operation.priority());
  }

  private static PlacementDriverHttpModel.JobSubscriptionMirrorView jobSubscriptionMirrorView(
      final JobSubscriptionMirrorRecord mirror, final Boolean created) {
    return new PlacementDriverHttpModel.JobSubscriptionMirrorView(
        mirror.getMirrorKey(),
        mirror.getSourceClusterId(),
        mirror.getTargetClusterId(),
        JobSubscriptionMirrorStatus.ACTIVE,
        created);
  }

  private static PlacementDriverHttpModel.PlacementDriverMemberOperationView memberOperation(
      final MemberChangeOperation operation) {
    return new PlacementDriverHttpModel.PlacementDriverMemberOperationView(
        operation.getType(),
        operation.getMemberId(),
        operation.getStatus(),
        operation.getBarrierIndex() > 0 ? operation.getBarrierIndex() : null,
        operation.getError().isBlank() ? null : operation.getError());
  }

  private static int quorum(final MemberTopologyRecord topology) {
    final long voters =
        topology.getMembers().stream().filter(member -> member.getRole().isVoter()).count();
    return (int) (voters / 2 + 1);
  }

  private static PlacementDriverHttpModel.PartitionPlacementView partitionPlacement(
      final BrokerClusterChangeStatusRecord.PartitionPlacement partition) {
    return new PlacementDriverHttpModel.PartitionPlacementView(
        new PlacementDriverHttpModel.PartitionPlacementView.Identity(
            partition.partitionId(), partition.status()),
        new PlacementDriverHttpModel.PartitionPlacementView.Membership(
            partition.desiredVoters(),
            partition.currentVoters(),
            partition.learners(),
            partition.desiredPrimary(),
            partition.priorities()));
  }

  private static Throwable unwrap(final Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private static String messageOf(final Throwable error) {
    final String message = error.getMessage();
    return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
  }
}
