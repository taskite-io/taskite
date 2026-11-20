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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.api.model.PlacementDriverHttpModel;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionMirrorRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.PlacementDriverConfigurationPatch;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.protocol.BrokerPartitionState;
import io.taskite.placementdriver.protocol.CreateJobSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.runtime.PlacementDriverNodeSnapshot;
import io.taskite.placementdriver.runtime.PlacementDriverNotLeaderException;
import io.taskite.placementdriver.runtime.PlacementDriverRejectionException;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
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
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorCreationOutcome;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.streamplatform.api.backpressure.BackpressureException;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.topology.MemberRole;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class PlacementDriverApiMapperTest {

  @Test
  void mapsSubscriptionDefaultsAndPreservesExplicitZeroOptions() {
    final var mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    for (final boolean supplied : List.of(false, true)) {
      final var subscription = CreateJobSubscriptionRequest.newBuilder().setJobType("charge");
      if (supplied) {
        subscription.setRetries(0).setPriority(0);
      }
      final var request =
          DistributeCommandRequest.newBuilder()
              .setClusterId("demo")
              .setCreateJobSubscription(subscription)
              .build();
      final var value =
          (JobSubscriptionRecord)
              ((CommandDistributionRequestRecord<?>)
                      mapper.toDistributeCommandRequest(request).value())
                  .getValue();
      assertEquals("", value.getName());
      assertEquals("{}", value.getVariables());
      assertEquals(supplied ? 0 : 3, value.getRetries());
      assertEquals(supplied ? 0 : 50, value.getPriority());
    }
  }

  @Test
  void mapsTimerDefinitionsAndDeletionWithClusterTenantAndExactKey() {
    final var mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    for (final var type :
        List.of(
            io.taskite.placementdriver.protocol.TimerSubscriptionType.DURATION,
            io.taskite.placementdriver.protocol.TimerSubscriptionType.CYCLE,
            io.taskite.placementdriver.protocol.TimerSubscriptionType.CRON,
            io.taskite.placementdriver.protocol.TimerSubscriptionType.TIMER_TYPE_UNSPECIFIED)) {
      final var request =
          mapper.toCreateTimerSubscriptionRequest(
              io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest.newBuilder()
                  .setClusterId("cluster")
                  .setTenantId("tenant")
                  .setName("daily")
                  .setType(type)
                  .setExpression("PT1S")
                  .addAllJobTypes(List.of("charge", "notify"))
                  .setVariables("{\"amount\":42}")
                  .build());
      assertEquals(ValueType.TIMER, request.valueType());
      assertEquals(io.taskite.protocol.record.intent.common.TimerIntent.CREATE, request.intent());
      final var value =
          (io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord) request.value();
      assertEquals(
          type == io.taskite.placementdriver.protocol.TimerSubscriptionType.TIMER_TYPE_UNSPECIFIED
              ? "UNSPECIFIED"
              : type.name(),
          value.getType().name());
      assertEquals("cluster", value.getClusterId());
      assertEquals("tenant", value.getTenantId());
      assertEquals("daily", value.getName());
      assertEquals("PT1S", value.getExpression());
      assertEquals(List.of("charge", "notify"), value.getJobTypes());
      assertEquals("{\"amount\":42}", value.getVariables());
    }
    final long key = 9007199254740993L;
    final var deletion =
        mapper.toDeleteTimerSubscriptionRequest(
            io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest.newBuilder()
                .setTimerSubscriptionKey(key)
                .setClusterId("cluster")
                .setTenantId("tenant")
                .build());
    assertEquals(key, deletion.key());
    assertEquals(io.taskite.protocol.record.intent.common.TimerIntent.CANCEL, deletion.intent());
    final var value =
        (io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord) deletion.value();
    assertEquals("cluster", value.getClusterId());
    assertEquals("tenant", value.getTenantId());
    org.junit.jupiter.api.Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            mapper.toDeleteTimerSubscriptionRequest(
                io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest
                    .getDefaultInstance()));
    final var responseMapper = new PlacementDriverResponseMapper();
    final var created =
        responseMapper.toCreateTimerSubscriptionResponse(
            new PlacementDriverApiResponse<>(value.setDueDate(key + 1000), key));
    assertEquals(key, created.getTimerSubscriptionKey());
    assertEquals(key + 1000, created.getDueDate());
    assertEquals(
        key,
        responseMapper
            .toDeleteTimerSubscriptionResponse(new PlacementDriverApiResponse<>(value, key))
            .getTimerSubscriptionKey());
  }

  @Test
  void mapsLocalAndForwardedBackpressureTo429AndResourceExhausted() {
    final var mapper = new PlacementDriverResponseMapper();
    final var local = new BackpressureException(1);
    final var forwarded = mapper.toGrpcStatus(local).asRuntimeException();
    for (final Throwable error : List.of(local, forwarded, new CompletionException(forwarded))) {
      assertEquals(Status.Code.RESOURCE_EXHAUSTED, mapper.toGrpcStatus(error).getCode());
      final var http = mapper.toHttpError(error, "cluster-a");
      assertEquals(429, http.status());
      final var body =
          assertInstanceOf(PlacementDriverHttpModel.AdminOperationErrorResponse.class, http.body());
      assertEquals("BACKPRESSURE", body.error());
    }
    assertEquals(
        503,
        mapper
            .toHttpError(
                Status.RESOURCE_EXHAUSTED
                    .withDescription("resource too large")
                    .asRuntimeException(),
                "cluster-a")
            .status());
  }

  @Test
  void clusterChangeResponsePreservesServerCancellationPolicyThroughJson() {
    for (final boolean cancellable : List.of(true, false)) {
      final var change =
          new BrokerClusterChangeStatusRecord()
              .setChangeId("recover:broker-4:3:id")
              .setStatus(cancellable ? ChangeStatus.WAITING_READY : ChangeStatus.SUPERSEDING)
              .setChangeType(ChangeType.PARTITION)
              .setCancellable(cancellable)
              .setCancellationReason(
                  cancellable
                      ? ChangeCancellationReason.NONE
                      : ChangeCancellationReason.CHANGE_RECONCILING);
      final var result =
          new PlacementDriverResponseMapper()
              .toClusterChangeResult(
                  new PlacementDriverApiResponse<>(
                      new TopologyQueryRecord().setResponseValue(change.toJson()), 1));
      final var response =
          assertInstanceOf(
              PlacementDriverHttpModel.ClusterChangeStatusResponse.class, result.body());
      assertEquals(200, result.status());
      assertEquals(change.getChangeId(), response.changeId());
      assertEquals(ChangeType.PARTITION, response.identityData().changeType());
      assertEquals(cancellable, response.identityData().cancellable());
      assertEquals(change.getCancellationReason(), response.identityData().cancellationReason());
    }
  }

  @Test
  void cancellationConflictsAndMissingChangesHaveDistinctHttpStatuses() {
    final var mapper = new PlacementDriverResponseMapper();
    final String id = "recover:broker-4:3:id";
    for (final String code :
        List.of(
            "CHANGE_RECONCILING",
            "CHANGE_RECOVERY_REQUIRED",
            "CHANGE_SUPERSEDED",
            "CHANGE_NOT_FOUND")) {
      for (final String reason :
          List.of(
              code + ": " + id,
              code + ": Expected to cancel topology change '" + id + "' in cluster 'demo'")) {
        final var result =
            mapper.toHttpError(
                new PlacementDriverRejectionException(
                    code.equals("CHANGE_NOT_FOUND")
                        ? RejectionType.NOT_FOUND
                        : RejectionType.INVALID_STATE,
                    reason),
                "demo");
        assertEquals(code.equals("CHANGE_NOT_FOUND") ? 404 : 409, result.status());
        final var response =
            assertInstanceOf(
                PlacementDriverHttpModel.AdminOperationErrorResponse.class, result.body());
        assertEquals(code, response.error());
        assertEquals(reason, response.message());
      }
    }
    final String pendingIds = "recover:broker-4:3:id, rebalance:broker-2:1:id";
    for (final String detail :
        List.of(
            pendingIds,
            pendingIds + "\nExpected to configure cluster 'demo', but topology changes are pending",
            "",
            "\nExpected to configure cluster 'demo', but a partition change is pending")) {
      final String reason = "TOPOLOGY_CHANGE_IN_PROGRESS:" + detail;
      final var result =
          mapper.toHttpError(
              new PlacementDriverRejectionException(RejectionType.INVALID_STATE, reason), "demo");
      final var response =
          assertInstanceOf(
              PlacementDriverHttpModel.AdminOperationErrorResponse.class, result.body());
      assertEquals(409, result.status());
      assertEquals("TOPOLOGY_CHANGE_IN_PROGRESS", response.error());
      assertEquals(reason, response.message());
      assertEquals(
          detail.startsWith(pendingIds)
              ? List.of("recover:broker-4:3:id", "rebalance:broker-2:1:id")
              : List.of(),
          response.pendingChangeIds());
    }
    for (final String reason :
        List.of(
            "TOPOLOGY_CHANGE_IN_PROGRESS",
            "TOPOLOGY_CHANGE_IN_PROGRESS\nExpected to configure cluster 'demo', but a partition change is pending")) {
      final var result =
          mapper.toHttpError(
              new PlacementDriverRejectionException(RejectionType.INVALID_STATE, reason), "demo");
      final var response =
          assertInstanceOf(
              PlacementDriverHttpModel.AdminOperationErrorResponse.class, result.body());
      assertEquals(409, result.status());
      assertEquals("TOPOLOGY_CHANGE_IN_PROGRESS", response.error());
      assertEquals(reason, response.message());
      assertNull(response.pendingChangeIds());
    }
  }

  @Test
  void partitionCancellationKeepsThePublicAcceptanceResponse() {
    final var value =
        new ClusterConfigRecord().setClusterId("demo").setChangeId("recover:broker-4:3:id");
    final var result =
        new PlacementDriverResponseMapper()
            .toCancelChangeResult(new PlacementDriverApiResponse<>(value, 1));
    assertEquals(202, result.status());
    final var response =
        assertInstanceOf(PlacementDriverHttpModel.CancelChangeResponse.class, result.body());
    assertEquals("demo", response.clusterId());
    assertEquals(value.getChangeId(), response.changeId());
    assertEquals(ChangeStatus.CANCELLED, response.status());
  }

  @Test
  void clusterConfigurationResponseIncludesAuthoritativeVoterRoster() {
    final var config =
        new ClusterConfigRecord()
            .setClusterId("demo")
            .setClusterKey(9)
            .setClusterSize(3)
            .setPartitionSize(5)
            .setReplicationFactor(3)
            .setVoterRoster(List.of("broker-0", "broker-1", "broker-2"));
    final var result =
        new PlacementDriverResponseMapper()
            .toClusterConfigurationResult(
                new PlacementDriverApiResponse<>(
                    new TopologyQueryRecord().setResponseValue(config.toJson()), 1));
    final var response =
        assertInstanceOf(PlacementDriverHttpModel.ClusterConfigResponse.class, result.body());

    assertEquals(9, response.clusterKey());
    assertEquals(3, response.clusterSize());
    assertEquals(List.of("broker-0", "broker-1", "broker-2"), response.voterRoster());

    final var grpc =
        new PlacementDriverResponseMapper()
            .toGetClusterConfigResponse(
                new PlacementDriverApiResponse<>(
                    new TopologyQueryRecord().setResponseValue(config.toJson()), 1));
    assertTrue(grpc.hasClusterKey());
    assertEquals(9, grpc.getClusterKey());

    final var created =
        assertInstanceOf(
            PlacementDriverHttpModel.CreateClusterResponse.class,
            new PlacementDriverResponseMapper()
                .toCreateClusterResult(new PlacementDriverApiResponse<>(config, 1))
                .body());
    assertEquals(9, created.clusterKey());
  }

  @Test
  void requestMapperMapsTypedProtoToInternalCommand() {
    final PlacementDriverRequestMapper mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    final LoggedEvent command =
        mapper.toPlacementDriverCommand(
            mapper.toGetClusterConfigRequest(
                GetClusterConfigRequest.newBuilder().setClusterId("demo").build()));

    assertEquals(ValueType.TOPOLOGY, command.metadata().getValueType());
    assertEquals(TopologyIntent.QUERY, command.metadata().getIntent());
    assertEquals(RecordType.COMMAND, command.metadata().getRecordType());
    final TopologyQueryRecord value = assertInstanceOf(TopologyQueryRecord.class, command.value());
    assertEquals("demo", value.getClusterId());
    assertEquals(QueryType.BROKER_CLUSTER_CONFIGURATION, value.getQueryType());
  }

  @Test
  void placementDriverQueryCarriesRequestingMemberId() {
    final PlacementDriverApiRequest<TopologyQueryRecord> request =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry())
            .toPlacementDriverQueryRequest(
                QueryType.PLACEMENT_DRIVER_HEALTH,
                new PlacementDriverNodeSnapshot("meta-1", "127.0.0.1", 19001, 29001, 12, 11, 13));

    final TopologyQueryRecord value =
        assertInstanceOf(TopologyQueryRecord.class, request.getRequestWriter());
    assertEquals(QueryType.PLACEMENT_DRIVER_HEALTH, value.getQueryType());
    assertEquals("meta-1", value.getRequestingMemberId());
    assertEquals("127.0.0.1", value.getRequestingHost());
    assertEquals(19001, value.getRequestingHttpPort());
    assertEquals(29001, value.getRequestingGrpcPort());
  }

  @Test
  void placementDriverConfigurationPatchCreatesMemberChangeRequest() {
    final PlacementDriverApiRequest<MemberChangeRecord> request =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry())
            .toPlacementDriverMemberChangeRequest(
                new PlacementDriverConfigurationPatch(List.of("meta-3"), List.of(), 4));

    final MemberChangeRecord value =
        assertInstanceOf(MemberChangeRecord.class, request.getRequestWriter());
    assertEquals(ValueType.MEMBER_CHANGE, request.getValueType());
    assertEquals(MemberChangeIntent.REQUEST, request.getIntent());
    assertEquals(MemberChangeScope.PLACEMENT_DRIVER, value.getScope());
    assertEquals(PlacementDriverCluster.CLUSTER_ID, value.getResourceId());
    assertEquals(List.of("meta-3"), value.getRequestedAdditions());
    assertEquals(4, value.getRequestedCount());
  }

  @Test
  void brokerClustersQueryAndResponseUseStructuredSummary() {
    final PlacementDriverRequestMapper requestMapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    final PlacementDriverApiRequest<TopologyQueryRecord> request =
        requestMapper.toQueryRequest(QueryType.BROKER_CLUSTERS, "", "");
    final TopologyQueryRecord requestValue =
        assertInstanceOf(TopologyQueryRecord.class, request.getRequestWriter());
    assertEquals(QueryType.BROKER_CLUSTERS, requestValue.getQueryType());

    final BrokerClustersRecord clusters =
        new BrokerClustersRecord()
            .setCount(1)
            .setClusters(
                List.of(
                    new BrokerClustersRecord.Cluster()
                        .setClusterId("demo")
                        .setClusterKey(9)
                        .setClusterSize(3)
                        .setPartitionSize(5)
                        .setReplicationFactor(3)
                        .setRegisteredBrokerCount(2)
                        .setOnlineBrokerCount(1)));
    final var result =
        new PlacementDriverResponseMapper()
            .toBrokerClustersResult(
                new PlacementDriverApiResponse<>(
                    new TopologyQueryRecord().setResponseValue(clusters.toJson()), 1));
    final var response =
        assertInstanceOf(PlacementDriverHttpModel.BrokerClustersResponse.class, result.body());

    assertEquals(1, response.count());
    assertEquals("demo", response.clusters().getFirst().clusterId());
    assertEquals(9, response.clusters().getFirst().clusterKey());
    assertEquals(3, response.clusters().getFirst().clusterSize());
    assertEquals(5, response.clusters().getFirst().partitionSize());
    assertEquals(2, response.clusters().getFirst().registeredBrokerCount());
    assertEquals(1, response.clusters().getFirst().onlineBrokerCount());
  }

  @Test
  void jobSubscriptionMirrorRequestsUseStructuredCommands() {
    final PlacementDriverRequestMapper mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());

    final PlacementDriverApiRequest<JobSubscriptionMirrorRecord> create =
        mapper.toCreateJobSubscriptionMirrorRequest(
            new CreateJobSubscriptionMirrorRequest("region-a", "region-b"));
    final PlacementDriverApiRequest<JobSubscriptionMirrorQueryRecord> query =
        mapper.toJobSubscriptionMirrorQueryRequest("", "region-a", "region-b");
    final PlacementDriverApiRequest<JobSubscriptionMirrorRecord> delete =
        mapper.toDeleteJobSubscriptionMirrorRequest("9001");

    final JobSubscriptionMirrorRecord createValue =
        assertInstanceOf(JobSubscriptionMirrorRecord.class, create.getRequestWriter());
    final JobSubscriptionMirrorQueryRecord queryValue =
        assertInstanceOf(JobSubscriptionMirrorQueryRecord.class, query.getRequestWriter());
    final JobSubscriptionMirrorRecord deleteValue =
        assertInstanceOf(JobSubscriptionMirrorRecord.class, delete.getRequestWriter());
    assertEquals(ValueType.JOB_SUBSCRIPTION_MIRROR, create.getValueType());
    assertEquals(JobSubscriptionMirrorIntent.CREATE, create.getIntent());
    assertEquals("region-a", createValue.getSourceClusterId());
    assertEquals("region-b", createValue.getTargetClusterId());
    assertEquals(JobSubscriptionMirrorIntent.QUERY, query.getIntent());
    assertEquals("region-a", queryValue.getSourceClusterId());
    assertEquals("region-b", queryValue.getTargetClusterId());
    assertEquals(JobSubscriptionMirrorIntent.DELETE, delete.getIntent());
    assertEquals(9001, deleteValue.getMirrorKey());
  }

  @Test
  void jobSubscriptionDistributionQueryUsesStructuredCommand() {
    final PlacementDriverApiRequest<CommandDistributionQueryRecord> request =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry())
            .toJobSubscriptionDistributionQueryRequest("1001");

    final CommandDistributionQueryRecord value =
        assertInstanceOf(CommandDistributionQueryRecord.class, request.getRequestWriter());
    assertEquals(ValueType.COMMAND_DISTRIBUTION, request.getValueType());
    assertEquals(CommandDistributionIntent.QUERY, request.getIntent());
    assertEquals(1001, value.getDistributionKey());
  }

  @Test
  void registryRequestMapperConvertsBrokerProtocolRequestsToCommands() {
    final PlacementDriverRequestMapper mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    final var partition =
        BrokerPartitionState.newBuilder()
            .setPartitionId(3)
            .setRole("LEADER")
            .setTerm(7)
            .setLifecycleState("ACTIVE")
            .setMatchIndex(42)
            .setLag(2)
            .setReadyForPromotion(true)
            .setPriority(100)
            .setLastOperationId("op-1")
            .setLastOperationStatus("COMPLETED")
            .setRecoveryEpoch(9)
            .build();

    final PlacementDriverApiRequest<BrokerRegistrationRecord> registration =
        mapper.toRegisterBrokerRequest(
            RegisterBrokerRequest.newBuilder()
                .setClusterId("demo")
                .setBrokerId("broker-1")
                .setHost("127.0.0.1")
                .setPort(26501)
                .setRaftPort(26601)
                .setVersion("0.1.0")
                .setRenewalTimeoutMillis(45_000)
                .addPartitions(partition)
                .build());
    final PlacementDriverApiRequest<BrokerRegistrationRecord> renewal =
        mapper.toRenewBrokerRequest(
            RenewBrokerRequest.newBuilder()
                .setClusterId("demo")
                .setBrokerId("broker-1")
                .addPartitions(partition)
                .build());
    final PlacementDriverApiRequest<BrokerRegistrationRecord> deregistration =
        mapper.toDeregisterBrokerRequest(
            DeregisterBrokerRequest.newBuilder()
                .setClusterId("demo")
                .setBrokerId("broker-1")
                .build());

    final BrokerRegistrationRecord registered =
        assertInstanceOf(BrokerRegistrationRecord.class, registration.getRequestWriter());
    final BrokerRegistrationRecord renewed =
        assertInstanceOf(BrokerRegistrationRecord.class, renewal.getRequestWriter());
    final BrokerRegistrationRecord deregistered =
        assertInstanceOf(BrokerRegistrationRecord.class, deregistration.getRequestWriter());
    assertEquals(BrokerIntent.REGISTER, registration.getIntent());
    assertEquals("127.0.0.1", registered.getHost());
    assertEquals(26601, registered.getRaftPort());
    assertEquals(45_000, registered.getRenewalTimeoutMillis());
    assertEquals(1, registered.getPartitions().size());
    assertEquals("op-1", registered.getPartitions().getFirst().getLastOpId());
    assertEquals(BrokerIntent.RENEW, renewal.getIntent());
    assertEquals(3, renewed.getPartitions().getFirst().getPartitionId());
    assertEquals(BrokerIntent.DEREGISTER, deregistration.getIntent());
    assertEquals("broker-1", deregistered.getBrokerId());
  }

  @Test
  void registryRequestMapperConvertsWorkerProtocolRequestsToCommands() {
    final PlacementDriverRequestMapper mapper =
        new PlacementDriverRequestMapper(PlacementDriverProcessors.createEventRegistry());
    final RegisterWorkerRequest registrationRequest =
        RegisterWorkerRequest.newBuilder()
            .setClusterId("demo")
            .addTenantIds("tenant-a")
            .setWorkerId("worker-1")
            .setName("payments")
            .setJobType("payment")
            .setMaxJobsActive(32)
            .setActiveJobs(3)
            .setConcurrency(4)
            .setTimeoutMillis(30_000)
            .setRequestTimeoutMillis(10_000)
            .setPollIntervalMillis(100)
            .addFetchVariables("orderId")
            .setClientVersion("0.1.0")
            .setRenewalTimeoutMillis(45_000)
            .build();
    final PlacementDriverApiRequest<WorkerRegistrationRecord> registration =
        mapper.toRegisterWorkerRequest(registrationRequest);
    final PlacementDriverApiRequest<WorkerRegistrationRecord> renewal =
        mapper.toRenewWorkerRequest(
            RenewWorkerRequest.newBuilder()
                .setClusterId(registrationRequest.getClusterId())
                .addAllTenantIds(registrationRequest.getTenantIdsList())
                .setWorkerId(registrationRequest.getWorkerId())
                .setName(registrationRequest.getName())
                .setJobType(registrationRequest.getJobType())
                .setMaxJobsActive(registrationRequest.getMaxJobsActive())
                .setActiveJobs(registrationRequest.getActiveJobs())
                .setConcurrency(registrationRequest.getConcurrency())
                .setTimeoutMillis(registrationRequest.getTimeoutMillis())
                .setRequestTimeoutMillis(registrationRequest.getRequestTimeoutMillis())
                .setPollIntervalMillis(registrationRequest.getPollIntervalMillis())
                .addAllFetchVariables(registrationRequest.getFetchVariablesList())
                .setClientVersion(registrationRequest.getClientVersion())
                .setRenewalTimeoutMillis(registrationRequest.getRenewalTimeoutMillis())
                .build());
    final PlacementDriverApiRequest<WorkerRegistrationRecord> deregistration =
        mapper.toDeregisterWorkerRequest(
            DeregisterWorkerRequest.newBuilder()
                .setClusterId("demo")
                .addTenantIds("tenant-a")
                .setWorkerId("worker-1")
                .setName("payments")
                .build());

    final WorkerRegistrationRecord registered =
        assertInstanceOf(WorkerRegistrationRecord.class, registration.getRequestWriter());
    final WorkerRegistrationRecord renewed =
        assertInstanceOf(WorkerRegistrationRecord.class, renewal.getRequestWriter());
    final WorkerRegistrationRecord deregistered =
        assertInstanceOf(WorkerRegistrationRecord.class, deregistration.getRequestWriter());
    assertEquals(WorkerIntent.REGISTER, registration.getIntent());
    assertEquals("payment", registered.getJobType());
    assertEquals(List.of("orderId"), registered.getFetchVariables());
    assertEquals(WorkerIntent.RENEW, renewal.getIntent());
    assertEquals(3, renewed.getActiveJobs());
    assertEquals(WorkerIntent.DEREGISTER, deregistration.getIntent());
    assertEquals(List.of("tenant-a"), deregistered.getTenantIds());
  }

  @Test
  void registryResponseMapperUsesCommittedBrokerAndWorkerRecords() {
    final PlacementDriverResponseMapper mapper = new PlacementDriverResponseMapper();
    final PlacementDriverApiResponse<BrokerRegistrationRecord> broker =
        new PlacementDriverApiResponse<>(
            new BrokerRegistrationRecord()
                .setClusterId("committed-cluster")
                .setBrokerId("committed-broker"),
            1);
    final PlacementDriverApiResponse<WorkerRegistrationRecord> worker =
        new PlacementDriverApiResponse<>(
            new WorkerRegistrationRecord()
                .setClusterId("committed-cluster")
                .setTenantIds(List.of("tenant-a"))
                .setWorkerId("committed-worker")
                .setName("committed-name"),
            2);

    assertEquals("committed-broker", mapper.toRegisterBrokerResponse(broker).getBrokerId());
    assertEquals("committed-broker", mapper.toRenewBrokerResponse(broker).getBrokerId());
    assertEquals("committed-broker", mapper.toDeregisterBrokerResponse(broker).getBrokerId());
    assertEquals("committed-worker", mapper.toRegisterWorkerResponse(worker).getWorkerId());
    assertEquals("committed-worker", mapper.toRenewWorkerResponse(worker).getWorkerId());
    assertEquals("committed-worker", mapper.toDeregisterWorkerResponse(worker).getWorkerId());
  }

  @Test
  void jobSubscriptionMirrorResponsesPreserveCreationAndErrorContracts() {
    final PlacementDriverResponseMapper mapper = new PlacementDriverResponseMapper();
    final JobSubscriptionMirrorRecord mirror =
        new JobSubscriptionMirrorRecord()
            .setMirrorKey(9001)
            .setSourceClusterId("region-a")
            .setTargetClusterId("region-b")
            .setOutcome(JobSubscriptionMirrorCreationOutcome.CREATED);

    final var created =
        mapper.toCreateJobSubscriptionMirrorResult(new PlacementDriverApiResponse<>(mirror, 9001));
    final var createdView =
        assertInstanceOf(PlacementDriverHttpModel.JobSubscriptionMirrorView.class, created.body());
    assertEquals(201, created.status());
    assertEquals(true, createdView.created());
    assertEquals(JobSubscriptionMirrorStatus.ACTIVE, createdView.status());

    mirror.setOutcome(JobSubscriptionMirrorCreationOutcome.EXISTS);
    final var existing =
        mapper.toCreateJobSubscriptionMirrorResult(new PlacementDriverApiResponse<>(mirror, 9001));
    final var existingView =
        assertInstanceOf(PlacementDriverHttpModel.JobSubscriptionMirrorView.class, existing.body());
    assertEquals(200, existing.status());
    assertEquals(false, existingView.created());

    final var error =
        mapper.toHttpError(
            new PlacementDriverRejectionException(
                RejectionType.NOT_FOUND, "SOURCE_CLUSTER_NOT_FOUND: cluster `region-a`"),
            "region-a");
    final var errorBody =
        assertInstanceOf(PlacementDriverHttpModel.AdminOperationErrorResponse.class, error.body());
    assertEquals(404, error.status());
    assertEquals("SOURCE_CLUSTER_NOT_FOUND", errorBody.error());
  }

  @Test
  void jobSubscriptionDistributionResponsePreservesTargetProgress() {
    final CommandDistributionQueryRecord distribution =
        new CommandDistributionQueryRecord()
            .setDistributionKey(1001)
            .setOriginClusterId("region-a")
            .setClusterId("region-a")
            .setCreationType(JobSubscriptionCreationType.SUBSCRIPTION)
            .setValueType("JOB_SUBSCRIPTION")
            .setStatus(JobSubscriptionDistributionStatus.DISTRIBUTING)
            .setTargets(
                List.of(
                    new CommandDistributionQueryRecord.Target()
                        .setClusterId("region-b")
                        .setExpectedPartitions(3)
                        .setAcknowledgedPartitions(2)
                        .setPendingPartitions(List.of(3))
                        .setStatus(JobSubscriptionDistributionStatus.DISTRIBUTING)));

    final var result =
        new PlacementDriverResponseMapper()
            .toJobSubscriptionDistributionResult(
                new PlacementDriverApiResponse<>(distribution, 1001));
    final var response =
        assertInstanceOf(
            PlacementDriverHttpModel.JobSubscriptionDistributionView.class, result.body());

    assertEquals(200, result.status());
    assertEquals(1001, response.distributionKey());
    assertEquals("region-a", response.sourceClusterId());
    assertEquals("region-a", response.clusterId());
    assertEquals(JobSubscriptionCreationType.SUBSCRIPTION, response.creationType());
    assertEquals(JobSubscriptionDistributionStatus.DISTRIBUTING, response.status());
    assertEquals(List.of(3), response.targets().getFirst().pendingPartitions());
  }

  @Test
  void responseMapperPreservesExecuteCommandContract() {
    final TopologyQueryRecord value =
        new TopologyQueryRecord()
            .setClusterId("demo")
            .setQueryType(QueryType.BROKER_CLUSTER_CONFIGURATION)
            .setResponseValue("{\"clusterId\":\"demo\"}");
    final LoggedEvent response =
        LoggedEvent.ofRecord(
            42,
            new RecordMetadata()
                .recordType(RecordType.EVENT)
                .valueType(ValueType.TOPOLOGY)
                .intent(TopologyIntent.QUERIED),
            value);

    final byte[] encoded = new PlacementDriverResponseMapper().toCommandResponse(response);
    final ExecuteCommandResponse decoded = new ExecuteCommandResponse();
    decoded.wrap(new UnsafeBuffer(encoded), 0, encoded.length);
    final TopologyQueryRecord decodedValue = new TopologyQueryRecord();
    decodedValue.wrap(decoded.getValue(), 0, decoded.getValue().capacity());

    assertEquals(42, decoded.getKey());
    assertEquals(RecordType.EVENT, decoded.getRecordType());
    assertEquals(ValueType.TOPOLOGY, decoded.getValueType());
    assertEquals(TopologyIntent.QUERIED, decoded.getIntent());
    assertEquals(value.getResponseValue(), decodedValue.getResponseValue());
  }

  @Test
  void healthResponseUsesRequestingNodeSnapshot() {
    final MemberTopologyRecord topology =
        new MemberTopologyRecord()
            .setLocalMemberId("meta-1")
            .setLeaderId("meta-0")
            .setLeaderEndpoint("leader.example:29000")
            .setTerm(7)
            .setMembers(
                List.of(
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-0")
                        .setRole(MemberRole.LEADER),
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-1")
                        .setRole(MemberRole.FOLLOWER)
                        .setSelf(true)));
    final TopologyQueryRecord query =
        new TopologyQueryRecord()
            .setResponseValue(topology.toJson())
            .setRequestingHost("follower.example")
            .setRequestingHttpPort(19001)
            .setRequestingGrpcPort(29001)
            .setRequestingCommitIndex(12)
            .setRequestingLastApplied(11)
            .setRequestingLastLogIndex(13);

    final var result =
        new PlacementDriverResponseMapper()
            .toHealthResult(new PlacementDriverApiResponse<>(query, 1));
    final var health =
        assertInstanceOf(PlacementDriverHttpModel.HealthResponse.class, result.body());

    assertEquals("follower.example", health.host());
    assertEquals(19001, health.port());
    assertEquals(29001, health.grpcPort());
    assertEquals(12, health.commitIndex());
    assertEquals(11, health.lastApplied());
    assertEquals(13, health.lastLogIndex());
  }

  @Test
  void placementDriverTopologyResponseIncludesMembersAndLeader() {
    final MemberTopologyRecord topology =
        new MemberTopologyRecord()
            .setLocalMemberId("meta-1")
            .setLeaderId("meta-0")
            .setLeaderEndpoint("leader.example:29000")
            .setTerm(7)
            .setMembers(
                List.of(
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-0")
                        .setEndpoint("leader.example:29000")
                        .setRole(MemberRole.LEADER),
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId("meta-1")
                        .setEndpoint("follower.example:29001")
                        .setRole(MemberRole.FOLLOWER)
                        .setSelf(true)));
    final TopologyQueryRecord query = new TopologyQueryRecord().setResponseValue(topology.toJson());

    final var result =
        new PlacementDriverResponseMapper()
            .toPlacementDriverTopologyResult(new PlacementDriverApiResponse<>(query, 1));
    final var response =
        assertInstanceOf(
            PlacementDriverHttpModel.PlacementDriverTopologyResponse.class, result.body());

    assertEquals("meta-1", response.localMemberId());
    assertEquals("meta-0", response.leaderId());
    assertEquals(7, response.term());
    assertEquals(2, response.members().size());
    assertEquals(true, response.members().get(1).self());
  }

  @Test
  void placementDriverConfigurationResponseSeparatesTargetAndActual() {
    final MemberConfigurationRecord configuration =
        new MemberConfigurationRecord()
            .setEpoch(8)
            .setTargetCount(4)
            .setTargetVoters(List.of("meta-0", "meta-1", "meta-2", "meta-3"))
            .setActualVoters(List.of("meta-0", "meta-1", "meta-2"))
            .setActualLearners(List.of("meta-3"))
            .setLeaderId("meta-0")
            .setTerm(42)
            .setActiveChangeId("change-1");
    final TopologyQueryRecord query =
        new TopologyQueryRecord().setResponseValue(configuration.toJson());

    final var result =
        new PlacementDriverResponseMapper()
            .toPlacementDriverConfigurationResult(new PlacementDriverApiResponse<>(query, 1));
    final var response =
        assertInstanceOf(
            PlacementDriverHttpModel.PlacementDriverConfigurationResponse.class, result.body());

    assertEquals(4, response.target().count());
    assertEquals(List.of("meta-3"), response.actual().learners());
    assertEquals("change-1", response.activeChangeId());
  }

  @Test
  void placementDriverChangeResponseIncludesCurrentMembership() {
    final MemberChangeOperation completedOperation =
        new MemberChangeOperation()
            .setType(MemberChangeOperationType.MEMBER_ADD_LEARNER)
            .setMemberId("meta-3")
            .setStatus(MemberChangeOperationStatus.COMPLETED);
    final MemberChangeOperation operation =
        new MemberChangeOperation()
            .setType(MemberChangeOperationType.MEMBER_PROMOTE_VOTER)
            .setMemberId("meta-3")
            .setStatus(MemberChangeOperationStatus.IN_PROGRESS)
            .setBarrierIndex(100);
    final MemberChangeRecord change =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.PLACEMENT_DRIVER)
            .setResourceId(PlacementDriverCluster.CLUSTER_ID)
            .setChangeId("change-1")
            .setSourceEpoch(7)
            .setTargetEpoch(8)
            .setTargetCount(4)
            .setTargetVoters(List.of("meta-0", "meta-1", "meta-2", "meta-3"))
            .setStatus(MemberChangeStatus.WAITING_CATCH_UP)
            .setNextOperationIndex(1)
            .setOperations(List.of(completedOperation, operation));
    final MemberChangeResponseRecord responseRecord =
        new MemberChangeResponseRecord()
            .setChange(change)
            .setActualVoters(List.of("meta-0", "meta-1", "meta-2"))
            .setActualLearners(List.of("meta-3"));
    final TopologyQueryRecord query =
        new TopologyQueryRecord().setResponseValue(responseRecord.toJson());

    final var result =
        new PlacementDriverResponseMapper()
            .toPlacementDriverMemberChangeStatusResult(new PlacementDriverApiResponse<>(query, 1));
    final var response =
        assertInstanceOf(
            PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.class,
            result.body());

    assertEquals(MemberChangeStatus.WAITING_CATCH_UP, response.status());
    assertEquals(List.of("meta-3"), response.actualLearners());
    assertEquals(
        MemberChangeOperationType.MEMBER_PROMOTE_VOTER, response.currentOperation().type());
    assertEquals(100, response.currentOperation().barrierIndex());
    assertEquals(2, response.plannedChanges().size());
    assertEquals(response.plannedChanges(), response.operations());
    assertEquals(1, response.pendingChanges().size());
    assertEquals(
        MemberChangeOperationType.MEMBER_PROMOTE_VOTER,
        response.pendingChanges().getFirst().type());
  }

  @Test
  void completedPlacementDriverChangeRetainsPlanAndClearsPendingChanges() {
    final MemberChangeOperation operation =
        new MemberChangeOperation()
            .setType(MemberChangeOperationType.MEMBER_PROMOTE_VOTER)
            .setMemberId("meta-3")
            .setStatus(MemberChangeOperationStatus.COMPLETED);
    final MemberChangeRecord change =
        new MemberChangeRecord()
            .setScope(MemberChangeScope.PLACEMENT_DRIVER)
            .setResourceId(PlacementDriverCluster.CLUSTER_ID)
            .setChangeId("change-1")
            .setStatus(MemberChangeStatus.COMPLETED)
            .setNextOperationIndex(1)
            .setOperations(List.of(operation));
    final TopologyQueryRecord query =
        new TopologyQueryRecord()
            .setResponseValue(new MemberChangeResponseRecord().setChange(change).toJson());

    final var result =
        new PlacementDriverResponseMapper()
            .toPlacementDriverMemberChangeStatusResult(new PlacementDriverApiResponse<>(query, 1));
    final var response =
        assertInstanceOf(
            PlacementDriverHttpModel.PlacementDriverMemberChangeStatusResponse.class,
            result.body());

    assertEquals(1, response.plannedChanges().size());
    assertEquals(
        MemberChangeOperationType.MEMBER_PROMOTE_VOTER,
        response.plannedChanges().getFirst().type());
    assertEquals(List.of(), response.pendingChanges());
    assertNull(response.currentOperation());
  }

  @Test
  void responseMapperPreservesNotLeaderHint() {
    final Status status =
        new PlacementDriverResponseMapper()
            .toGrpcStatus(new PlacementDriverNotLeaderException("127.0.0.1:26500"));

    assertEquals(Status.Code.FAILED_PRECONDITION, status.getCode());
    assertEquals("NOT_LEADER:127.0.0.1:26500", status.getDescription());
  }
}
