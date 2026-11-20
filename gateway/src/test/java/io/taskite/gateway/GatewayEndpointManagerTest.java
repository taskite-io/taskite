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
package io.taskite.gateway;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerExecuteCommand;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.gateway.api.GatewayEndpointManager;
import io.taskite.gateway.api.grpc.GatewayInterceptorTestContext;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.common.GatewayClusterResolver;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.ActivateJobsResponse;
import io.taskite.gateway.protocol.ActivatedJob;
import io.taskite.gateway.protocol.CancelJobRequest;
import io.taskite.gateway.protocol.CancelJobResponse;
import io.taskite.gateway.protocol.CompleteJobRequest;
import io.taskite.gateway.protocol.CompleteJobResponse;
import io.taskite.gateway.protocol.CreateJobRequest;
import io.taskite.gateway.protocol.CreateJobResponse;
import io.taskite.gateway.protocol.CreateJobSubscriptionRequest;
import io.taskite.gateway.protocol.CreateJobSubscriptionResponse;
import io.taskite.gateway.protocol.FailJobRequest;
import io.taskite.gateway.protocol.FailJobResponse;
import io.taskite.gateway.protocol.QueryIncidentRequest;
import io.taskite.gateway.protocol.QueryIncidentResponse;
import io.taskite.gateway.protocol.QueryJobRequest;
import io.taskite.gateway.protocol.ResolveIncidentRequest;
import io.taskite.gateway.protocol.ResolveIncidentResponse;
import io.taskite.gateway.protocol.SetVariablesRequest;
import io.taskite.gateway.protocol.SetVariablesResponse;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.gateway.routing.GatewayTopologyCache;
import io.taskite.gateway.routing.GatewayTopologyManager;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.client.TopologyChangeListener;
import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.DeregisterBrokerResponse;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerResponse;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.DistributeCommandResponse;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerResponse;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerResponse;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerResponse;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerResponse;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.VariableDocumentRecord;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.VariableDocumentIntent;
import io.taskite.protocol.record.value.broker.ErrorType;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ActorSchedulingService;
import io.taskite.scheduler.SchedulingHints;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

final class GatewayEndpointManagerTest {
  private static final String CLUSTER_ID = "demo";
  private static final String TENANT_ID = "tenant-a";
  private static ActorScheduler requestScheduler;

  @BeforeAll
  static void startRequestScheduler() {
    requestScheduler = ActorScheduler.newActorScheduler().setCpuBoundActorThreadCount(4).build();
    requestScheduler.start();
  }

  @AfterAll
  static void stopRequestScheduler() {
    requestScheduler.stop().join();
  }

  @Test
  void submitsDedicatedActorForEveryRequest() {
    final RecordingActorSchedulingService scheduler = new RecordingActorSchedulingService();
    final GatewayTopologyCache topologyCache = new GatewayTopologyCache();
    final GatewayEndpointManager manager =
        new GatewayEndpointManager(
            new GatewayBrokerCommandDispatcher(
                new CapturingBrokerClient(), new GatewayBrokerRouter(topologyCache)),
            NoopPlacementDriverClient.INSTANCE,
            ignored -> CompletableActorFuture.completed(topology(1, PartitionStatus.ACTIVE, 0)),
            scheduler);

    manager.completeJob(CompleteJobRequest.getDefaultInstance(), new CapturingObserver<>());
    manager.queryJob(QueryJobRequest.getDefaultInstance(), new CapturingObserver<>());

    assertEquals(2, scheduler.actors.size());
    assertNotSame(scheduler.actors.get(0), scheduler.actors.get(1));
  }

  @Test
  void typeOnlyBatchActivationVisitsTopologyPartitionsAndRotatesPollingStart() throws Exception {
    final var broker = new CapturingBrokerClient();
    broker.expectedFetchVariables = List.of();
    final var manager = endpointManager(broker, batchTopology());
    final var request = ActivateJobsRequest.newBuilder().setType("payment").build();
    final var first = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(request, first));
    assertTrue(first.await());
    assertNull(first.error);
    assertEquals(List.of(1, 2, 3), broker.batchPartitions);
    assertEquals(List.of(100, 99, 98), broker.batchLimits);
    assertEquals(3, first.values.stream().mapToInt(ActivateJobsResponse::getJobsCount).sum());
    assertTrue(
        first.values.stream()
            .flatMap(v -> v.getJobsList().stream())
            .allMatch(j -> TENANT_ID.equals(j.getTenantId())));
    broker.batchPartitions.clear();
    final var second = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(request, second));
    assertTrue(second.await());
    assertNull(second.error);
    assertEquals(List.of(2, 3, 1), broker.batchPartitions);
  }

  @Test
  void typeOnlyBatchActivationReturnsEmptyResponseWhenNoJobsMatch() throws Exception {
    final var broker = new CapturingBrokerClient();
    broker.expectedFetchVariables = List.of();
    broker.batchJobs = 0;
    final var manager = endpointManager(broker, batchTopology());
    final var observer = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(
        () ->
            manager.activateJobs(
                ActivateJobsRequest.newBuilder().setType("missing").setRequestTimeout(-1).build(),
                observer));
    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(List.of(1, 2, 3), broker.batchPartitions);
    assertEquals(List.of(ActivateJobsResponse.getDefaultInstance()), observer.values);
  }

  @Test
  void typeOnlyBatchActivationUsesActualPartitionIdInsteadOfIndex() throws Exception {
    final var broker = new CapturingBrokerClient();
    broker.expectedFetchVariables = List.of();
    final var manager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(
        () ->
            manager.activateJobs(
                ActivateJobsRequest.newBuilder().setType("payment").build(), observer));
    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(List.of(7), broker.batchPartitions);
    assertEquals(5, broker.routingEpoch);
  }

  @Test
  void batchActivationVisitsPartitionsWithRemainingLimitAndRotatesStart() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, batchTopology());
    final var first = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(batchRequest(2), first));
    assertTrue(first.await());
    assertNull(first.error);
    assertEquals(List.of(1, 2), broker.batchPartitions);
    assertEquals(List.of(2, 1), broker.batchLimits);
    assertEquals(2, first.values.stream().mapToInt(ActivateJobsResponse::getJobsCount).sum());
    assertEquals(TENANT_ID, first.value.getJobs(0).getTenantId());
    assertEquals("payment", first.value.getJobs(0).getType());
    assertEquals("worker", first.value.getJobs(0).getWorker());
    assertEquals(12345, first.value.getJobs(0).getDeadline());
    assertEquals("{\"amount\":42}", first.value.getJobs(0).getVariables());
    broker.batchPartitions.clear();
    final var second = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(batchRequest(1), second));
    assertTrue(second.await());
    assertEquals(List.of(2), broker.batchPartitions);
  }

  @Test
  void batchActivationScansEmptyPartitionsAndReturnsEmptyResponse() throws Exception {
    final var broker = new CapturingBrokerClient();
    broker.batchJobs = 0;
    final var manager = endpointManager(broker, batchTopology());
    final var observer = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(batchRequest(3), observer));
    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(List.of(1, 2, 3), broker.batchPartitions);
    assertEquals(1, observer.values.size());
    assertEquals(0, observer.value.getJobsCount());
  }

  @Test
  void batchActivationRejectsInvalidRequestsBeforeDispatch() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, batchTopology());
    for (final var request :
        List.of(
            batchRequest(-1),
            batchRequest(1).toBuilder().setTimeout(-1).build(),
            batchRequest(1).toBuilder().setType(" ").build())) {
      final var observer = new CapturingObserver<ActivateJobsResponse>();
      runWithRequestContext(() -> manager.activateJobs(request, observer));
      assertTrue(observer.await());
      assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(observer.error).getCode());
    }
    assertTrue(broker.batchPartitions.isEmpty());
  }

  @Test
  void batchActivationPropagatesPartitionFailureAfterDeliveringCommittedBatch() throws Exception {
    final var broker = new CapturingBrokerClient();
    broker.failingPartition = 2;
    final var manager = endpointManager(broker, batchTopology());
    final var observer = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(batchRequest(3), observer));
    assertTrue(observer.await());
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
    assertEquals(List.of(1, 2), broker.batchPartitions);
    assertEquals(1, observer.values.size());
    assertEquals(1, observer.value.getJobsCount());
  }

  @Test
  void batchActivationStopsAfterClientCancels() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, batchTopology());
    final var observer = new CapturingObserver<ActivateJobsResponse>();
    final AtomicReference<Context.CancellableContext> cancellation = new AtomicReference<>();
    runWithRequestContext(
        () -> {
          final var context = Context.current().withCancellation();
          cancellation.set(context);
          observer.afterNext = () -> context.cancel(null);
          context.run(() -> manager.activateJobs(batchRequest(3), observer));
        });
    try {
      assertTrue(observer.await());
      assertEquals(Status.Code.CANCELLED, Status.fromThrowable(observer.error).getCode());
      assertEquals(List.of(1), broker.batchPartitions);
    } finally {
      cancellation.get().close();
    }
  }

  @Test
  void batchActivationRequiresMetadataAndReturnsEmptyWhenNoPartitionIsReady() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, topology(1, PartitionStatus.REBALANCING, 0));
    final var missing = new CapturingObserver<ActivateJobsResponse>();
    manager.activateJobs(batchRequest(1), missing);
    assertTrue(missing.await());
    assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(missing.error).getCode());
    final var unavailable = new CapturingObserver<ActivateJobsResponse>();
    runWithRequestContext(() -> manager.activateJobs(batchRequest(1), unavailable));
    assertTrue(unavailable.await());
    assertNull(unavailable.error);
    assertEquals(List.of(ActivateJobsResponse.getDefaultInstance()), unavailable.values);
    assertTrue(broker.batchPartitions.isEmpty());
  }

  private static ActivateJobsRequest batchRequest(int count) {
    return ActivateJobsRequest.newBuilder()
        .setType("payment")
        .setWorker("worker")
        .setTimeout(1000)
        .setMaxJobsToActivate(count)
        .addFetchVariable("amount")
        .setRequestTimeout(-1)
        .build();
  }

  private static ClusterTopology batchTopology() {
    final var partitions = new ArrayList<Partition>();
    final var endpoints = new HashMap<String, BrokerEndpoint>();
    for (int id = 1; id <= 3; id++) {
      final var topology = topology(id, PartitionStatus.ACTIVE, 5);
      partitions.addAll(topology.partitions());
      endpoints.putAll(topology.brokers());
    }
    return new ClusterTopology(CLUSTER_ID, 1, 3, 1, partitions, endpoints, Instant.EPOCH);
  }

  @Test
  void jobCreationUsesReadyPartitionAndMetadata() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            manager.createJob(
                CreateJobRequest.newBuilder()
                    .setJobType("payment")
                    .setVariables("{\"amount\":10}")
                    .build(),
                observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(Map.of("amount", 10), broker.jobRequest.getVariables());
    assertEquals(0, observer.value.getSubscriptionKey());
    assertEquals("payment", observer.value.getJobType());
    assertEquals(101, observer.value.getJobKey());
  }

  @Test
  void invalidJobVariablesAreRejectedBeforeBrokerDispatch() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();
    runWithRequestContext(
        () ->
            manager.createJob(
                CreateJobRequest.newBuilder().setJobType("payment").setVariables("[]").build(),
                observer));

    assertTrue(observer.await());
    assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(observer.error).getCode());
    assertNull(broker.endpoint);
  }

  @Test
  void jobCreationRequiresClusterContext() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();
    manager.createJob(CreateJobRequest.newBuilder().setJobType("payment").build(), observer);

    assertTrue(observer.await());
    assertEquals(Status.Code.FAILED_PRECONDITION, Status.fromThrowable(observer.error).getCode());
    assertNull(broker.endpoint);
  }

  @Test
  void jobCreationDoesNotDispatchWhenNoPartitionIsReady() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final var manager = endpointManager(broker, topology(7, PartitionStatus.REBALANCING, 0));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();
    runWithRequestContext(
        () ->
            manager.createJob(
                CreateJobRequest.newBuilder().setJobType("payment").build(), observer));

    assertFalse(observer.completed.await(100, TimeUnit.MILLISECONDS));
    assertNull(broker.endpoint);
    assertTrue(observer.completed.await(12, TimeUnit.SECONDS));
    assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
    assertNull(broker.endpoint);
  }

  @Test
  void keylessJobCreationUsesRequestDispatchStrategy() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final GatewayEndpointManager endpointManager =
        endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            endpointManager.createJob(
                CreateJobRequest.newBuilder()
                    .setJobType("payment")
                    .setVariables("{\"deadline\":5}")
                    .build(),
                observer));

    assertTrue(observer.await(), "CreateJob response was not completed");
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals("broker-7", broker.endpoint.brokerId());
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(101L, observer.value.getJobKey());
  }

  @Test
  void keyedRequestUsesPartitionFromKey() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final GatewayEndpointManager endpointManager =
        endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CompleteJobResponse> observer = new CapturingObserver<>();
    final long jobKey = Protocol.encodePartitionId(7, 42);

    runWithRequestContext(
        () ->
            endpointManager.completeJob(
                CompleteJobRequest.newBuilder()
                    .setJobKey(jobKey)
                    .setVariables("{\"deadline\":5}")
                    .build(),
                observer));

    assertTrue(observer.await(), "CompleteJob response was not completed");
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals("broker-7", broker.endpoint.brokerId());
    assertEquals(101L, observer.value.getJobKey());
  }

  @Test
  void failJobRoutesByJobKeyAndPropagatesPayloadAndTenant() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<FailJobResponse>();
    final long jobKey = Protocol.encodePartitionId(7, 42);

    runWithRequestContext(
        () ->
            endpointManager.failJob(
                FailJobRequest.newBuilder()
                    .setJobKey(jobKey)
                    .setRetries(2)
                    .setErrorMessage("temporary")
                    .setRetryBackOff(500)
                    .setVariables("{\"attempt\":2}")
                    .build(),
                observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(JobIntent.FAIL, broker.jobIntent);
    assertEquals(2, broker.jobRequest.getRetries());
    assertEquals("temporary", broker.jobRequest.getErrorMessage());
    assertEquals(500, broker.jobRequest.getRetryBackoff());
    assertEquals(Map.of("attempt", 2), broker.jobRequest.getVariables());
    assertEquals(0, observer.value.getSerializedSize());
  }

  @Test
  void jobCancellationRoutesByKeyAndPropagatesTenant() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<CancelJobResponse>();
    final long processInstanceKey = Protocol.encodePartitionId(7, 42);

    runWithRequestContext(
        () ->
            endpointManager.cancelJob(
                CancelJobRequest.newBuilder().setJobKey(processInstanceKey).build(), observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(TENANT_ID, broker.jobRequest.getTenantId());
    assertEquals(processInstanceKey, observer.value.getJobKey());
  }

  @Test
  void queryJobRoutesByKeyAndReturnsActivatedJobDetails() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<ActivatedJob>();
    final long jobKey = Protocol.encodePartitionId(7, 42);

    runWithRequestContext(
        () ->
            endpointManager.queryJob(
                QueryJobRequest.newBuilder().setJobKey(jobKey).build(), observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(jobKey, observer.value.getKey());
    assertEquals("payment", observer.value.getType());
    assertEquals("worker-a", observer.value.getWorker());
    assertEquals("{\"deadline\":5}", observer.value.getVariables());
    assertEquals("tenant-a", observer.value.getTenantId());
  }

  @Test
  void queryIncidentRoutesByOwnerKeyAndPropagatesTenant() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<QueryIncidentResponse>();
    final long ownerKey = Protocol.encodePartitionId(7, 42);

    runWithRequestContext(
        () ->
            endpointManager.queryIncident(
                QueryIncidentRequest.newBuilder().setJobKey(ownerKey).build(), observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(IncidentIntent.QUERY, broker.incidentIntent);
    assertEquals(ownerKey + 1, observer.value.getKey());
    assertEquals(ownerKey, observer.value.getJobKey());
    assertEquals("JOB_NO_RETRIES", observer.value.getErrorType());
    assertEquals("tenant-a", observer.value.getTenantId());
  }

  @Test
  void resolveIncidentRoutesByIncidentKeyAndPropagatesTenant() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<ResolveIncidentResponse>();
    final long incidentKey = Protocol.encodePartitionId(7, 43);

    runWithRequestContext(
        () ->
            endpointManager.resolveIncident(
                ResolveIncidentRequest.newBuilder().setIncidentKey(incidentKey).build(), observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(IncidentIntent.RESOLVE, broker.incidentIntent);
    assertEquals(0, observer.value.getSerializedSize());
  }

  @Test
  void setVariablesRoutesByScopeKeyAndPropagatesPayloadAndTenant() throws Exception {
    final var broker = new CapturingBrokerClient();
    final var endpointManager = endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final var observer = new CapturingObserver<SetVariablesResponse>();
    final long scopeKey = Protocol.encodePartitionId(7, 44);

    runWithRequestContext(
        () ->
            endpointManager.setVariables(
                SetVariablesRequest.newBuilder()
                    .setJobKey(scopeKey)
                    .setVariables("{\"amount\":42}")
                    .build(),
                observer));

    assertTrue(observer.await());
    assertNull(observer.error);
    assertEquals(7, broker.partitionId);
    assertEquals(5, broker.routingEpoch);
    assertEquals(CLUSTER_ID, broker.clusterId);
    assertEquals(TENANT_ID, broker.tenantId);
    assertEquals(VariableDocumentIntent.UPDATE, broker.variableDocumentIntent);
    assertEquals(scopeKey, broker.variableDocumentRequest.getJobKey());
    assertEquals(Map.of("amount", 42), broker.variableDocumentRequest.getVariables());
    assertEquals(scopeKey, observer.value.getJobKey());
  }

  @Test
  void unknownPartitionSurfacesUnavailableWithExpectedMessage() throws Exception {
    final GatewayEndpointManager endpointManager =
        endpointManager(new CapturingBrokerClient(), topology(1, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CompleteJobResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            endpointManager.completeJob(
                CompleteJobRequest.newBuilder()
                    .setJobKey(Protocol.encodePartitionId(7, 42))
                    .build(),
                observer));

    assertTrue(observer.await(), "CompleteJob error was not completed");
    final StatusRuntimeException status = (StatusRuntimeException) observer.error;
    assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
    assertEquals(
        "Expected to execute command on partition 7, but either it does not exist, or the gateway is not yet aware of it",
        status.getStatus().getDescription());
  }

  @Test
  void knownPartitionWithoutServiceReadinessWaitsThenSurfacesUnavailable() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final GatewayEndpointManager endpointManager =
        endpointManager(broker, topology(1, PartitionStatus.REBALANCING, 0));
    final CapturingObserver<CompleteJobResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            endpointManager.completeJob(
                CompleteJobRequest.newBuilder()
                    .setJobKey(Protocol.encodePartitionId(1, 42))
                    .build(),
                observer));

    assertFalse(observer.completed.await(100, TimeUnit.MILLISECONDS));
    assertNull(broker.endpoint);
    assertTrue(
        observer.completed.await(12, TimeUnit.SECONDS), "CompleteJob error was not completed");
    final StatusRuntimeException status = (StatusRuntimeException) observer.error;
    assertEquals(Status.Code.UNAVAILABLE, status.getStatus().getCode());
    assertEquals(
        "Expected to route request to partition 1, but partition is not ready (status=REBALANCING)",
        status.getStatus().getDescription());
    assertNull(broker.endpoint);
  }

  @Test
  void stampsPartitionRoutingEpochOntoRequest() throws Exception {
    final CapturingBrokerClient broker = new CapturingBrokerClient();
    final GatewayEndpointManager endpointManager =
        endpointManager(broker, topology(7, PartitionStatus.ACTIVE, 5));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            endpointManager.createJob(
                CreateJobRequest.newBuilder().setJobType("payment").build(), observer));

    assertTrue(observer.await(), "CreateJob response was not completed");
    assertNull(observer.error);
    assertEquals(5L, broker.routingEpoch);
    assertEquals("demo", broker.clusterId);
  }

  @Test
  void placementDriverRequestResolvesCluster() throws Exception {
    final AtomicReference<String> resolvedCluster = new AtomicReference<>();
    final GatewayClusterResolver clusterResolver =
        clusterId -> {
          resolvedCluster.set(clusterId);
          return CompletableActorFuture.completed(topology(1, PartitionStatus.ACTIVE, 0));
        };
    final GatewayEndpointManager endpointManager =
        new GatewayEndpointManager(
            new GatewayBrokerCommandDispatcher(
                new CapturingBrokerClient(), new GatewayBrokerRouter(new GatewayTopologyCache())),
            NoopPlacementDriverClient.INSTANCE,
            clusterResolver,
            requestScheduler);
    final CapturingObserver<CreateJobSubscriptionResponse> observer = new CapturingObserver<>();

    runWithRequestContext(
        () ->
            endpointManager.createJobSubscription(
                CreateJobSubscriptionRequest.newBuilder()
                    .setName("payment")
                    .setJobType("payment")
                    .build(),
                observer));

    assertTrue(observer.await(), "CreateJobSubscription error was not completed");
    assertEquals(CLUSTER_ID, resolvedCluster.get());
  }

  @Test
  void missingClusterContextIsRejected() throws Exception {
    final GatewayEndpointManager endpointManager =
        endpointManager(new CapturingBrokerClient(), topology(1, PartitionStatus.ACTIVE, 0));
    final CapturingObserver<CreateJobResponse> observer = new CapturingObserver<>();

    endpointManager.createJob(
        CreateJobRequest.newBuilder().setJobType("payment").build(), observer);

    assertTrue(observer.await(), "CreateJob error was not completed");
    final StatusRuntimeException status = (StatusRuntimeException) observer.error;
    assertEquals(Status.Code.FAILED_PRECONDITION, status.getStatus().getCode());
    assertEquals("clusterId is missing from the gRPC context", status.getStatus().getDescription());
  }

  private static void runWithRequestContext(final Runnable requestAction) {
    GatewayInterceptorTestContext.run(CLUSTER_ID, TENANT_ID, requestAction);
  }

  private static GatewayEndpointManager endpointManager(
      final BrokerClient broker, final ClusterTopology topology) {
    final GatewayTopologyCache topologyCache = new GatewayTopologyCache();
    topologyCache.update(topology);
    final GatewayTopologyManager topologyManager =
        new GatewayTopologyManager(
            Actor.newActor().build(),
            topologyCache,
            NoopPlacementDriverClient.INSTANCE,
            Duration.ofSeconds(1));
    return new GatewayEndpointManager(
        new GatewayBrokerCommandDispatcher(broker, new GatewayBrokerRouter(topologyCache)),
        NoopPlacementDriverClient.INSTANCE,
        topologyManager,
        requestScheduler);
  }

  private static ClusterTopology topology(
      final int partitionId, final PartitionStatus status, final long recoveryEpoch) {
    final BrokerEndpoint endpoint =
        new BrokerEndpoint(
            "broker-" + partitionId, "127.0.0.1", 26500 + partitionId, 26600 + partitionId);
    final Partition partition =
        new Partition(
            partitionId,
            new Partition.Leadership(endpoint.brokerId(), 1),
            new Partition.Membership(List.of(), List.of(), Map.of(), recoveryEpoch),
            status);
    return new ClusterTopology(
        "demo", 1, 1, 1, List.of(partition), Map.of(endpoint.brokerId(), endpoint), Instant.EPOCH);
  }

  private static final class CapturingBrokerClient implements BrokerClient {
    private final List<Integer> batchPartitions = new ArrayList<>();
    private final List<Integer> batchLimits = new ArrayList<>();
    private List<String> expectedFetchVariables = List.of("amount");
    private int batchJobs = 1;
    private int failingPartition = -1;
    private int partitionId;
    private long routingEpoch;
    private String clusterId;
    private String tenantId;
    private IncidentIntent incidentIntent;
    private JobIntent jobIntent;
    private JobRecord jobRequest;
    private VariableDocumentIntent variableDocumentIntent;
    private VariableDocumentRecord variableDocumentRequest;
    private BrokerEndpoint endpoint;

    @Override
    @SuppressWarnings("unchecked")
    public <T> ActorFuture<BrokerResponse<T>> sendRequest(
        final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
      this.endpoint = endpoint;
      partitionId = request.getPartitionId();
      routingEpoch = request.getRoutingEpoch();
      clusterId = request.getClusterId();
      if (request.getRequestWriter() instanceof JobBatchRecord batch) {
        batchPartitions.add(partitionId);
        batchLimits.add(batch.getMaxJobsToActivate());
        assertEquals(CLUSTER_ID, clusterId);
        assertEquals(TENANT_ID, batch.getTenantId());
        assertEquals(expectedFetchVariables, batch.getFetchVariables());
        assertEquals(5, routingEpoch);
        if (partitionId == failingPartition) {
          return CompletableActorFuture.completedExceptionally(
              Status.UNAVAILABLE.withDescription("partition unavailable").asRuntimeException());
        }
        final var response = new JobBatchRecord();
        if (batchJobs > 0) {
          response
              .getJobs()
              .add(
                  new JobBatchRecord.ActivatedJob(
                      Protocol.encodePartitionId(partitionId, 1),
                      new JobRecord()
                          .setTenantId(batch.getTenantId())
                          .setJobType(batch.getType())
                          .setWorker(batch.getWorker())
                          .setDeadline(12345)
                          .setVariablesJson("{\"amount\":42}")));
        }
        return CompletableActorFuture.completed(request.mapResponse(partitionId, 100, response));
      }
      if (request.getRequestWriter() instanceof IncidentRecord incident) {
        tenantId = incident.getTenantId();
        final var command = (BrokerExecuteCommand<?>) request;
        incidentIntent = (IncidentIntent) command.getIntent();
        final long ownerKey = command.getKey();
        final var response =
            new IncidentRecord()
                .setJobKey(ownerKey)
                .setErrorType(ErrorType.JOB_NO_RETRIES)
                .setErrorMessage("exhausted")
                .setTenantId(incident.getTenantId());
        return CompletableActorFuture.completed(
            new BrokerResponse<>((T) response, partitionId, ownerKey + 1));
      }
      if (request.getRequestWriter() instanceof VariableDocumentRecord document) {
        tenantId = document.getTenantId();
        variableDocumentRequest = document.copy();
        variableDocumentIntent =
            (VariableDocumentIntent) ((BrokerExecuteCommand<?>) request).getIntent();
        return CompletableActorFuture.completed(
            new BrokerResponse<>((T) document.copy(), partitionId, document.getJobKey() + 1));
      }
      final JobRecord requestJob = (JobRecord) request.getRequestWriter();
      tenantId = requestJob.getTenantId();
      jobRequest = requestJob.copy();
      jobIntent = (JobIntent) ((BrokerExecuteCommand<?>) request).getIntent();
      if (jobIntent == JobIntent.CREATE) {
        return CompletableActorFuture.completed(
            new BrokerResponse<>((T) jobRequest.copy(), partitionId, 101L));
      }
      final JobRecord record =
          new JobRecord()
              .setJobType("payment")
              .setWorker("worker-a")
              .setTenantId(tenantId)
              .setVariablesJson("{\"deadline\":5}");
      if (request instanceof BrokerExecuteCommand<?> command
          && (command.getIntent() == JobIntent.QUERY || command.getIntent() == JobIntent.CANCEL)) {
        return CompletableActorFuture.completed(
            new BrokerResponse<>((T) record, partitionId, command.getKey()));
      }
      return CompletableActorFuture.completed(new BrokerResponse<>((T) record, partitionId, 101L));
    }
  }

  private enum NoopPlacementDriverClient implements Client {
    INSTANCE;

    @Override
    public ActorFuture<ClusterConfig> clusterConfig(final GetClusterConfigRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<ClusterTopology> topology(final GetBrokerTopologyRequest request) {
      return unsupported();
    }

    @Override
    public AutoCloseable watchBrokerTopology(
        final GetBrokerTopologyRequest request, final TopologyChangeListener listener) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ActorFuture<RegisterBrokerResponse> registerBroker(final RegisterBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RenewBrokerResponse> renewBroker(final RenewBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DeregisterBrokerResponse> deregisterBroker(
        final DeregisterBrokerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RegisterWorkerResponse> registerWorker(final RegisterWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<RenewWorkerResponse> renewWorker(final RenewWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DeregisterWorkerResponse> deregisterWorker(
        final DeregisterWorkerRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<DistributeCommandResponse> distributeCommand(
        final DistributeCommandRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse>
        createTimerSubscription(
            final io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest request) {
      return unsupported();
    }

    @Override
    public ActorFuture<io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse>
        deleteTimerSubscription(
            final io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest request) {
      return unsupported();
    }

    @Override
    public void close() {}

    private static <T> ActorFuture<T> unsupported() {
      return CompletableActorFuture.completedExceptionally(new UnsupportedOperationException());
    }
  }

  private static final class CapturingObserver<T> implements StreamObserver<T> {
    private final CountDownLatch completed = new CountDownLatch(1);
    private final List<T> values = new ArrayList<>();
    private Runnable afterNext = () -> {};
    private T value;
    private Throwable error;

    @Override
    public void onNext(final T value) {
      this.value = value;
      values.add(value);
      afterNext.run();
    }

    @Override
    public void onError(final Throwable error) {
      this.error = error;
      completed.countDown();
    }

    @Override
    public void onCompleted() {
      completed.countDown();
    }

    private boolean await() throws InterruptedException {
      return completed.await(1, TimeUnit.SECONDS);
    }
  }

  private static final class RecordingActorSchedulingService implements ActorSchedulingService {
    private final List<Actor> actors = new ArrayList<>();

    @Override
    public ActorFuture<Void> submitActor(final Actor actor) {
      actors.add(actor);
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> submitActor(final Actor actor, final SchedulingHints schedulingHints) {
      return submitActor(actor);
    }
  }
}
