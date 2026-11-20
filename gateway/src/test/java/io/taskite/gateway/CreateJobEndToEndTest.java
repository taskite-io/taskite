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

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.taskite.broker.Broker;
import io.taskite.broker.BrokerConfig;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.interceptor.GatewayInterceptorConfig;
import io.taskite.gateway.protocol.*;
import io.taskite.placementdriver.PlacementDriver;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.client.GrpcClient;
import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.placementdriver.runtime.transport.InternalPlacementDriverCommandGrpc;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.encoding.ExecuteCommandRequest;
import io.taskite.protocol.impl.encoding.ExecuteCommandResponse;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Generated clients, real Gateway/PD/Broker servers, Raft and persisted task state. */
@Timeout(90)
public final class CreateJobEndToEndTest {
  private static final Metadata.Key<String> TENANT_HEADER =
      Metadata.Key.of("x-test-tenant", Metadata.ASCII_STRING_MARSHALLER);

  @Test
  void createsIndependentJobsAcrossPartitionsAndRecovery(@TempDir final Path directory)
      throws Exception {
    try (final var cluster = new TestCluster(directory, 3, true, false)) {
      final var client = cluster.client("tenant-a");
      assertStatus(
          Status.Code.NOT_FOUND,
          () -> client.createJob(CreateJobRequest.newBuilder().setJobType("charge").build()));
      cluster.createSubscription("tenant-a", "charge");
      assertStatus(
          Status.Code.NOT_FOUND,
          () ->
              cluster
                  .client("tenant-b")
                  .createJob(CreateJobRequest.newBuilder().setJobType("charge").build()));
      final var requests = new ArrayList<CreateJobRequest>();
      final var responses = new ArrayList<CreateJobResponse>();
      for (int partition = 1; partition <= 3; partition++) {
        final var request =
            CreateJobRequest.newBuilder()
                .setJobType("charge")
                .setVariables("{\"amount\":42,\"nested\":{\"ready\":true}}")
                .build();
        requests.add(request);
        responses.add(client.createJob(request));
      }
      cluster.restart();
      assertEquals(
          java.util.Set.of(1, 2, 3),
          responses.stream()
              .map(response -> Protocol.decodePartitionId(response.getJobKey()))
              .collect(java.util.stream.Collectors.toSet()));
      for (int i = 0; i < requests.size(); i++) {
        final var response = responses.get(i);
        assertNotEquals(response.getJobKey(), client.createJob(requests.get(i)).getJobKey());
        final var job =
            client.queryJob(QueryJobRequest.newBuilder().setJobKey(response.getJobKey()).build());
        assertEquals("tenant-a", job.getTenantId());
        assertEquals(requests.get(i).getVariables(), job.getVariables());
        assertStatus(
            Status.Code.NOT_FOUND,
            () ->
                cluster
                    .client("tenant-b")
                    .queryJob(QueryJobRequest.newBuilder().setJobKey(job.getKey()).build()));
        final var changed =
            client.createJob(requests.get(i).toBuilder().setVariables("{\"amount\":43}").build());
        assertNotEquals(response.getJobKey(), changed.getJobKey());
        assertEquals(
            "{\"amount\":43}",
            client
                .queryJob(QueryJobRequest.newBuilder().setJobKey(changed.getJobKey()).build())
                .getVariables());
      }
      assertStatus(
          Status.Code.INVALID_ARGUMENT,
          () ->
              client.createJob(
                  CreateJobRequest.newBuilder()
                      .setJobType("charge")
                      .setVariables("{} {}")
                      .build()));
    }
  }

  @Test
  void priorityOrderingAndActivationLeasesSurviveRestart(@TempDir final Path directory)
      throws Exception {
    try (final var cluster = new TestCluster(directory, 3, true, false)) {
      final var client = cluster.client("tenant-a");
      cluster.createSubscription("tenant-a", "work");
      final var byPartition = new HashMap<Integer, List<ActivatedJob>>();
      final var priorities = List.of(Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0);
      for (int i = 0; i < 12; i++) {
        final long key =
            client.createJob(CreateJobRequest.newBuilder().setJobType("work").build()).getJobKey();
        final int partition = Protocol.decodePartitionId(key);
        final var jobs = byPartition.computeIfAbsent(partition, ignored -> new ArrayList<>());
        final int priority = priorities.get(jobs.size() % priorities.size());
        client.updateJobPriority(
            UpdateJobPriorityRequest.newBuilder().setJobKey(key).setPriority(priority).build());
        jobs.add(client.queryJob(QueryJobRequest.newBuilder().setJobKey(key).build()));
      }
      assertEquals(3, byPartition.size());
      cluster.restart();
      for (final var jobs : byPartition.values()) {
        assertEquals(4, jobs.size());
        for (final var job : jobs) {
          assertEquals(
              job, client.queryJob(QueryJobRequest.newBuilder().setJobKey(job.getKey()).build()));
        }
      }
      final var activated =
          awaitActivated(
              client,
              ActivateJobsRequest.newBuilder()
                  .setType("work")
                  .setWorker("priority-worker")
                  .setTimeout(60_000)
                  .setRequestTimeout(-1)
                  .build(),
              12);
      final var observed = new HashMap<Integer, List<Integer>>();
      for (final var job : activated) {
        observed
            .computeIfAbsent(Protocol.decodePartitionId(job.getKey()), ignored -> new ArrayList<>())
            .add(job.getPriority());
      }
      for (final var order : observed.values()) {
        assertEquals(List.of(Integer.MAX_VALUE, 0, -1, Integer.MIN_VALUE), order);
      }
      final var first = activated.getFirst();
      client.updateJobTimeout(
          UpdateJobTimeoutRequest.newBuilder()
              .setJobKey(first.getKey())
              .setTimeout(120_000)
              .build());
      final var updated =
          client.queryJob(QueryJobRequest.newBuilder().setJobKey(first.getKey()).build());
      assertTrue(updated.getDeadline() > first.getDeadline());
      cluster.restart();
      assertEquals(
          updated, client.queryJob(QueryJobRequest.newBuilder().setJobKey(first.getKey()).build()));
      for (final var job : activated) {
        final var completed =
            client.completeJob(
                CompleteJobRequest.newBuilder()
                    .setJobKey(job.getKey())
                    .setVariables("{\"done\":true}")
                    .build());
        assertEquals(job.getKey(), completed.getJobKey());
        assertStatus(
            Status.Code.NOT_FOUND,
            () -> client.queryJob(QueryJobRequest.newBuilder().setJobKey(job.getKey()).build()));
      }
    }
  }

  @Test
  void retriesVariablesIncidentsAndCancellationUseOnlyJobKeys(@TempDir final Path directory)
      throws Exception {
    try (final var cluster = new TestCluster(directory, 1, true, false)) {
      final var client = cluster.client("tenant-a");
      cluster.createSubscription("tenant-a", "charge");
      final long key =
          client
              .createJob(
                  CreateJobRequest.newBuilder()
                      .setJobType("charge")
                      .setVariables("{\"amount\":42}")
                      .build())
              .getJobKey();
      assertEquals(
          key,
          client
              .setVariables(
                  SetVariablesRequest.newBuilder()
                      .setJobKey(key)
                      .setVariables("{\"attempt\":1}")
                      .build())
              .getJobKey());
      final var activation =
          ActivateJobsRequest.newBuilder()
              .setType("charge")
              .setTimeout(60_000)
              .setRequestTimeout(-1)
              .build();
      final var first = awaitActivated(client, activation, 1).getFirst();
      assertTrue(first.getVariables().contains("\"amount\":42"));
      assertTrue(first.getVariables().contains("\"attempt\":1"));
      client.failJob(
          FailJobRequest.newBuilder()
              .setJobKey(key)
              .setRetries(2)
              .setRetryBackOff(200)
              .setVariables("{\"attempt\":2}")
              .setErrorMessage("retry")
              .build());
      final var second = awaitActivated(client, activation, 1).getFirst();
      assertEquals(key, second.getKey());
      assertEquals(2, second.getRetries());
      assertTrue(second.getVariables().contains("\"attempt\":2"));
      client.failJob(
          FailJobRequest.newBuilder()
              .setJobKey(key)
              .setRetries(0)
              .setErrorMessage("exhausted")
              .build());
      final var incident =
          client.queryIncident(QueryIncidentRequest.newBuilder().setJobKey(key).build());
      assertEquals(key, incident.getJobKey());
      assertEquals("JOB_NO_RETRIES", incident.getErrorType());
      assertStatus(
          Status.Code.NOT_FOUND,
          () ->
              cluster
                  .client("tenant-b")
                  .queryIncident(QueryIncidentRequest.newBuilder().setJobKey(key).build()));
      cluster.restart();
      assertEquals(
          incident, client.queryIncident(QueryIncidentRequest.newBuilder().setJobKey(key).build()));
      client.updateJobRetries(
          UpdateJobRetriesRequest.newBuilder().setJobKey(key).setRetries(3).build());
      client.resolveIncident(
          ResolveIncidentRequest.newBuilder().setIncidentKey(incident.getKey()).build());
      final var recovered = awaitActivated(client, activation, 1).getFirst();
      assertEquals(3, recovered.getRetries());
      assertStatus(
          Status.Code.NOT_FOUND,
          () ->
              cluster
                  .client("tenant-b")
                  .cancelJob(CancelJobRequest.newBuilder().setJobKey(key).build()));
      assertEquals(
          key, client.cancelJob(CancelJobRequest.newBuilder().setJobKey(key).build()).getJobKey());
      assertStatus(
          Status.Code.NOT_FOUND,
          () -> client.queryJob(QueryJobRequest.newBuilder().setJobKey(key).build()));
    }
  }

  @Test
  void longPollingAndExplicitTenantFiltersActivateDirectJobs(@TempDir final Path directory)
      throws Exception {
    try (final var cluster = new TestCluster(directory, 3, true, false)) {
      final var client = cluster.client("tenant-a");
      cluster.createSubscription("tenant-a", "delayed");
      cluster.createSubscription("tenant-a", "shared");
      cluster.createSubscription("tenant-b", "shared");
      final var polling =
          CompletableFuture.supplyAsync(
              () ->
                  client
                      .activateJobs(
                          ActivateJobsRequest.newBuilder()
                              .setType("delayed")
                              .setRequestTimeout(10_000)
                              .setMaxJobsToActivate(1)
                              .build())
                      .next());
      final long key =
          client.createJob(CreateJobRequest.newBuilder().setJobType("delayed").build()).getJobKey();
      assertEquals(key, polling.get(15, TimeUnit.SECONDS).getJobs(0).getKey());
      for (final var tenant : List.of("tenant-a", "tenant-b")) {
        for (int partition = 1; partition <= 3; partition++) {
          cluster
              .client(tenant)
              .createJob(
                  CreateJobRequest.newBuilder()
                      .setJobType("shared")
                      .setVariables("{\"amount\":42}")
                      .build());
        }
      }
      final var selected =
          awaitActivated(
              cluster.client("tenant-a,tenant-b"),
              ActivateJobsRequest.newBuilder()
                  .setType("shared")
                  .addTenantIds("tenant-a")
                  .addTenantIds("tenant-b")
                  .addFetchVariable("amount")
                  .setRequestTimeout(-1)
                  .build(),
              6);
      assertEquals(
          3, selected.stream().filter(job -> job.getTenantId().equals("tenant-a")).count());
      assertEquals(
          3, selected.stream().filter(job -> job.getTenantId().equals("tenant-b")).count());
      assertStatus(
          Status.Code.PERMISSION_DENIED,
          () ->
              client
                  .activateJobs(
                      ActivateJobsRequest.newBuilder()
                          .setType("shared")
                          .addTenantIds("tenant-b")
                          .setRequestTimeout(-1)
                          .build())
                  .forEachRemaining(ignored -> {}));
    }
  }

  @Test
  void independentTimersCreateMultipleJobTypesAcrossRecoveryAndDeletion(
      @TempDir final Path directory) throws Exception {
    try (final var cluster = new TestCluster(directory, 3, true, false)) {
      final var client = cluster.client("tenant-a");
      final var request =
          CreateTimerSubscriptionRequest.newBuilder()
              .setName("scheduled-charge")
              .addJobTypes("scheduled")
              .addJobTypes("notify")
              .setVariables("{\"amount\":42,\"seconds\":2}")
              .setType(TimerType.CYCLE)
              .setExpression("R/PT2S")
              .build();
      assertStatus(Status.Code.NOT_FOUND, () -> client.createTimerSubscription(request));
      final var scheduled =
          client.createJobSubscription(
              CreateJobSubscriptionRequest.newBuilder()
                  .setName("scheduled")
                  .setJobType("scheduled")
                  .setVariables("{\"amount\":1,\"currency\":\"USD\"}")
                  .build());
      cluster.awaitSubscription(scheduled.getKey(), true);
      assertStatus(Status.Code.NOT_FOUND, () -> client.createTimerSubscription(request));
      final var notify =
          client.createJobSubscription(
              CreateJobSubscriptionRequest.newBuilder()
                  .setName("notify")
                  .setJobType("notify")
                  .build());
      cluster.awaitSubscription(notify.getKey(), true);
      assertStatus(
          Status.Code.NOT_FOUND, () -> cluster.client("tenant-b").createTimerSubscription(request));
      assertStatus(
          Status.Code.INVALID_ARGUMENT,
          () ->
              client.createTimerSubscription(
                  request.toBuilder().setExpression("= \"R/PT2S\"").build()));
      final var response = client.createTimerSubscription(request);
      final var activation =
          ActivateJobsRequest.newBuilder()
              .setType("scheduled")
              .setTimeout(60_000)
              .setRequestTimeout(-1)
              .build();
      final var first = awaitActivated(client, activation, 1).getFirst();
      assertEquals(scheduled.getKey(), first.getSubscriptionKey());
      assertEquals(
          Map.of("amount", 42, "currency", "USD", "seconds", 2),
          new JobRecord().setVariablesJson(first.getVariables()).getVariables());
      assertEquals("tenant-a", first.getTenantId());
      final var notification =
          awaitActivated(client, activation.toBuilder().setType("notify").build(), 1).getFirst();
      assertEquals(notify.getKey(), notification.getSubscriptionKey());
      assertEquals(request.getVariables(), notification.getVariables());
      client.completeJob(CompleteJobRequest.newBuilder().setJobKey(first.getKey()).build());
      cluster.restart();
      final var next = awaitActivated(client, activation, 1).getFirst();
      assertNotEquals(first.getKey(), next.getKey());
      assertEquals(scheduled.getKey(), next.getSubscriptionKey());
      assertEquals(first.getVariables(), next.getVariables());
      client.deleteTimerSubscription(
          io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest.newBuilder()
              .setKey(response.getKey())
              .build());
      assertStatus(
          Status.Code.NOT_FOUND,
          () ->
              client.deleteTimerSubscription(
                  io.taskite.gateway.protocol.DeleteTimerSubscriptionRequest.newBuilder()
                      .setKey(response.getKey())
                      .build()));
      assertEquals(
          next.getKey(),
          client.queryJob(QueryJobRequest.newBuilder().setJobKey(next.getKey()).build()).getKey());
      client.deleteJobSubscription(
          DeleteJobSubscriptionRequest.newBuilder().setKey(notify.getKey()).build());
      cluster.awaitSubscription(notify.getKey(), false);
      assertStatus(
          Status.Code.NOT_FOUND,
          () ->
              client.queryJob(
                  QueryJobRequest.newBuilder().setJobKey(notification.getKey()).build()));
      assertEquals(
          next.getKey(),
          client.queryJob(QueryJobRequest.newBuilder().setJobKey(next.getKey()).build()).getKey());
      assertStatus(Status.Code.NOT_FOUND, () -> client.createTimerSubscription(request));
    }
  }

  @Test
  void workerRegistrationRenewalAndCompletionFencingRemainIntact(@TempDir final Path directory)
      throws Exception {
    try (final var cluster = new TestCluster(directory, 1, false, true)) {
      final var client = cluster.client(null);
      cluster.createSubscription(null, "charge");
      final long key =
          client.createJob(CreateJobRequest.newBuilder().setJobType("charge").build()).getJobKey();
      final var pdChannel =
          ManagedChannelBuilder.forAddress("127.0.0.1", cluster.pdConfig.network().grpcPort())
              .usePlaintext()
              .build();
      try {
        final var pd = PlacementDriverGrpc.newBlockingStub(pdChannel);
        final var registration =
            RegisterWorkerRequest.newBuilder()
                .setClusterId(cluster.clusterId)
                .setWorkerId("owner")
                .setName("owner")
                .addTenantIds("<default>")
                .setJobType("charge")
                .setMaxJobsActive(1)
                .setConcurrency(1)
                .setTimeoutMillis(60_000)
                .setRenewalTimeoutMillis(1_000)
                .build();
        final long token = pd.registerWorker(registration).getWorkerLeaseToken();
        assertEquals(token, pd.registerWorker(registration).getWorkerLeaseToken());
        pd.renewWorker(
            RenewWorkerRequest.newBuilder()
                .setClusterId(cluster.clusterId)
                .setWorkerId("owner")
                .setName("owner")
                .addTenantIds("<default>")
                .setJobType("charge")
                .setMaxJobsActive(1)
                .setConcurrency(1)
                .setTimeoutMillis(60_000)
                .setRenewalTimeoutMillis(1_000)
                .setWorkerLeaseToken(token)
                .build());
        final var activated =
            awaitActivated(
                    client,
                    ActivateJobsRequest.newBuilder()
                        .setType("charge")
                        .setWorkerId("owner")
                        .setWorkerLeaseToken(token)
                        .setTimeout(60_000)
                        .setRequestTimeout(-1)
                        .build(),
                    1)
                .getFirst();
        assertEquals(token, activated.getWorkerLeaseToken());
        assertStatus(
            Status.Code.FAILED_PRECONDITION,
            () ->
                client.completeJob(
                    CompleteJobRequest.newBuilder()
                        .setJobKey(key)
                        .setWorkerId("other")
                        .setWorkerLeaseToken(token)
                        .build()));
        client.completeJob(
            CompleteJobRequest.newBuilder()
                .setJobKey(key)
                .setWorkerId("owner")
                .setWorkerLeaseToken(token)
                .build());
        assertStatus(
            Status.Code.NOT_FOUND,
            () -> client.queryJob(QueryJobRequest.newBuilder().setJobKey(key).build()));
      } finally {
        pdChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void workerExpiryAndDeregistrationReturnActivatedJobsBeforeTheirDeadline(
      final boolean expire, @TempDir final Path directory) throws Exception {
    try (final var cluster = new TestCluster(directory, 1, false, true)) {
      final var client = cluster.client(null);
      cluster.createSubscription(null, "charge");
      final long key =
          client.createJob(CreateJobRequest.newBuilder().setJobType("charge").build()).getJobKey();
      final var pdChannel =
          ManagedChannelBuilder.forAddress("127.0.0.1", cluster.pdConfig.network().grpcPort())
              .usePlaintext()
              .build();
      try {
        final var pd = PlacementDriverGrpc.newBlockingStub(pdChannel);
        final var registration =
            RegisterWorkerRequest.newBuilder()
                .setClusterId(cluster.clusterId)
                .setWorkerId("expired-owner")
                .setName("owner")
                .addTenantIds("<default>")
                .setJobType("charge")
                .setMaxJobsActive(1)
                .setConcurrency(1)
                .setTimeoutMillis(60_000)
                .setRenewalTimeoutMillis(1_000)
                .build();
        final long token = pd.registerWorker(registration).getWorkerLeaseToken();
        final var activated =
            awaitActivated(
                    client,
                    ActivateJobsRequest.newBuilder()
                        .setType("charge")
                        .setWorkerId("expired-owner")
                        .setWorkerLeaseToken(token)
                        .setTimeout(60_000)
                        .setRequestTimeout(-1)
                        .build(),
                    1)
                .getFirst();
        if (!expire) {
          pd.deregisterWorker(
              DeregisterWorkerRequest.newBuilder()
                  .setClusterId(cluster.clusterId)
                  .setWorkerId("expired-owner")
                  .setName("owner")
                  .addTenantIds("<default>")
                  .setWorkerLeaseToken(token)
                  .build());
        }
        final long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        ActivatedJob returned;
        do {
          returned = client.queryJob(QueryJobRequest.newBuilder().setJobKey(key).build());
          if (returned.getDeadline() == 0) {
            break;
          }
          Thread.sleep(20);
        } while (System.nanoTime() < waitUntil);
        assertEquals(
            0, returned.getDeadline(), "PD must yield the job when its worker session ends");
        assertTrue(System.currentTimeMillis() < activated.getDeadline());
        assertStatus(
            Status.Code.FAILED_PRECONDITION,
            () ->
                client.completeJob(
                    CompleteJobRequest.newBuilder()
                        .setJobKey(key)
                        .setWorkerId("expired-owner")
                        .setWorkerLeaseToken(token)
                        .build()));
        final long replacement =
            pd.registerWorker(registration.toBuilder().setWorkerId("replacement").build())
                .getWorkerLeaseToken();
        final var resumed =
            awaitActivated(
                    client,
                    ActivateJobsRequest.newBuilder()
                        .setType("charge")
                        .setWorkerId("replacement")
                        .setWorkerLeaseToken(replacement)
                        .setTimeout(60_000)
                        .setRequestTimeout(-1)
                        .build(),
                    1)
                .getFirst();
        assertEquals(key, resumed.getKey());
        client.completeJob(
            CompleteJobRequest.newBuilder()
                .setJobKey(key)
                .setWorkerId("replacement")
                .setWorkerLeaseToken(replacement)
                .build());
      } finally {
        pdChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  private static void assertStatus(final Status.Code expected, final Runnable operation) {
    assertEquals(
        expected, assertThrows(StatusRuntimeException.class, operation::run).getStatus().getCode());
  }

  private static List<ActivatedJob> awaitActivated(
      final GatewayGrpc.GatewayBlockingStub client,
      final ActivateJobsRequest request,
      final int count)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    final var result = new ArrayList<ActivatedJob>();
    while (result.size() < count && System.nanoTime() < deadline) {
      client
          .withDeadlineAfter(10, TimeUnit.SECONDS)
          .activateJobs(request.toBuilder().setMaxJobsToActivate(count - result.size()).build())
          .forEachRemaining(batch -> result.addAll(batch.getJobsList()));
      assertTrue(result.size() <= count);
      if (result.size() < count) {
        Thread.sleep(20);
      }
    }
    assertEquals(count, result.size(), "jobs did not become activatable");
    return result;
  }

  private static int freePort() throws Exception {
    try (final var socket = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))) {
      return socket.getLocalPort();
    }
  }

  private static void createCluster(final int pdPort, final String clusterId, final int partitions)
      throws Exception {
    final var value =
        new ClusterConfigRecord()
            .setClusterId(clusterId)
            .setClusterSize(1)
            .setPartitionSize(partitions)
            .setReplicationFactor(1)
            .setRosterManaged(true);
    final var valueBuffer = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(valueBuffer, 0);
    final var command =
        new ExecuteCommandRequest()
            .setPartitionId(1)
            .setValueType(ValueType.TOPOLOGY)
            .setIntent(TopologyIntent.CREATE_CLUSTER)
            .setValue(valueBuffer, 0, valueBuffer.capacity());
    final var bytes = new UnsafeBuffer(new byte[command.getLength()]);
    command.write(bytes, 0);
    final var channel =
        ManagedChannelBuilder.forAddress("127.0.0.1", pdPort).usePlaintext().build();
    try {
      InternalPlacementDriverCommandGrpc.executeCommand(
              channel, bytes.byteArray(), Duration.ofSeconds(5))
          .join();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  public static final class TestTenantInterceptor implements ServerInterceptor {
    public TestTenantInterceptor() {}

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call,
        final Metadata headers,
        final ServerCallHandler<ReqT, RespT> next) {
      final String tenant = headers.get(TENANT_HEADER);
      if (tenant == null) {
        call.close(
            Status.UNAUTHENTICATED.withDescription("test tenant is required"), new Metadata());
        return new ServerCall.Listener<>() {};
      }
      return Contexts.interceptCall(
          GatewayInterceptorUtil.withAuthorizedTenantIds(List.of(tenant.split(","))),
          call,
          headers,
          next);
    }
  }

  private static final class TestCluster implements AutoCloseable {
    private final String clusterId = "jobs-" + UUID.randomUUID();
    private final int partitions;
    private PlacementDriver placementDriver;
    private Broker broker;
    private Gateway gateway;
    private ManagedChannel channel;
    private PlacementDriverConfiguration pdConfig;
    private BrokerConfig brokerConfig;
    private int pdHttpPort;

    private TestCluster(
        final Path directory,
        final int partitions,
        final boolean multiTenant,
        final boolean workerTokens)
        throws Exception {
      this.partitions = partitions;
      try {
        final int pdPort = freePort();
        int pdRaftPort;
        do {
          pdRaftPort = freePort();
        } while (pdRaftPort == pdPort);
        pdConfig =
            new PlacementDriverConfiguration(
                "meta-0",
                new PlacementDriverConfiguration.Network("127.0.0.1", pdPort, pdRaftPort),
                new PlacementDriverConfiguration.Consensus(
                    Duration.ofMillis(150), Duration.ofMillis(50)),
                new PlacementDriverConfiguration.Storage(directory.resolve("pd"), null),
                PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT,
                null,
                workerTokens
                    ? new PlacementDriverConfiguration.WorkerLeaseConfiguration(
                        Duration.ofSeconds(5), Duration.ofMinutes(10))
                    : null,
                null);
        pdHttpPort = freePort();
        placementDriver = new PlacementDriver(pdConfig, pdHttpPort);
        placementDriver.start().join();
        createCluster(pdPort, clusterId, partitions);
        final int brokerApiPort = freePort();
        int brokerRaftPort;
        do {
          brokerRaftPort = freePort();
        } while (brokerRaftPort == brokerApiPort);
        brokerConfig =
            new BrokerConfig(
                new BrokerConfig.Identity(clusterId, "broker-0", "test"),
                new BrokerConfig.Node(
                    new BrokerConfig.Network("127.0.0.1", brokerApiPort, brokerRaftPort),
                    directory.resolve("broker")),
                new BrokerConfig.Registration(
                    "127.0.0.1:" + pdPort,
                    Duration.ofMillis(200),
                    BrokerConfig.DEFAULT_RENEWAL_TIMEOUT,
                    Duration.ofMillis(200)),
                new BrokerConfig.Engine(
                    new BrokerConfig.Snapshot(
                        BrokerConfig.DEFAULT_SNAPSHOT_INTERVAL,
                        BrokerConfig.DEFAULT_SNAPSHOT_LOG_SIZE_THRESHOLD_BYTES),
                    new BrokerConfig.Consensus(BrokerConfig.DEFAULT_ELECTION_TIMEOUT),
                    BrokerConfig.defaultProcessing(),
                    workerTokens));
        startBroker();
        gateway =
            new Gateway(
                new GatewayConfig(
                    new GatewayConfig.Network("127.0.0.1", 0, Duration.ofSeconds(30)),
                    new GatewayConfig.PlacementDriver(
                        "127.0.0.1:" + pdPort, Duration.ofSeconds(10), Duration.ofSeconds(1)),
                    new GatewayConfig.Broker(Duration.ofSeconds(15), 0),
                    new GatewayConfig.Threads(GatewayConfig.DEFAULT_MANAGEMENT_THREADS),
                    new GatewayConfig.MultiTenancy(
                        multiTenant,
                        multiTenant
                            ? List.of(
                                new GatewayInterceptorConfig(
                                    "test-tenant", TestTenantInterceptor.class.getName(), ""))
                            : List.of())));
        gateway.start().join();
        channel =
            ManagedChannelBuilder.forAddress("127.0.0.1", gateway.port()).usePlaintext().build();
      } catch (final Exception | AssertionError error) {
        close();
        throw error;
      }
    }

    private void startBroker() throws Exception {
      broker = new Broker(brokerConfig);
      broker.start().join();
      for (int partition = 1; partition <= partitions; partition++) {
        assertTrue(broker.awaitPartitionLeadership(partition, Duration.ofSeconds(10)));
      }
      try (final var client = GrpcClient.connect("127.0.0.1:" + placementDriver.grpcPort())) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
          final var topology =
              client
                  .topology(PlacementDriverRequests.getBrokerTopology(clusterId))
                  .get(5, TimeUnit.SECONDS);
          if (topology.partitions().size() == partitions
              && topology.partitions().stream()
                  .allMatch(
                      partition -> topology.leaderEndpoint(partition.partitionId()).isPresent())) {
            return;
          }
          Thread.sleep(20);
        }
        throw new AssertionError("PD has not observed every service-ready partition leader");
      }
    }

    private void restart() throws Exception {
      broker.stop().get(20, TimeUnit.SECONDS);
      placementDriver.stop().get(20, TimeUnit.SECONDS);
      placementDriver = new PlacementDriver(pdConfig, pdHttpPort);
      placementDriver.start().join();
      startBroker();
    }

    private Channel clientChannel(final String tenant) {
      final var headers = new Metadata();
      headers.put(GatewayInterceptorUtil.CLUSTER_ID_METADATA_KEY, clusterId);
      if (tenant != null) {
        headers.put(TENANT_HEADER, tenant);
      }
      return ClientInterceptors.intercept(
          channel, MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private GatewayGrpc.GatewayBlockingStub client(final String tenant) {
      return GatewayGrpc.newBlockingStub(clientChannel(tenant));
    }

    private void createSubscription(final String tenant, final String jobType) throws Exception {
      final var response =
          client(tenant)
              .createJobSubscription(
                  CreateJobSubscriptionRequest.newBuilder().setJobType(jobType).build());
      awaitSubscription(response.getKey(), true);
    }

    private void awaitSubscription(final long key, final boolean present) throws Exception {
      final var value = new CommandDistributionQueryRecord().setDistributionKey(key);
      final var valueBuffer = new UnsafeBuffer(new byte[value.getLength()]);
      value.write(valueBuffer, 0);
      final var query =
          new ExecuteCommandRequest()
              .setPartitionId(1)
              .setValueType(ValueType.COMMAND_DISTRIBUTION)
              .setIntent(CommandDistributionIntent.QUERY)
              .setValue(valueBuffer, 0, valueBuffer.capacity());
      final var request = new UnsafeBuffer(new byte[query.getLength()]);
      query.write(request, 0);
      final var pd =
          ManagedChannelBuilder.forAddress("127.0.0.1", placementDriver.grpcPort())
              .usePlaintext()
              .build();
      final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      try {
        while (System.nanoTime() < deadline) {
          boolean matches = true;
          for (int partition = 1; partition <= partitions; partition++) {
            matches &= broker.jobSubscription(partition, key).isPresent() == present;
          }
          if (matches) {
            final var bytes =
                InternalPlacementDriverCommandGrpc.executeCommand(
                        pd, request.byteArray(), Duration.ofSeconds(5))
                    .get(5, TimeUnit.SECONDS);
            final var response = new ExecuteCommandResponse();
            response.wrap(new UnsafeBuffer(bytes), 0, bytes.length);
            assertEquals(RecordType.EVENT, response.getRecordType());
            assertEquals(CommandDistributionIntent.QUERIED, response.getIntent());
            final var distribution = new CommandDistributionQueryRecord();
            distribution.wrap(response.getValue());
            if (distribution.getStatus()
                == (present
                    ? JobSubscriptionDistributionStatus.COMPLETED
                    : JobSubscriptionDistributionStatus.DELETED)) {
              return;
            }
          }
          Thread.sleep(20);
        }
        throw new AssertionError(
            "subscription " + key + " presence did not converge to " + present);
      } finally {
        pd.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    @Override
    public void close() throws Exception {
      if (channel != null) {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      }
      if (gateway != null) {
        gateway.stop().get(20, TimeUnit.SECONDS);
      }
      try {
        if (broker != null) {
          broker.stop().get(20, TimeUnit.SECONDS);
        }
      } finally {
        if (placementDriver != null) {
          placementDriver.stop().get(20, TimeUnit.SECONDS);
        }
      }
    }
  }
}
