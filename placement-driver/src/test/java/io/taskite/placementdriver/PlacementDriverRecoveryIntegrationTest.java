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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.placementdriver.bootstrap.PlacementDriverSpringLifecycle;
import io.taskite.placementdriver.client.GrpcClient;
import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.placementdriver.protocol.CreateJobSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeleteJobSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeregisterWorkerRequest;
import io.taskite.placementdriver.protocol.DistributeCommandRequest;
import io.taskite.placementdriver.protocol.RegisterWorkerRequest;
import io.taskite.placementdriver.protocol.RenewWorkerRequest;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

final class PlacementDriverRecoveryIntegrationTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static int raftPort;
  private static int serviceGrpcPort;
  private static int brokerPort;
  private static int brokerRaftPort;

  @BeforeAll
  static void allocateTestPorts() throws Exception {
    final InetAddress loopback = InetAddress.getByName("127.0.0.1");
    try (ServerSocket grpc = new ServerSocket(0, 0, loopback);
        ServerSocket raft = new ServerSocket(0, 0, loopback);
        ServerSocket broker = new ServerSocket(0, 0, loopback);
        ServerSocket brokerRaft = new ServerSocket(0, 0, loopback)) {
      raftPort = raft.getLocalPort();
      serviceGrpcPort = grpc.getLocalPort();
      brokerPort = broker.getLocalPort();
      brokerRaftPort = brokerRaft.getLocalPort();
    }
  }

  @Test
  void resourceManagementHttpPersistsRedistributionAndMirrorTargetsAcrossRestart(
      @TempDir final Path tempDir) throws Exception {
    final String variables = "{\"amount\":42}";
    final Path dataDir = tempDir.resolve("resources");
    final long key;
    try (Fixture fixture = Fixture.start(dataDir)) {
      fixture.createCluster(1, 2, 1);
      assertEquals(
          201,
          fixture
              .post(
                  "/api/v1/broker/clusters",
                  "{\"clusterId\":\"backup\",\"clusterSize\":1,\"partitionSize\":1,\"replicationFactor\":1}")
              .statusCode());
      final var mirror =
          fixture.post(
              "/api/v1/broker/job-subscription-mirrors",
              "{\"sourceClusterId\":\"demo\",\"targetClusterId\":\"backup\"}");
      assertEquals(201, mirror.statusCode(), mirror.body());
      final var deployment =
          DistributeCommandRequest.newBuilder()
              .setClusterId("demo")
              .setTenantId("tenant")
              .setCreateJobSubscription(
                  CreateJobSubscriptionRequest.newBuilder()
                      .setName("order")
                      .setJobType("payment")
                      .setVariables(variables)
                      .setRetries(3)
                      .setPriority(50))
              .build();
      key =
          fixture
              .client
              .distributeCommand(deployment)
              .get(10, TimeUnit.SECONDS)
              .getDistributionKey();
      final var list =
          fixture.getJson(
              "/api/v1/broker/job-subscriptions?jobType=payment&clusterId=demo&distributionKey="
                  + key);
      assertEquals(1, list.path("total").asInt());
      assertFalse(list.path("subscriptions").get(0).has("resourceContent"));
      assertEquals("payment", list.path("subscriptions").get(0).path("jobType").asText());
      assertEquals(
          0, fixture.getJson("/api/v1/broker/job-subscriptions?jobType=pay").path("total").asInt());
      final String detailPath = "/api/v1/broker/job-subscriptions/" + key;
      assertEquals(
          variables, fixture.getJson(detailPath).path("subscription").path("variables").asText());
      assertEquals(
          204,
          fixture
              .delete(
                  "/api/v1/broker/job-subscription-mirrors/"
                      + json(mirror).path("mirrorKey").asLong())
              .statusCode());
      final var accepted =
          fixture.post(detailPath + "/redistribute", "{\"expectedGeneration\":\"0\"}");
      assertEquals(202, accepted.statusCode(), accepted.body());
      assertEquals(1, json(accepted).path("generation").asLong());
      final var conflict =
          fixture.post(detailPath + "/redistribute", "{\"expectedGeneration\":\"0\"}");
      assertEquals(409, conflict.statusCode(), conflict.body());
      assertEquals(
          "JOB_SUBSCRIPTION_DISTRIBUTION_GENERATION_CONFLICT",
          json(conflict).path("error").asText());
      assertEquals(
          400, fixture.get("/api/v1/broker/job-subscriptions?distributionKey=-1").statusCode());
    }
    try (Fixture restored = Fixture.start(dataDir)) {
      final String detailPath = "/api/v1/broker/job-subscriptions/" + key;
      final var detail = restored.getJson(detailPath);
      assertEquals(1, detail.path("generation").asLong());
      assertTrue(detail.path("lastRedistributedAt").asLong() > 0);
      assertEquals(2, detail.path("targets").size());
      assertEquals(2, detail.path("targets").get(0).path("pendingPartitions").size());
      assertEquals("backup", detail.path("targets").get(1).path("clusterId").asText());
      assertEquals(1, detail.path("targets").get(1).path("pendingPartitions").size());
      assertTrue(detail.path("targets").get(1).path("mirrorKey").isNull());
      final var mirrorList =
          restored.getJson(
              "/api/v1/broker/job-subscriptions?clusterId=backup&jobType=payment&distributionKey="
                  + key);
      assertEquals(1, mirrorList.path("total").asInt());
      assertEquals(
          "demo", mirrorList.path("subscriptions").get(0).path("sourceClusterId").asText());
      assertEquals("backup", mirrorList.path("subscriptions").get(0).path("clusterId").asText());
      assertEquals("MIRROR", mirrorList.path("subscriptions").get(0).path("creationType").asText());
      assertEquals(1, mirrorList.path("subscriptions").get(0).path("targets").size());
      restored
          .client
          .distributeCommand(
              DistributeCommandRequest.newBuilder()
                  .setClusterId("demo")
                  .setTenantId("tenant")
                  .setDeleteJobSubscription(
                      DeleteJobSubscriptionRequest.newBuilder().setSubscriptionKey(key))
                  .build())
          .get(10, TimeUnit.SECONDS);
      assertEquals("DELETING", restored.getJson(detailPath).path("status").asText());
      final var rejected =
          restored.post(detailPath + "/redistribute", "{\"expectedGeneration\":\"1\"}");
      assertEquals(409, rejected.statusCode(), rejected.body());
      assertEquals("JOB_SUBSCRIPTION_DELETED", json(rejected).path("error").asText());
      assertEquals(2, restored.getJson("/api/v1/broker/job-subscriptions").path("total").asInt());
    }
  }

  @Test
  void createsClusterIdempotentlyAndRejectsConflict(@TempDir final Path tempDir) throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("create"))) {
      final String config =
          "{\"clusterId\":\"demo\",\"clusterSize\":2,\"partitionSize\":1,"
              + "\"replicationFactor\":2}";

      assertEquals(201, fixture.post("/api/v1/broker/clusters", config).statusCode());
      final HttpResponse<String> repeated = fixture.post("/api/v1/broker/clusters", config);
      assertEquals(200, repeated.statusCode(), repeated.body());

      final HttpResponse<String> conflict =
          fixture.post(
              "/api/v1/broker/clusters",
              "{\"clusterId\":\"demo\",\"clusterSize\":3,\"partitionSize\":1,"
                  + "\"replicationFactor\":2}");
      assertEquals(409, conflict.statusCode());
      assertEquals("CLUSTER_CONFIG_CONFLICT", json(conflict).path("error").asText());
      assertEquals(
          2,
          fixture
              .getJson("/api/v1/broker/clusters/demo/configuration")
              .path("clusterSize")
              .asInt());
    }
  }

  @Test
  void placementDriverTopologyExposesMembersAndLeader(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("pd-topology"))) {
      final JsonNode topology = fixture.getJson("/api/v1/pd/topology");

      assertEquals("meta-0", topology.path("localMemberId").asText());
      assertEquals("meta-0", topology.path("leaderId").asText());
      assertEquals(1, topology.path("members").size());
      assertTrue(topology.path("members").get(0).path("self").asBoolean());
    }
  }

  @Test
  void brokerRegistrationAutomaticallyCreatesInitialPlacement(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("registration"))) {
      fixture.createCluster(1, 1, 1);
      fixture.registerBroker("broker-0");

      final JsonNode topology = fixture.getJson("/api/v1/broker/clusters/demo/topology");
      assertEquals(1, topology.path("clusterSize").asInt());
      assertEquals(
          List.of("broker-0"),
          MAPPER.convertValue(topology.path("partitions").get(0).path("replicas"), List.class));
    }
  }

  @Test
  void workerHeartbeatDrivesRegistryApi(@TempDir final Path tempDir) throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("worker-registry"))) {
      fixture.createCluster(1, 1, 1);
      final RegisterWorkerRequest heartbeat =
          RegisterWorkerRequest.newBuilder()
              .setClusterId("demo")
              .addAllTenantIds(List.of("tenant-a", "tenant-b"))
              .setWorkerId("instance-1")
              .setName("payments")
              .setJobType("payment")
              .setMaxJobsActive(32)
              .setActiveJobs(4)
              .setConcurrency(8)
              .setTimeoutMillis(30_000)
              .setRequestTimeoutMillis(10_000)
              .setPollIntervalMillis(100)
              .addFetchVariables("orderId")
              .setClientVersion("0.1.0")
              .setRenewalTimeoutMillis(30_000)
              .build();

      final long token =
          fixture.client.registerWorker(heartbeat).get(10, TimeUnit.SECONDS).getWorkerLeaseToken();
      assertTrue(token > 0);
      assertEquals(
          token,
          fixture
              .client
              .renewWorker(renew(heartbeat, token))
              .get(10, TimeUnit.SECONDS)
              .getWorkerLeaseToken());
      final JsonNode workers = fixture.getJson("/api/v1/workers?cluster=demo&name=payments");
      assertEquals(2, workers.path("total").asInt());
      assertEquals(2, workers.path("online").asInt());
      assertEquals("tenant-a", workers.path("workers").get(0).path("tenantId").asText());
      assertEquals(32, workers.path("workers").get(0).path("maxJobsActive").asInt());
      assertEquals(
          Long.toString(token), workers.path("workers").get(0).path("workerLeaseToken").asText());
      assertTrue(workers.path("workers").get(0).path("workerLeaseToken").isTextual());

      fixture
          .client
          .deregisterWorker(
              DeregisterWorkerRequest.newBuilder()
                  .setClusterId("demo")
                  .addAllTenantIds(heartbeat.getTenantIdsList())
                  .setWorkerId(heartbeat.getWorkerId())
                  .setName(heartbeat.getName())
                  .setWorkerLeaseToken(token)
                  .build())
          .get(10, TimeUnit.SECONDS);
      fixture.await(
          () -> {
            try {
              return fixture.getJson("/api/v1/workers?cluster=demo").path("offline").asInt() == 2;
            } catch (final Exception ignored) {
              return false;
            }
          });
    }
  }

  @Test
  void restoresOverdueWorkerLeaseUsingPdPolicyAndDeletesOfflineRegistration(
      @TempDir final Path tempDir) throws Exception {
    final Path dataDir = tempDir.resolve("worker-deadline-recovery");
    // Keep the offline observation window open across graceful shutdown and cold startup.
    final Duration offlineRetention = Duration.ofSeconds(10);
    final RegisterWorkerRequest heartbeat =
        RegisterWorkerRequest.newBuilder()
            .setClusterId("demo")
            .addTenantIds("tenant-a")
            .setWorkerId("instance-expiring")
            .setName("payments")
            .setJobType("payment")
            .setMaxJobsActive(32)
            .setConcurrency(4)
            .setRenewalTimeoutMillis(30_000)
            .build();
    try (Fixture fixture = Fixture.start(dataDir, Duration.ofMillis(500), offlineRetention)) {
      fixture.createCluster(1, 1, 1);
      fixture.client.registerWorker(heartbeat).get(10, TimeUnit.SECONDS);
      assertEquals(1, fixture.getJson("/api/v1/workers?cluster=demo").path("online").asInt());
    }

    Thread.sleep(600);

    String yieldId;
    try (Fixture restored = Fixture.start(dataDir, Duration.ofMillis(500), offlineRetention)) {
      restored.await(
          () -> {
            try {
              return restored.getJson("/api/v1/workers?cluster=demo").path("offline").asInt() == 1;
            } catch (final Exception ignored) {
              return false;
            }
          });
      final JsonNode history =
          restored.getJson("/api/v1/workers/yields?cluster=demo&workerId=instance-expiring");
      assertEquals(1, history.path("total").asInt());
      final JsonNode plan = history.path("yields").get(0);
      assertEquals("PENDING", plan.path("status").asText());
      yieldId = plan.path("yieldId").asText();
      assertEquals(200, restored.get("/api/v1/workers/yields/" + yieldId).statusCode());
      restored.await(
          () -> {
            try {
              return restored.getJson("/api/v1/workers?cluster=demo").path("total").asInt() == 0;
            } catch (final Exception ignored) {
              return false;
            }
          },
          offlineRetention.plusSeconds(5));
      assertEquals(
          0,
          restored
              .getJson("/api/v1/workers/yields?cluster=demo&workerId=instance-expiring")
              .path("total")
              .asInt());
      assertEquals(404, restored.get("/api/v1/workers/yields/" + yieldId).statusCode());
    }
    try (Fixture recovered = Fixture.start(dataDir, Duration.ofMillis(500), offlineRetention)) {
      assertEquals(0, recovered.getJson("/api/v1/workers?cluster=demo").path("total").asInt());
      assertEquals(
          0, recovered.getJson("/api/v1/workers/yields?cluster=demo").path("total").asInt());
      assertEquals(404, recovered.get("/api/v1/workers/yields/" + yieldId).statusCode());
    }
  }

  @Test
  void patchAddsPartitionAndReturnsAggregateUuid(@TempDir final Path tempDir) throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("patch"))) {
      fixture.createCluster(2, 1, 1);
      fixture.registerBroker("broker-0");

      final HttpResponse<String> accepted =
          fixture.patch(
              "/api/v1/broker/clusters/demo/configuration", "{\"partitions\":{\"count\":2}}");

      assertEquals(202, accepted.statusCode());
      final String changeId = json(accepted).path("changeId").asText();
      UUID.fromString(changeId);
      fixture.await(
          () -> {
            try {
              return fixture
                  .getJson("/api/v1/broker/clusters/demo/changes/active")
                  .path("plannedChanges")
                  .isArray();
            } catch (final Exception ignored) {
              return false;
            }
          });

      final JsonNode latestChange = fixture.getJson("/api/v1/broker/clusters/demo/changes/active");
      assertEquals(changeId, latestChange.path("changeId").asText());
      final JsonNode change = fixture.getJson("/api/v1/broker/clusters/demo/changes/" + changeId);
      assertEquals(changeId, change.path("changeId").asText());
      assertEquals("PENDING", change.path("status").asText());
      assertTrue(change.has("currentTopology"));
      assertTrue(change.has("plannedChanges"));
      assertTrue(change.has("pendingChanges"));
      assertTrue(change.has("expectedTopology"));
      assertTrue(change.path("executionHistory").isArray());
      assertEquals("CLUSTER", change.path("changeType").asText());
      assertTrue(change.path("cancellable").asBoolean());
      assertEquals("", change.path("cancellationReason").asText());
      assertEquals(10, change.size());
      final JsonNode progress = change.path("pendingChanges").get(0);
      assertTrue(progress.has("partitionId"));
      assertTrue(progress.has("status"));
      assertTrue(progress.has("nextOperationIndex"));
      assertTrue(progress.has("operationCount"));
      assertTrue(progress.has("currentOperation"));
      assertTrue(progress.has("remainingOperations"));
      assertEquals("ACTIVE", change.path("expectedTopology").get(1).path("status").asText());

      final HttpResponse<String> conflict =
          fixture.patch(
              "/api/v1/broker/clusters/demo/configuration", "{\"partitions\":{\"count\":3}}");
      assertEquals(409, conflict.statusCode(), conflict.body());
      assertEquals(changeId, json(conflict).path("pendingChangeIds").get(0).asText());

      final HttpResponse<String> cancelled =
          fixture.delete("/api/v1/broker/clusters/demo/changes/" + changeId);
      assertEquals(202, cancelled.statusCode(), cancelled.body());
      assertEquals("CANCELLED", json(cancelled).path("status").asText());
      final JsonNode cancelledChange =
          fixture.getJson("/api/v1/broker/clusters/demo/changes/" + changeId);
      assertEquals("CANCELLED", cancelledChange.path("status").asText());
      assertFalse(cancelledChange.path("plannedChanges").isEmpty());
      assertTrue(cancelledChange.path("pendingChanges").isEmpty());
      final var activeAfterCancellation =
          fixture.get("/api/v1/broker/clusters/demo/changes/active");
      // Releasing pending ownership can let reconciliation create a different plan.
      // Cancellation must not resurrect the cancelled aggregate as the active change.
      if (activeAfterCancellation.statusCode() == 200) {
        assertFalse(changeId.equals(json(activeAfterCancellation).path("changeId").asText()));
      } else {
        assertEquals(404, activeAfterCancellation.statusCode(), activeAfterCancellation.body());
      }
    }
  }

  @Test
  void brokerBeyondClusterSizeRemainsOutsideDesiredVoters(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("extra-learner"))) {
      fixture.createCluster(1, 1, 1);
      fixture.registerBroker("broker-0");
      fixture.registerBroker("broker-1");

      final JsonNode partition =
          fixture.getJson("/api/v1/broker/clusters/demo/topology").path("partitions").get(0);
      assertEquals(
          List.of("broker-0"), MAPPER.convertValue(partition.path("replicas"), List.class));
    }
  }

  @Test
  void countOnlyShrinkRemovesDeterministicallySelectedBroker(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("count-shrink"))) {
      fixture.createCluster(3, 1, 1);
      fixture.registerBroker("broker-0");
      fixture.registerBroker("broker-1");
      fixture.registerBroker("broker-2");

      final HttpResponse<String> accepted =
          fixture.patch(
              "/api/v1/broker/clusters/demo/configuration", "{\"brokers\":{\"count\":2}}");

      assertTrue(accepted.statusCode() == 200 || accepted.statusCode() == 202, accepted.body());
      assertEquals(
          2,
          fixture
              .getJson("/api/v1/broker/clusters/demo/configuration")
              .path("clusterSize")
              .asInt());
      final List<String> brokerIds = new ArrayList<>();
      fixture
          .getJson("/api/v1/broker/clusters/demo/topology")
          .path("brokers")
          .forEach(broker -> brokerIds.add(broker.path("brokerId").asText()));
      assertFalse(brokerIds.contains("broker-2"));
    }
  }

  @Test
  void brokerTopologyContainsOnlyOnlineBrokerEndpoints(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("online-topology"))) {
      fixture.createCluster(2, 1, 1);
      fixture.registerBroker("broker-0");
      fixture.registerBroker("broker-1");
      fixture
          .client
          .deregisterBroker(PlacementDriverRequests.deregisterBroker("demo", "broker-1"))
          .get(10, TimeUnit.SECONDS);
      fixture.await(
          () -> {
            try {
              for (final JsonNode broker :
                  fixture.getJson("/api/v1/broker/clusters/demo/topology").path("brokers")) {
                if ("broker-1".equals(broker.path("brokerId").asText())) {
                  return false;
                }
              }
              return true;
            } catch (final Exception ignored) {
              return false;
            }
          });

      final var topology = fixture.client.topology("demo").get(10, TimeUnit.SECONDS);
      assertTrue(topology.brokers().containsKey("broker-0"));
      assertFalse(topology.brokers().containsKey("broker-1"));
    }
  }

  @Test
  void brokerTopologyIncludesBrokerPartitionObservations(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("broker-partitions"))) {
      fixture.createCluster(3, 1, 1);
      fixture.registerBroker(
          "broker-0",
          List.of(
              new BrokerPartitionState(
                  1,
                  new BrokerPartitionState.Status(
                      MemberRole.LEADER, 7, BrokerPartitionLifecycle.ACTIVE, 10, 3),
                  new BrokerPartitionState.Replication(123, 4, true),
                  new BrokerPartitionState.Operation("op-1", BrokerTopologyOperationStatus.APPLIED),
                  BrokerPartitionElectionState.UNKNOWN)));

      final JsonNode topology = fixture.getJson("/api/v1/broker/clusters/demo/topology");
      assertEquals(3, topology.path("clusterSize").asInt());
      final JsonNode broker = topology.path("brokers").get(0);
      assertEquals("test", broker.path("version").asText());
      assertEquals("ONLINE", broker.path("status").asText());
      assertEquals(120_000, broker.path("renewalTimeoutMillis").asLong());
      assertTrue(broker.path("registeredAt").isTextual());
      assertTrue(broker.path("lastRenewal").isTextual());
      final JsonNode partition = broker.path("partitions").get(0);
      assertEquals(1, partition.path("partitionId").asInt());
      assertEquals("LEADER", partition.path("role").asText());
      assertEquals(7, partition.path("term").asLong());
      assertEquals("ACTIVE", partition.path("lifecycleState").asText());
      assertEquals(123, partition.path("matchIndex").asLong());
      assertEquals(4, partition.path("lag").asLong());
      assertTrue(partition.path("readyForPromotion").asBoolean());
      assertEquals(10, partition.path("priority").asInt());
      assertEquals("op-1", partition.path("lastOpId").asText());
      assertEquals("APPLIED", partition.path("lastOpStatus").asText());
      assertEquals(3, partition.path("recoveryEpoch").asLong());
    }
  }

  @Test
  void rejectsPartitionDecreaseAndActiveClusterDeletion(@TempDir final Path tempDir)
      throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("guards"))) {
      fixture.createCluster(1, 2, 1);
      fixture.registerBroker("broker-0");

      final HttpResponse<String> decrease =
          fixture.patch(
              "/api/v1/broker/clusters/demo/configuration", "{\"partitions\":{\"count\":1}}");
      assertEquals(409, decrease.statusCode());
      assertEquals("PARTITION_SIZE_DECREASE_NOT_ALLOWED", json(decrease).path("error").asText());

      final HttpResponse<String> delete = fixture.delete("/api/v1/broker/clusters/demo");
      assertEquals(409, delete.statusCode());
      assertEquals("CLUSTER_HAS_ACTIVE_BROKERS", json(delete).path("error").asText());
    }
  }

  @Test
  void deletesEmptyClusterIdempotently(@TempDir final Path tempDir) throws Exception {
    try (Fixture fixture = Fixture.start(tempDir.resolve("delete"))) {
      fixture.createCluster(1, 1, 1);

      assertEquals(204, fixture.delete("/api/v1/broker/clusters/demo").statusCode());
      assertEquals(204, fixture.delete("/api/v1/broker/clusters/demo").statusCode());
      assertEquals(404, fixture.get("/api/v1/broker/clusters/demo/configuration").statusCode());
    }
  }

  private static RenewWorkerRequest renew(final RegisterWorkerRequest request, final long token) {
    return RenewWorkerRequest.newBuilder()
        .setClusterId(request.getClusterId())
        .addAllTenantIds(request.getTenantIdsList())
        .setWorkerId(request.getWorkerId())
        .setWorkerLeaseToken(token)
        .setName(request.getName())
        .setJobType(request.getJobType())
        .setMaxJobsActive(request.getMaxJobsActive())
        .setActiveJobs(request.getActiveJobs())
        .setConcurrency(request.getConcurrency())
        .setTimeoutMillis(request.getTimeoutMillis())
        .setRequestTimeoutMillis(request.getRequestTimeoutMillis())
        .setPollIntervalMillis(request.getPollIntervalMillis())
        .addAllFetchVariables(request.getFetchVariablesList())
        .setClientVersion(request.getClientVersion())
        .setRenewalTimeoutMillis(request.getRenewalTimeoutMillis())
        .build();
  }

  private static JsonNode json(final HttpResponse<String> response) throws Exception {
    return MAPPER.readTree(response.body());
  }

  private static final class Fixture implements AutoCloseable {
    private final ConfigurableApplicationContext context;
    private final GrpcClient client;
    private final HttpClient http = HttpClient.newHttpClient();
    private final String httpBase;

    private Fixture(
        final ConfigurableApplicationContext context,
        final GrpcClient client,
        final String httpBase) {
      this.context = context;
      this.client = client;
      this.httpBase = httpBase;
    }

    static Fixture start(final Path dataDir) throws Exception {
      return start(
          dataDir,
          PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT,
          PlacementDriverConfiguration.DEFAULT_WORKER_OFFLINE_RETENTION);
    }

    static Fixture start(
        final Path dataDir, final Duration workerLeaseTimeout, final Duration offlineRetention)
        throws Exception {
      final ConfigurableApplicationContext context =
          new SpringApplicationBuilder(PlacementDriverApplication.class)
              .run(
                  "--server.port=0",
                  "--taskite.pd.node-id=meta-0",
                  "--taskite.pd.network.host=127.0.0.1",
                  "--taskite.pd.network.grpc-port=" + serviceGrpcPort,
                  "--taskite.pd.network.raft-port=" + raftPort,
                  "--taskite.pd.consensus.election-timeout=150ms",
                  "--taskite.pd.consensus.heartbeat-interval=50ms",
                  "--taskite.pd.worker.lease-timeout=" + workerLeaseTimeout,
                  "--taskite.pd.worker.offline-retention=" + offlineRetention,
                  "--taskite.pd.storage.data-dir=" + dataDir,
                  "--taskite.pd.consensus.bootstrap-peers=");
      final int httpPort = context.getEnvironment().getProperty("local.server.port", Integer.class);
      final int grpcPort = context.getBean(PlacementDriverSpringLifecycle.class).grpcPort();
      final Fixture fixture =
          new Fixture(
              context, GrpcClient.connect("127.0.0.1:" + grpcPort), "http://127.0.0.1:" + httpPort);
      final AtomicReference<String> lastHealth = new AtomicReference<>("not queried");
      final HttpRequest healthRequest =
          HttpRequest.newBuilder(URI.create(fixture.httpBase + "/api/v1/pd/health"))
              .timeout(Duration.ofSeconds(2))
              .GET()
              .build();
      try {
        fixture.await(
            () -> {
              try {
                final HttpResponse<String> response = fixture.request(healthRequest);
                lastHealth.set("HTTP " + response.statusCode() + ": " + response.body());
                return response.statusCode() == 200
                    && "LEADER".equals(json(response).path("role").asText());
              } catch (final Exception error) {
                lastHealth.set(error.toString());
                return false;
              }
            },
            Duration.ofSeconds(10),
            () -> "placement-driver health did not become LEADER: " + lastHealth.get());
        return fixture;
      } catch (final Exception | Error failure) {
        try {
          fixture.close();
        } catch (final RuntimeException | Error cleanupFailure) {
          failure.addSuppressed(cleanupFailure);
        }
        throw failure;
      }
    }

    void createCluster(final int clusterSize, final int partitionSize, final int replicationFactor)
        throws Exception {
      final HttpResponse<String> response =
          post(
              "/api/v1/broker/clusters",
              "{\"clusterId\":\"demo\",\"clusterSize\":"
                  + clusterSize
                  + ",\"partitionSize\":"
                  + partitionSize
                  + ",\"replicationFactor\":"
                  + replicationFactor
                  + "}");
      assertTrue(response.statusCode() == 200 || response.statusCode() == 201, response.body());
    }

    void registerBroker(final String brokerId) throws Exception {
      registerBroker(brokerId, List.of());
    }

    void registerBroker(final String brokerId, final List<BrokerPartitionState> partitions)
        throws Exception {
      client
          .registerBroker(
              PlacementDriverRequests.registerBroker(
                  "demo",
                  brokerId,
                  "127.0.0.1",
                  brokerPort,
                  brokerRaftPort,
                  "test",
                  120_000,
                  partitions))
          .get(10, TimeUnit.SECONDS);
    }

    HttpResponse<String> get(final String path) throws Exception {
      return request(HttpRequest.newBuilder(URI.create(httpBase + path)).GET().build());
    }

    JsonNode getJson(final String path) throws Exception {
      final HttpResponse<String> response = get(path);
      assertEquals(200, response.statusCode(), response.body());
      return json(response);
    }

    HttpResponse<String> post(final String path, final String body) throws Exception {
      return request(
          HttpRequest.newBuilder(URI.create(httpBase + path))
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build());
    }

    HttpResponse<String> patch(final String path, final String body) throws Exception {
      return request(
          HttpRequest.newBuilder(URI.create(httpBase + path))
              .header("Content-Type", "application/json")
              .method("PATCH", HttpRequest.BodyPublishers.ofString(body))
              .build());
    }

    HttpResponse<String> delete(final String path) throws Exception {
      return request(HttpRequest.newBuilder(URI.create(httpBase + path)).DELETE().build());
    }

    private HttpResponse<String> request(final HttpRequest request) throws Exception {
      return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    void await(final BooleanSupplier condition) throws Exception {
      await(condition, Duration.ofSeconds(10));
    }

    void await(final BooleanSupplier condition, final Duration timeout) throws Exception {
      await(condition, timeout, () -> "condition did not converge within " + timeout);
    }

    void await(
        final BooleanSupplier condition, final Duration timeout, final Supplier<String> message)
        throws Exception {
      final long deadline = System.nanoTime() + timeout.toNanos();
      while (System.nanoTime() < deadline) {
        if (condition.getAsBoolean()) {
          return;
        }
        Thread.sleep(50);
      }
      assertTrue(condition.getAsBoolean(), message);
    }

    @Override
    public void close() {
      try {
        client.close();
      } finally {
        context.close();
      }
    }
  }
}
