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
import io.taskite.placementdriver.api.model.PlacementDriverHttpModel;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RegistrationStatus;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the JSON shape of the read-only HTTP DTOs so the admin/observability contract stays stable
 * after the move from hand-built JSON strings to Jackson serialization.
 */
class PlacementDriverHttpModelTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void executionHistoryIsFlatAndPreservesAuthorityAndLineage() throws Exception {
    final var pending =
        new PlacementDriverHttpModel.PendingChangeProgressView(
            new PlacementDriverHttpModel.PendingChangeProgressView.Identity(
                1, ChangeStatus.SUPERSEDING, "child", false),
            new PlacementDriverHttpModel.PendingChangeProgressView.Progress(1, 2, null, List.of()),
            new PlacementDriverHttpModel.PendingChangeProgressView.Deadline("start", "deadline"),
            new PlacementDriverHttpModel.PendingChangeProgressView.Execution(
                9007199254740993L,
                9007199254740995L,
                "root",
                "previous",
                "next",
                PlanBlockedReason.NO_QUORUM,
                "since"));
    final var response =
        new PlacementDriverHttpModel.ClusterChangeStatusResponse(
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Identity(
                "root",
                ChangeStatus.WAITING_READY,
                ChangeType.PARTITION,
                false,
                ChangeCancellationReason.CHANGE_RECOVERY_REQUIRED),
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Topologies(
                List.of(), List.of()),
            new PlacementDriverHttpModel.ClusterChangeStatusResponse.Changes(
                List.of(), List.of(pending), List.of(pending)));
    final JsonNode node = MAPPER.readTree(MAPPER.writeValueAsBytes(response));
    final var history = node.path("executionHistory").get(0);
    assertEquals("child", history.path("changeId").asText());
    assertEquals(9007199254740993L, history.path("executionGeneration").asLong());
    assertEquals(9007199254740995L, history.path("authorityIndex").asLong());
    assertEquals("root", history.path("rootChangeId").asText());
    assertEquals("previous", history.path("supersedes").asText());
    assertEquals("next", history.path("supersededBy").asText());
    assertEquals("NO_QUORUM", history.path("blockedReason").asText());
    assertFalse(history.path("terminal").asBoolean());
    assertFalse(history.has("executionData"));
    assertEquals(history, node.path("pendingChanges").get(0));
    assertEquals("PARTITION", node.path("changeType").asText());
    assertFalse(node.path("cancellable").asBoolean());
    assertEquals("CHANGE_RECOVERY_REQUIRED", node.path("cancellationReason").asText());
  }

  @Test
  void healthResponseShape() throws Exception {
    JsonNode node =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.HealthResponse(
                    new PlacementDriverHttpModel.HealthResponse.Identity(
                        PlacementDriverHttpModel.HealthStatus.UP,
                        "node-0",
                        MemberRole.LEADER,
                        "node-0",
                        "http://h:1"),
                    new PlacementDriverHttpModel.HealthResponse.Endpoint("h", 1, 2),
                    new PlacementDriverHttpModel.HealthResponse.Consensus(3, 2, 1),
                    new PlacementDriverHttpModel.HealthResponse.Progress(4, 5, 6))));
    assertEquals("UP", node.get("status").asText());
    assertEquals("node-0", node.get("nodeId").asText());
    assertEquals("LEADER", node.get("role").asText());
    assertEquals(1, node.get("port").asInt());
    assertEquals(2, node.get("grpcPort").asInt());
    assertEquals(3, node.get("term").asLong());
    assertEquals(2, node.get("quorum").asInt());
    assertEquals(4, node.get("commitIndex").asLong());
    assertEquals(5, node.get("lastApplied").asLong());
    assertEquals(6, node.get("lastLogIndex").asLong());
    assertEquals(1, node.get("peerCount").asInt());
    assertTrue(node.get("renewalTimeoutMillis") == null);
  }

  @Test
  void clusterChangeStatusIncludesPartitionProgress() throws Exception {
    final var operation =
        new PlacementDriverHttpModel.TopologyOperationView(
            "op-1", TopologyOperationType.PROMOTE_LEARNER, "broker-1", 0);
    final var pending =
        new PlacementDriverHttpModel.PendingChangeProgressView(
            new PlacementDriverHttpModel.PendingChangeProgressView.Identity(
                1, ChangeStatus.WAITING_READY),
            new PlacementDriverHttpModel.PendingChangeProgressView.Progress(
                1, 3, operation, List.of(operation)),
            new PlacementDriverHttpModel.PendingChangeProgressView.Deadline(
                "2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z"));

    final JsonNode node =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.ClusterChangeStatusResponse(
                    new PlacementDriverHttpModel.ClusterChangeStatusResponse.Identity(
                        "change-1",
                        ChangeStatus.WAITING_READY,
                        ChangeType.CLUSTER,
                        true,
                        ChangeCancellationReason.NONE),
                    new PlacementDriverHttpModel.ClusterChangeStatusResponse.Topologies(
                        List.of(), List.of()),
                    new PlacementDriverHttpModel.ClusterChangeStatusResponse.Changes(
                        List.of(operation), List.of(pending)))));

    assertEquals("WAITING_READY", node.path("status").asText());
    final JsonNode progress = node.path("pendingChanges").get(0);
    assertEquals(1, progress.path("partitionId").asInt());
    assertEquals("WAITING_READY", progress.path("status").asText());
    assertEquals(1, progress.path("nextOperationIndex").asInt());
    assertEquals(3, progress.path("operationCount").asInt());
    assertEquals("op-1", progress.path("currentOperation").path("opId").asText());
    assertEquals(
        "PROMOTE_LEARNER", progress.path("remainingOperations").get(0).path("type").asText());
  }

  @Test
  void topologyResponseShape() throws Exception {
    JsonNode node =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.TopologyResponse(
                    new PlacementDriverHttpModel.TopologyResponse.Identity(
                        "demo", 12, "2024-01-01T00:00:00Z"),
                    new PlacementDriverHttpModel.TopologyResponse.Configuration(3, 5, 2),
                    new PlacementDriverHttpModel.TopologyResponse.Brokers(
                        List.of(
                            new PlacementDriverHttpModel.BrokerView(
                                new PlacementDriverHttpModel.BrokerView.Identity(
                                    "broker-0", "1.2.3"),
                                new PlacementDriverHttpModel.BrokerView.Endpoint(
                                    "127.0.0.1", 26501, 26601),
                                new PlacementDriverHttpModel.BrokerView.Lease(
                                    "2024-01-01T00:00:00Z", "2024-01-01T00:05:00Z", 30_000),
                                new PlacementDriverHttpModel.BrokerView.Report(
                                    RegistrationStatus.ONLINE,
                                    List.of(
                                        new PlacementDriverHttpModel.BrokerPartitionView(
                                            0, MemberRole.LEADER, 7)))))),
                    new PlacementDriverHttpModel.TopologyResponse.Partitions(
                        List.of(
                            new PlacementDriverHttpModel.PartitionView(
                                new PlacementDriverHttpModel.PartitionView.Identity(
                                    0, "broker-0", 7, 0, PartitionStatus.REBALANCING, true),
                                new PlacementDriverHttpModel.PartitionView.Membership(
                                    List.of("broker-1"), List.of())))))));
    assertEquals("demo", node.get("clusterId").asText());
    assertEquals(12, node.get("version").asLong());
    assertEquals(3, node.get("clusterSize").asInt());
    assertEquals(5, node.get("partitionSize").asInt());
    assertEquals(2, node.get("replicationFactor").asInt());
    assertEquals("2024-01-01T00:00:00Z", node.get("updatedAt").asText());
    JsonNode broker = node.get("brokers").get(0);
    assertEquals("broker-0", broker.get("brokerId").asText());
    assertEquals(26501, broker.get("port").asInt());
    assertEquals(26601, broker.get("raftPort").asInt());
    assertEquals(30_000, broker.get("renewalTimeoutMillis").asLong());
    assertEquals("ONLINE", broker.get("status").asText());
    assertEquals(0, broker.get("partitions").get(0).get("partitionId").asInt());
    assertEquals("LEADER", broker.get("partitions").get(0).get("role").asText());
    JsonNode partition = node.get("partitions").get(0);
    assertEquals(0, partition.get("partitionId").asInt());
    assertEquals("broker-0", partition.get("leaderId").asText());
    assertEquals("broker-1", partition.get("replicas").get(0).asText());
    assertTrue(partition.get("learners").isEmpty());
    assertEquals(7, partition.get("term").asLong());
    assertEquals("REBALANCING", partition.get("status").asText());
    assertTrue(partition.get("serviceReady").asBoolean());
  }

  @Test
  void workerRegistryUsesLastRenewal() throws Exception {
    final JsonNode worker =
        MAPPER
            .readTree(
                MAPPER.writeValueAsBytes(
                    new PlacementDriverHttpModel.WorkerRegistryResponse(
                        "2024-01-01T00:05:00Z",
                        1,
                        1,
                        0,
                        List.of(
                            new PlacementDriverHttpModel.WorkerView(
                                new PlacementDriverHttpModel.WorkerView.Identity(
                                    "demo", "tenant-a", "worker-0", "payments", "payment"),
                                new PlacementDriverHttpModel.WorkerView.Execution(32, 1, 4),
                                new PlacementDriverHttpModel.WorkerView.Polling(
                                    30_000, 10_000, 100, List.of("orderId")),
                                new PlacementDriverHttpModel.WorkerView.Registration(
                                    "1.2.3",
                                    "2024-01-01T00:00:00Z",
                                    "2024-01-01T00:04:50Z",
                                    "2024-01-01T00:00:00Z",
                                    30_000),
                                new PlacementDriverHttpModel.WorkerView.Status(
                                    WorkerStatus.ONLINE))))))
            .path("workers")
            .get(0);

    assertEquals("2024-01-01T00:04:50Z", worker.path("lastRenewal").asText());
    assertFalse(worker.has("lastHeartbeat"));
  }

  @Test
  void clusterConfigAndErrorShapes() throws Exception {
    JsonNode config =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.ClusterConfigResponse(
                    "demo", 9, 3, 5, 2, List.of("broker-0", "broker-1", "broker-2"))));
    assertEquals("demo", config.get("clusterId").asText());
    assertEquals(9, config.get("clusterKey").asLong());
    assertEquals(3, config.get("clusterSize").asInt());
    assertEquals(5, config.get("partitionSize").asInt());
    assertEquals(2, config.get("replicationFactor").asInt());
    assertEquals(3, config.get("voterRoster").size());
    assertEquals("broker-2", config.get("voterRoster").get(2).asText());

    JsonNode error =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.ErrorResponse("NO_BROKER_REGISTERED", "demo")));
    assertEquals("NO_BROKER_REGISTERED", error.get("error").asText());
    assertEquals("demo", error.get("clusterId").asText());
  }

  @Test
  void placementDriverConfigurationShape() throws Exception {
    final JsonNode configuration =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.PlacementDriverConfigurationResponse(
                    8,
                    new PlacementDriverHttpModel.PlacementDriverTargetConfiguration(
                        4, List.of("meta-0", "meta-1", "meta-2", "meta-3")),
                    new PlacementDriverHttpModel.PlacementDriverActualConfiguration(
                        "meta-0", 42, List.of("meta-0", "meta-1", "meta-2"), List.of("meta-3")),
                    null)));

    assertEquals(8, configuration.path("epoch").asLong());
    assertEquals(4, configuration.path("target").path("count").asInt());
    assertEquals("meta-3", configuration.path("actual").path("learners").get(0).asText());
    assertTrue(configuration.get("activeChangeId") == null);
  }

  @Test
  void adminOperationErrorShape() throws Exception {
    final JsonNode error =
        MAPPER.readTree(
            MAPPER.writeValueAsBytes(
                new PlacementDriverHttpModel.AdminOperationErrorResponse(
                    "NOT_LEADER",
                    "Expected to add partition, but this placement-driver node is not the leader",
                    "127.0.0.1:26500")));
    assertEquals("NOT_LEADER", error.path("error").asText());
    assertEquals("127.0.0.1:26500", error.path("leaderEndpoint").asText());
  }
}
