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
package io.taskite.placementdriver.client.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.placementdriver.protocol.Broker;
import io.taskite.placementdriver.protocol.BrokerTopology;
import io.taskite.placementdriver.protocol.GetClusterConfigResponse;
import io.taskite.placementdriver.protocol.Partition;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import java.util.List;
import org.junit.jupiter.api.Test;

final class PlacementDriverMapperTest {
  @Test
  void brokerRequestFactoryBuildsGeneratedProto() {
    final var request =
        PlacementDriverRequests.registerBroker(
            "demo",
            "broker-1",
            "127.0.0.1",
            26501,
            26601,
            "0.1.0",
            45_000,
            List.of(new BrokerPartitionState(1, MemberRole.LEADER, 7)));

    assertEquals("demo", request.getClusterId());
    assertEquals("broker-1", request.getBrokerId());
    assertEquals(26601, request.getRaftPort());
    assertEquals(45_000, request.getRenewalTimeoutMillis());
    assertEquals("LEADER", request.getPartitions(0).getRole());
  }

  @Test
  void mapsGeneratedResponsesToClientDomainModels() {
    final var config =
        PlacementDriverResponseMapper.toClusterConfig(
            GetClusterConfigResponse.newBuilder()
                .setClusterId("demo")
                .setClusterKey(1)
                .setClusterSize(3)
                .setPartitionSize(5)
                .setReplicationFactor(2)
                .addRemovedBrokers("broker-4")
                .build());
    final var topology =
        PlacementDriverResponseMapper.toClusterTopology(
            BrokerTopology.newBuilder()
                .setClusterId("demo")
                .setVersion(9)
                .setPartitionSize(5)
                .setReplicationFactor(1)
                .setUpdatedAtEpochMillis(1)
                .addBrokers(
                    Broker.newBuilder()
                        .setBrokerId("broker-1")
                        .setHost("127.0.0.1")
                        .setPort(26501)
                        .setRaftPort(26601))
                .addPartitions(
                    Partition.newBuilder()
                        .setPartitionId(1)
                        .setLeaderId("broker-1")
                        .addReplicas("broker-1")
                        .setStatus("ACTIVE"))
                .build());

    assertEquals(List.of("broker-4"), config.removedBrokers());
    assertEquals(1, config.clusterKey());
    assertEquals("broker-1", topology.partition(1).orElseThrow().leaderId());
    assertEquals(5, topology.partitionSize());
    assertEquals(26601, topology.brokers().get("broker-1").raftPort());
  }

  @Test
  void rejectsMissingAndOutOfRangeClusterKeys() {
    final var response = GetClusterConfigResponse.newBuilder().setClusterId("demo");
    assertThrows(
        IllegalArgumentException.class,
        () -> PlacementDriverResponseMapper.toClusterConfig(response.build()));
    assertThrows(
        IllegalArgumentException.class,
        () -> PlacementDriverResponseMapper.toClusterConfig(response.setClusterKey(-1).build()));
    assertThrows(
        IllegalArgumentException.class,
        () -> PlacementDriverResponseMapper.toClusterConfig(response.setClusterKey(0).build()));
    assertThrows(
        IllegalArgumentException.class,
        () -> PlacementDriverResponseMapper.toClusterConfig(response.setClusterKey(16).build()));
    assertEquals(
        15,
        PlacementDriverResponseMapper.toClusterConfig(response.setClusterKey(15).build())
            .clusterKey());
  }
}
