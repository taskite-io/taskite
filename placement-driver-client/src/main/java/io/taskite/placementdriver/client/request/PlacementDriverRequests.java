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
package io.taskite.placementdriver.client.request;

import static io.taskite.placementdriver.protocol.BrokerPartitionState.newBuilder;

import io.taskite.placementdriver.protocol.BrokerPartitionState.Builder;
import io.taskite.placementdriver.protocol.DeregisterBrokerRequest;
import io.taskite.placementdriver.protocol.GetBrokerTopologyRequest;
import io.taskite.placementdriver.protocol.GetClusterConfigRequest;
import io.taskite.placementdriver.protocol.RegisterBrokerRequest;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.topology.BrokerPartitionState;
import java.util.List;

public final class PlacementDriverRequests {
  public static final long DEFAULT_RENEWAL_TIMEOUT_MILLIS = 30_000L;

  private PlacementDriverRequests() {}

  public static GetClusterConfigRequest getClusterConfig(final String clusterId) {
    return GetClusterConfigRequest.newBuilder().setClusterId(requireClusterId(clusterId)).build();
  }

  public static GetBrokerTopologyRequest getBrokerTopology(final String clusterId) {
    return GetBrokerTopologyRequest.newBuilder().setClusterId(requireClusterId(clusterId)).build();
  }

  public static RegisterBrokerRequest registerBroker(
      final String clusterId,
      final String brokerId,
      final String host,
      final int port,
      final int raftPort,
      final String version,
      final long renewalTimeoutMillis,
      final List<BrokerPartitionState> partitions) {
    if (renewalTimeoutMillis <= 0) {
      throw new IllegalArgumentException("renewalTimeoutMillis must be positive");
    }
    final RegisterBrokerRequest.Builder request =
        RegisterBrokerRequest.newBuilder()
            .setClusterId(requireClusterId(clusterId))
            .setBrokerId(requireText(brokerId, "brokerId"))
            .setHost(requireText(host, "host"))
            .setPort(port)
            .setRaftPort(raftPort)
            .setVersion(version == null ? "" : version)
            .setRenewalTimeoutMillis(renewalTimeoutMillis);
    safePartitions(partitions).forEach(partition -> request.addPartitions(toProto(partition)));
    return request.build();
  }

  public static RegisterBrokerRequest registerBroker(
      final String clusterId,
      final String brokerId,
      final String host,
      final int port,
      final int raftPort,
      final String version,
      final List<BrokerPartitionState> partitions) {
    return registerBroker(
        clusterId,
        brokerId,
        host,
        port,
        raftPort,
        version,
        DEFAULT_RENEWAL_TIMEOUT_MILLIS,
        partitions);
  }

  public static RenewBrokerRequest renewBroker(
      final String clusterId, final String brokerId, final List<BrokerPartitionState> partitions) {
    final RenewBrokerRequest.Builder request =
        RenewBrokerRequest.newBuilder()
            .setClusterId(requireClusterId(clusterId))
            .setBrokerId(requireText(brokerId, "brokerId"));
    safePartitions(partitions).forEach(partition -> request.addPartitions(toProto(partition)));
    return request.build();
  }

  public static DeregisterBrokerRequest deregisterBroker(
      final String clusterId, final String brokerId) {
    return DeregisterBrokerRequest.newBuilder()
        .setClusterId(requireClusterId(clusterId))
        .setBrokerId(requireText(brokerId, "brokerId"))
        .build();
  }

  private static Builder toProto(final BrokerPartitionState partition) {
    return newBuilder()
        .setPartitionId(partition.partitionId())
        .setRole(partition.role().name())
        .setTerm(partition.term())
        .setLifecycleState(partition.lifecycleState().name())
        .setMatchIndex(partition.matchIndex())
        .setLag(partition.lag())
        .setReadyForPromotion(partition.readyForPromotion())
        .setPriority(partition.priority())
        .setLastOperationId(partition.lastOpId())
        .setLastOperationStatus(partition.lastOpStatus().name())
        .setRecoveryEpoch(partition.recoveryEpoch())
        .setElection(BrokerPartitionElectionRequestMapper.toProto(partition.election()));
  }

  private static List<BrokerPartitionState> safePartitions(
      final List<BrokerPartitionState> partitions) {
    return partitions == null ? List.of() : partitions;
  }

  private static String requireClusterId(final String clusterId) {
    return requireText(clusterId, "clusterId");
  }

  private static String requireText(final String value, final String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }
}
