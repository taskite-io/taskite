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
package io.taskite.placementdriver.client;

import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.CreateTimerSubscriptionResponse;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionRequest;
import io.taskite.placementdriver.protocol.DeleteTimerSubscriptionResponse;
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
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.ClusterTopology;

public interface Client extends AutoCloseable {
  ActorFuture<ClusterConfig> clusterConfig(GetClusterConfigRequest request);

  ActorFuture<ClusterTopology> topology(GetBrokerTopologyRequest request);

  AutoCloseable watchBrokerTopology(
      GetBrokerTopologyRequest request, TopologyChangeListener listener);

  ActorFuture<RegisterBrokerResponse> registerBroker(RegisterBrokerRequest request);

  ActorFuture<RenewBrokerResponse> renewBroker(RenewBrokerRequest request);

  ActorFuture<DeregisterBrokerResponse> deregisterBroker(DeregisterBrokerRequest request);

  ActorFuture<RegisterWorkerResponse> registerWorker(RegisterWorkerRequest request);

  ActorFuture<RenewWorkerResponse> renewWorker(RenewWorkerRequest request);

  ActorFuture<DeregisterWorkerResponse> deregisterWorker(DeregisterWorkerRequest request);

  /**
   * Submits a typed request that PD must persist, coordinate, and distribute to Broker partitions.
   */
  ActorFuture<DistributeCommandResponse> distributeCommand(DistributeCommandRequest request);

  ActorFuture<CreateTimerSubscriptionResponse> createTimerSubscription(
      CreateTimerSubscriptionRequest request);

  ActorFuture<DeleteTimerSubscriptionResponse> deleteTimerSubscription(
      DeleteTimerSubscriptionRequest request);

  default ActorFuture<ClusterConfig> clusterConfig(final String clusterId) {
    return clusterConfig(
        GetClusterConfigRequest.newBuilder().setClusterId(requireClusterId(clusterId)).build());
  }

  default ActorFuture<ClusterTopology> topology(final String clusterId) {
    return topology(
        GetBrokerTopologyRequest.newBuilder().setClusterId(requireClusterId(clusterId)).build());
  }

  default AutoCloseable watchBrokerTopology(
      final String clusterId, final TopologyChangeListener listener) {
    return watchBrokerTopology(
        GetBrokerTopologyRequest.newBuilder().setClusterId(requireClusterId(clusterId)).build(),
        listener);
  }

  private static String requireClusterId(final String clusterId) {
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId must not be blank");
    }
    return clusterId;
  }

  @Override
  void close();
}
