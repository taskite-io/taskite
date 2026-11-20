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
package io.taskite.placementdriver.broker.cluster;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.placementdriver.broker.request.BrokerApplyTopologyOperationRequest;
import io.taskite.placementdriver.broker.request.BrokerInstallPartitionAuthorityRequest;
import io.taskite.placementdriver.broker.request.BrokerReadPartitionConfigurationRequest;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.util.Objects;

public final class GrpcBrokerClusterChangeTransport implements BrokerClusterChangeTransport {
  private final BrokerClient brokerClient;

  public GrpcBrokerClusterChangeTransport(final BrokerClient brokerClient) {
    this.brokerClient = Objects.requireNonNull(brokerClient, "brokerClient");
  }

  @Override
  public ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
      final BrokerEndpoint endpoint, final BrokerApplyTopologyOperationRequest request) {
    return brokerClient
        .sendRequest(endpoint, request)
        .thenApply(
            response ->
                new BrokerClusterChangeDispatchResult(
                    response.getResponse().opId(), response.getResponse().applied()));
  }

  @Override
  public ActorFuture<BrokerPartitionConfigurationRecord> readConfiguration(
      final BrokerEndpoint endpoint, final BrokerReadPartitionConfigurationRequest request) {
    return brokerClient.sendRequest(endpoint, request).thenApply(BrokerResponse::getResponse);
  }

  @Override
  public ActorFuture<BrokerPartitionConfigurationRecord> installAuthority(
      final BrokerEndpoint endpoint, final BrokerInstallPartitionAuthorityRequest request) {
    return brokerClient.sendRequest(endpoint, request).thenApply(BrokerResponse::getResponse);
  }

  @Override
  public void close() throws Exception {
    brokerClient.close();
  }
}
