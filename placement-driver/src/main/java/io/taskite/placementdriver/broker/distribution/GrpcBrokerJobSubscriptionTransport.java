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
package io.taskite.placementdriver.broker.distribution;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerCommandDistributionRequest;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.util.Objects;

public final class GrpcBrokerJobSubscriptionTransport implements BrokerJobSubscriptionTransport {
  private final BrokerClient brokerClient;

  public GrpcBrokerJobSubscriptionTransport(final BrokerClient brokerClient) {
    this.brokerClient = Objects.requireNonNull(brokerClient, "brokerClient");
  }

  @Override
  public ActorFuture<Void> distributeJobSubscription(
      final BrokerEndpoint endpoint, final BrokerJobSubscriptionDistribution distribution) {
    final CommandDistributionRecord<?> request =
        new CommandDistributionRecord<UnifiedRecordValue>()
            .setDistributionKey(distribution.distributionKey())
            .setClusterId(distribution.clusterId())
            .setPartitionId(distribution.partitionId())
            .setValueType(distribution.valueType())
            .setIntent(distribution.intent())
            .setValue(distribution.value());
    return brokerClient
        .sendRequest(
            endpoint, new BrokerCommandDistributionRequest(distribution.partitionId(), request))
        .thenApply(ignored -> null);
  }

  @Override
  public void close() throws Exception {
    brokerClient.close();
  }
}
