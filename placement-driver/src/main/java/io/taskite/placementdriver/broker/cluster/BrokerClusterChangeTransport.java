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

import io.taskite.placementdriver.broker.request.BrokerApplyTopologyOperationRequest;
import io.taskite.placementdriver.broker.request.BrokerInstallPartitionAuthorityRequest;
import io.taskite.placementdriver.broker.request.BrokerReadPartitionConfigurationRequest;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.BrokerEndpoint;

public interface BrokerClusterChangeTransport extends AutoCloseable {
  ActorFuture<BrokerClusterChangeDispatchResult> dispatchBrokerClusterChange(
      BrokerEndpoint endpoint, BrokerApplyTopologyOperationRequest request);

  default ActorFuture<BrokerPartitionConfigurationRecord> readConfiguration(
      BrokerEndpoint endpoint, BrokerReadPartitionConfigurationRequest request) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("configuration read is unsupported"));
  }

  default ActorFuture<BrokerPartitionConfigurationRecord> installAuthority(
      BrokerEndpoint endpoint, BrokerInstallPartitionAuthorityRequest request) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("authority installation is unsupported"));
  }

  @Override
  default void close() throws Exception {}
}
