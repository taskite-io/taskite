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
package io.taskite.broker.client.request;

import io.taskite.broker.client.RequestDispatchStrategy;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.util.BufferWriter;
import java.util.Optional;
import org.agrona.DirectBuffer;

public interface BrokerRequest<T> {
  void setPartitionId(int partitionId);

  void setClusterId(String clusterId);

  default void setTenantId(final String tenantId) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support tenant context");
  }

  String getClusterId();

  int getPartitionId();

  void serializeValue();

  byte[] serializeRequest();

  /** Administration retries belong to the placement driver's durable plan execution loop. */
  default boolean allowsTransportRetry() {
    return true;
  }

  BrokerResponse<T> getResponse(DirectBuffer responseBuffer);

  BufferWriter getRequestWriter();

  /**
   * Overridden by requests that carry their own partition-selection policy. When empty, the
   * client's default {@link RequestDispatchStrategy} is used to pick a partition.
   */
  default Optional<RequestDispatchStrategy> requestDispatchStrategy() {
    return Optional.empty();
  }

  /** Returns true when the request already carries enough data to select a specific partition. */
  default boolean addressesSpecificPartition() {
    return false;
  }

  /**
   * The fenced-recovery routing epoch (G2) this request is being routed under, as observed from the
   * gateway's topology at resolution time. Defaults to {@code 0} for requests that predate fenced
   * recovery. The value never travels on the wire — it is a gateway-internal routing key used to
   * partition the transport's channel cache so a superseded generation's channels are evicted.
   */
  default long getRoutingEpoch() {
    return 0L;
  }

  /** Stamps the routing epoch resolved from topology; see {@link #getRoutingEpoch()}. */
  default void setRoutingEpoch(final long routingEpoch) {}

  BrokerResponse<T> mapResponse(int partitionId, long key, UnifiedRecordValue value);
}
