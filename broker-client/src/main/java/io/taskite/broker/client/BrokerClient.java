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
package io.taskite.broker.client;

import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.util.function.Supplier;

public interface BrokerClient extends AutoCloseable {
  <T> ActorFuture<BrokerResponse<T>> sendRequest(BrokerEndpoint endpoint, BrokerRequest<T> request);

  default <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final Supplier<BrokerEndpoint> endpointSupplier, final BrokerRequest<T> request) {
    return sendRequest(endpointSupplier.get(), request);
  }

  @Override
  default void close() throws Exception {}
}
