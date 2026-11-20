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
package io.taskite.broker.client.impl;

import io.taskite.broker.client.*;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.transport.BrokerClientTransport;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.BrokerEndpoint;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

public final class BrokerClientImpl implements BrokerClient {
  private final BrokerRequestManager requestManager;

  public BrokerClientImpl(
      final BrokerClientTransport transport, final BrokerClientConfig clientConfig) {
    this(
        Objects.requireNonNull(transport, "transport"),
        Objects.requireNonNull(clientConfig, "clientConfig").requestTimeout(),
        clientConfig.maxRetries());
  }

  public BrokerClientImpl(
      final BrokerClientTransport transport, final Duration requestTimeout, final int maxRetries) {
    this(
        new BrokerRequestManager(
            Objects.requireNonNull(transport, "transport"),
            Objects.requireNonNull(requestTimeout, "requestTimeout"),
            maxRetries));
  }

  public BrokerClientImpl(final BrokerRequestManager requestManager) {
    this.requestManager = Objects.requireNonNull(requestManager, "requestManager");
  }

  @Override
  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final BrokerEndpoint endpoint, final BrokerRequest<T> request) {
    return requestManager.sendRequest(endpoint, request);
  }

  @Override
  public <T> ActorFuture<BrokerResponse<T>> sendRequest(
      final Supplier<BrokerEndpoint> endpointSupplier, final BrokerRequest<T> request) {
    return requestManager.sendRequest(endpointSupplier, request);
  }

  @Override
  public void close() throws Exception {
    requestManager.close();
  }
}
