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
package io.taskite.broker.api.grpc;

import io.grpc.stub.StreamObserver;
import io.taskite.broker.api.BrokerEndpointManager;
import io.taskite.broker.client.transport.BrokerRequestService;
import java.util.Objects;

public final class BrokerRequestApi extends BrokerRequestService {
  private final BrokerEndpointManager endpointManager;

  public BrokerRequestApi(final BrokerEndpointManager endpointManager) {
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  @Override
  protected void sendRequest(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    endpointManager.sendRequest(request, responseObserver);
  }
}
