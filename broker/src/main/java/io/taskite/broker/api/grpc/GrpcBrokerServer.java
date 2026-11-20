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

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.taskite.broker.api.BrokerEndpointManager;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The broker's business and administration gRPC API server. Peer raft RPCs (vote, append,
 * install-snapshot) are served by the shared {@code JRaftServer} on its own dedicated raft port, so
 * this server hosts the common request service.
 */
public final class GrpcBrokerServer implements AutoCloseable {
  private final int port;
  private final BrokerEndpointManager endpointManager;
  private Server server;

  public GrpcBrokerServer(final int port, final BrokerEndpointManager endpointManager) {
    this.port = port;
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  public void start() throws IOException {
    server =
        NettyServerBuilder.forPort(port)
            .addService(new BrokerRequestApi(endpointManager))
            .build()
            .start();
  }

  public int port() {
    if (server == null) {
      return port;
    }
    return server.getPort();
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  @Override
  public void close() {
    if (server != null) {
      server.shutdown();
      try {
        if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
          server.shutdownNow();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        server.shutdownNow();
      }
    }
  }
}
