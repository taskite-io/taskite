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
package io.taskite.placementdriver.api.grpc;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The placement-driver's business gRPC API server. Since the jraft cut-over, peer raft RPCs are
 * served by the shared {@code JRaftServer} on its own dedicated raft port, so this server hosts
 * only the placement-driver API service.
 */
public final class PlacementDriverGrpcServer implements AutoCloseable {
  private final int port;
  private final PlacementDriverEndpointManager endpointManager;
  private Server server;

  public PlacementDriverGrpcServer(
      final int port, final PlacementDriverEndpointManager endpointManager) {
    this.port = port;
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  public void start() throws IOException {
    server =
        NettyServerBuilder.forPort(port)
            .addService(new PlacementDriverGrpcApi(endpointManager))
            .addService(new PlacementDriverInternalCommandApi(endpointManager))
            .build()
            .start();
  }

  public int port() {
    return server == null ? port : server.getPort();
  }

  @Override
  public void close() {
    if (server == null) {
      return;
    }
    server.shutdown();
    try {
      if (!server.awaitTermination(1, TimeUnit.SECONDS)) {
        server.shutdownNow();
      }
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      server.shutdownNow();
    }
  }
}
