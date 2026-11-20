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
package io.taskite.placementdriver.bootstrap;

import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.scheduler.future.ActorFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PlacementDriverHealthCheckStep
    implements PlacementDriverStartupStep<PlacementDriverStartupContext> {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementDriverHealthCheckStep.class);

  @Override
  public String name() {
    return "Health Check";
  }

  @Override
  public ActorFuture<PlacementDriverStartupContext> startup(
      final PlacementDriverStartupContext context) {
    return context
        .concurrencyControl()
        .call(
            () -> {
              final PlacementDriverConfiguration config = context.config();
              final String raftPeers =
                  config.consensus().bootstrapPeers().stream()
                      .map(
                          peer ->
                              peer.memberId()
                                  + "@"
                                  + peer.host()
                                  + ":"
                                  + peer.grpcPort()
                                  + ":"
                                  + peer.raftPort())
                      .collect(Collectors.joining(","));
              LOG.info(
                  "placement-driver node {} started at http://{}:{}, grpc={}:{}, raft={}:{}, "
                      + "raftPeers=[{}]",
                  config.nodeId(),
                  config.network().host(),
                  context.httpPort(),
                  config.network().host(),
                  context.grpcServer().port(),
                  config.network().host(),
                  context.raftRuntime().localRaftPort(),
                  raftPeers);
              return context;
            });
  }
}
