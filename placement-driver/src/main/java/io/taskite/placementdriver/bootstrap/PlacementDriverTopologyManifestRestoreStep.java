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

import io.taskite.placementdriver.runtime.PlacementDriverTopologyManifestReader;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.util.HostPort;

public final class PlacementDriverTopologyManifestRestoreStep
    implements PlacementDriverStartupStep<PlacementDriverStartupContext> {
  private final PlacementDriverTopologyManifestReader reader =
      new PlacementDriverTopologyManifestReader();

  @Override
  public String name() {
    return "Topology Manifest";
  }

  @Override
  public ActorFuture<PlacementDriverStartupContext> startup(
      final PlacementDriverStartupContext context) {
    return context
        .concurrencyControl()
        .call(
            () -> {
              reader
                  .read(context.config().storage().dataDir(), context.config().nodeId())
                  .ifPresent(
                      manifest -> {
                        context.topologyManifest(manifest);
                        context.bootstrapPeers(reader.toRaftPeers(manifest));
                        manifest.members().stream()
                            .filter(member -> manifest.leaderId().equals(member.memberId()))
                            .filter(member -> member.grpcPort() > 0)
                            .findFirst()
                            .ifPresent(
                                member ->
                                    context.bootstrapLeaderEndpoint(
                                        HostPort.format(member.host(), member.grpcPort())));
                      });
              return context;
            });
  }
}
