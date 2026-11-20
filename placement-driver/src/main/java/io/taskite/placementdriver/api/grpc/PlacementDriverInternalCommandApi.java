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

import io.grpc.stub.StreamObserver;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.runtime.transport.InternalPlacementDriverCommandService;
import java.util.Objects;

/** PD-internal follower-to-leader transport for runtime-generated Raft commands. */
final class PlacementDriverInternalCommandApi extends InternalPlacementDriverCommandService {
  private final PlacementDriverEndpointManager endpointManager;

  PlacementDriverInternalCommandApi(final PlacementDriverEndpointManager endpointManager) {
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
  }

  @Override
  public void executeCommand(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    endpointManager.submitCommand(request, responseObserver);
  }
}
