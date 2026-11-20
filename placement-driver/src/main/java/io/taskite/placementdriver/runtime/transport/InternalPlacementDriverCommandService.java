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
package io.taskite.placementdriver.runtime.transport;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

public abstract class InternalPlacementDriverCommandService implements BindableService {
  public void executeCommand(final byte[] request, final StreamObserver<byte[]> responseObserver) {
    responseObserver.onError(
        Status.UNIMPLEMENTED.withDescription("method is not implemented").asException());
  }

  @Override
  public final ServerServiceDefinition bindService() {
    return ServerServiceDefinition.builder(InternalPlacementDriverCommandGrpc.serviceDescriptor())
        .addMethod(
            InternalPlacementDriverCommandGrpc.EXECUTE_COMMAND_METHOD,
            ServerCalls.asyncUnaryCall(this::executeCommand))
        .build();
  }
}
