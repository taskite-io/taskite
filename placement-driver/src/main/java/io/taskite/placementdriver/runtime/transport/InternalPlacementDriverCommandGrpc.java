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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public final class InternalPlacementDriverCommandGrpc {
  public static final String SERVICE_NAME = "io.taskite.placementdriver.CommandService";
  public static final MethodDescriptor<byte[], byte[]> EXECUTE_COMMAND_METHOD =
      MethodDescriptor.<byte[], byte[]>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(
              MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ExecuteCommand"))
          .setRequestMarshaller(ByteMarshaller.INSTANCE)
          .setResponseMarshaller(ByteMarshaller.INSTANCE)
          .build();

  private InternalPlacementDriverCommandGrpc() {}

  public static ActorFuture<byte[]> executeCommand(
      final Channel channel, final byte[] request, final Duration timeout) {
    final CompletableActorFuture<byte[]> future = new CompletableActorFuture<>();
    final CallOptions options =
        CallOptions.DEFAULT
            .withWaitForReady()
            .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS);
    ClientCalls.asyncUnaryCall(
        channel.newCall(EXECUTE_COMMAND_METHOD, options),
        request,
        new StreamObserver<>() {
          @Override
          public void onNext(final byte[] value) {
            future.complete(value);
          }

          @Override
          public void onError(final Throwable throwable) {
            future.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {
            // Unary responses complete the future in onNext.
          }
        });
    return future;
  }

  public static ServiceDescriptor serviceDescriptor() {
    return ServiceDescriptor.newBuilder(SERVICE_NAME).addMethod(EXECUTE_COMMAND_METHOD).build();
  }

  private enum ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {
    INSTANCE;

    @Override
    public InputStream stream(final byte[] value) {
      return new ByteArrayInputStream(value);
    }

    @Override
    public byte[] parse(final InputStream stream) {
      try {
        return stream.readAllBytes();
      } catch (final IOException error) {
        throw new IllegalStateException("failed to parse internal placement-driver command", error);
      }
    }
  }
}
