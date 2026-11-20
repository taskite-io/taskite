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
package io.taskite.broker.client.transport;

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

public final class BrokerRequestGrpc {
  public static final String SERVICE_NAME = "io.taskite.broker.RequestService";
  public static final MethodDescriptor<byte[], byte[]> SEND_REQUEST_METHOD = method("SendRequest");

  private BrokerRequestGrpc() {}

  public static ActorFuture<byte[]> sendRequest(final Channel channel, final byte[] request) {
    return unary(channel, SEND_REQUEST_METHOD, request, CallOptions.DEFAULT.withWaitForReady());
  }

  public static ActorFuture<byte[]> sendRequest(
      final Channel channel, final byte[] request, final Duration timeout) {
    return unary(
        channel,
        SEND_REQUEST_METHOD,
        request,
        CallOptions.DEFAULT
            .withWaitForReady()
            .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS));
  }

  private static ActorFuture<byte[]> unary(
      final Channel channel,
      final MethodDescriptor<byte[], byte[]> method,
      final byte[] request,
      final CallOptions callOptions) {
    final CompletableActorFuture<byte[]> future = new CompletableActorFuture<>();
    ClientCalls.asyncUnaryCall(
        channel.newCall(method, callOptions),
        request,
        new StreamObserver<>() {
          private byte[] response;

          @Override
          public void onNext(final byte[] value) {
            response = value;
          }

          @Override
          public void onError(final Throwable throwable) {
            future.completeExceptionally(throwable);
          }

          @Override
          public void onCompleted() {
            if (response == null) {
              future.completeExceptionally(new IllegalStateException("empty broker response"));
            } else {
              future.complete(response);
            }
          }
        });
    return future;
  }

  private static MethodDescriptor<byte[], byte[]> method(final String name) {
    return MethodDescriptor.<byte[], byte[]>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, name))
        .setRequestMarshaller(byteMarshaller())
        .setResponseMarshaller(byteMarshaller())
        .build();
  }

  static ServiceDescriptor serviceDescriptor() {
    return ServiceDescriptor.newBuilder(SERVICE_NAME).addMethod(SEND_REQUEST_METHOD).build();
  }

  private static MethodDescriptor.Marshaller<byte[]> byteMarshaller() {
    return new MethodDescriptor.Marshaller<>() {
      @Override
      public InputStream stream(final byte[] value) {
        return new ByteArrayInputStream(value);
      }

      @Override
      public byte[] parse(final InputStream stream) {
        try {
          return stream.readAllBytes();
        } catch (final IOException error) {
          throw new IllegalStateException("failed to parse broker command grpc payload", error);
        }
      }
    };
  }
}
