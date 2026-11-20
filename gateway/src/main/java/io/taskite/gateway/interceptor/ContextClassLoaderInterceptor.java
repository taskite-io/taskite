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
package io.taskite.gateway.interceptor;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.Objects;
import java.util.function.Supplier;

/** Runs an external interceptor and its request callbacks with its plugin class loader active. */
public final class ContextClassLoaderInterceptor implements ServerInterceptor {
  private final ServerInterceptor delegate;
  private final ClassLoader classLoader;

  public ContextClassLoaderInterceptor(
      final ServerInterceptor delegate, final ClassLoader classLoader) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.classLoader = Objects.requireNonNull(classLoader, "classLoader");
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final ServerCall.Listener<ReqT> listener =
        withClassLoader(() -> delegate.interceptCall(call, headers, next));
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
      @Override
      public void onMessage(final ReqT message) {
        withClassLoader(
            () -> {
              super.onMessage(message);
              return null;
            });
      }

      @Override
      public void onHalfClose() {
        withClassLoader(
            () -> {
              super.onHalfClose();
              return null;
            });
      }

      @Override
      public void onCancel() {
        withClassLoader(
            () -> {
              super.onCancel();
              return null;
            });
      }

      @Override
      public void onComplete() {
        withClassLoader(
            () -> {
              super.onComplete();
              return null;
            });
      }

      @Override
      public void onReady() {
        withClassLoader(
            () -> {
              super.onReady();
              return null;
            });
      }
    };
  }

  private <T> T withClassLoader(final Supplier<T> action) {
    final Thread thread = Thread.currentThread();
    final ClassLoader previous = thread.getContextClassLoader();
    try {
      thread.setContextClassLoader(classLoader);
      return action.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }
}
