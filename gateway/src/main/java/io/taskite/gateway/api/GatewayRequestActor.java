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
package io.taskite.gateway.api;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorSchedulingService;
import io.taskite.scheduler.ConcurrencyControl;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/** Owns one request's actor, gRPC context and terminal cleanup. */
final class GatewayRequestActor<ResponseT> {
  private static final AtomicLong REQUEST_SEQUENCE = new AtomicLong();

  private final ActorSchedulingService scheduler;
  private final Context grpcContext;
  private final BiConsumer<ConcurrencyControl, StreamObserver<ResponseT>> request;
  private final Actor actor;
  private final ManagedObserver<ResponseT> responseObserver;

  private GatewayRequestActor(
      final ActorSchedulingService scheduler,
      final Context grpcContext,
      final StreamObserver<ResponseT> responseObserver,
      final BiConsumer<ConcurrencyControl, StreamObserver<ResponseT>> request) {
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    this.grpcContext = Objects.requireNonNull(grpcContext, "grpcContext");
    this.request = Objects.requireNonNull(request, "request");
    actor =
        Actor.newActor()
            .name("gateway-request-" + REQUEST_SEQUENCE.incrementAndGet())
            .actorStartedHandler(this::start)
            .build();
    this.responseObserver = new ManagedObserver<>(responseObserver, actor);
  }

  static <ResponseT> void submit(
      final ActorSchedulingService scheduler,
      final Context grpcContext,
      final StreamObserver<ResponseT> responseObserver,
      final BiConsumer<ConcurrencyControl, StreamObserver<ResponseT>> request) {
    new GatewayRequestActor<>(scheduler, grpcContext, responseObserver, request).submit();
  }

  private void submit() {
    scheduler
        .submitActor(actor)
        .onComplete(
            (ignored, error) -> {
              if (error != null) {
                responseObserver.onError(GatewayErrorMapper.toStatusException(error));
              }
            },
            Runnable::run);
  }

  private void start(final ConcurrencyControl control) {
    grpcContext.run(
        () -> {
          try {
            request.accept(control, responseObserver);
          } catch (final Throwable error) {
            responseObserver.onError(GatewayErrorMapper.toStatusException(error));
          }
        });
  }

  private static final class ManagedObserver<ResponseT> implements StreamObserver<ResponseT> {
    private final StreamObserver<ResponseT> delegate;
    private final Actor actor;
    private final AtomicBoolean terminated = new AtomicBoolean();

    private ManagedObserver(final StreamObserver<ResponseT> delegate, final Actor actor) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.actor = actor;
    }

    @Override
    public void onNext(final ResponseT value) {
      if (!terminated.get()) {
        delegate.onNext(value);
      }
    }

    @Override
    public void onError(final Throwable error) {
      if (terminated.compareAndSet(false, true)) {
        try {
          delegate.onError(error);
        } finally {
          actor.closeAsync();
        }
      }
    }

    @Override
    public void onCompleted() {
      if (terminated.compareAndSet(false, true)) {
        try {
          delegate.onCompleted();
        } finally {
          actor.closeAsync();
        }
      }
    }
  }
}
