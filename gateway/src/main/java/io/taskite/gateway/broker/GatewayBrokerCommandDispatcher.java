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
package io.taskite.gateway.broker;

import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.request.BrokerRequest;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.response.PartitionNotReadyException;
import io.taskite.gateway.routing.GatewayBrokerRouter;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;

/** Applies Gateway routing decisions and delegates requests to the Broker client. */
public final class GatewayBrokerCommandDispatcher {
  private final BrokerClient brokerClient;
  private final GatewayBrokerRouter router;

  public GatewayBrokerCommandDispatcher(
      final BrokerClient brokerClient, final GatewayBrokerRouter router) {
    this.brokerClient = Objects.requireNonNull(brokerClient, "brokerClient");
    this.router = Objects.requireNonNull(router, "router");
  }

  public <T> ActorFuture<BrokerResponse<T>> dispatch(
      final String clusterId, final BrokerRequest<T> request) {
    return dispatch(clusterId, request, () -> false);
  }

  public <T> ActorFuture<BrokerResponse<T>> dispatch(
      final String clusterId,
      final BrokerRequest<T> request,
      final BooleanSupplier cancellationRequested) {
    if (cancellationRequested.getAsBoolean()) {
      return CompletableActorFuture.completedExceptionally(
          new CancellationException("gateway request was cancelled"));
    }
    request.setClusterId(clusterId);
    final GatewayBrokerRouter.Route route = router.resolve(clusterId, request);
    return dispatchToPartition(clusterId, request, cancellationRequested, route.partitionId());
  }

  private <T> ActorFuture<BrokerResponse<T>> dispatchToPartition(
      final String clusterId,
      final BrokerRequest<T> request,
      final BooleanSupplier cancellationRequested,
      final int partitionId) {
    request.setPartitionId(partitionId);
    return brokerClient.sendRequest(
        () -> {
          if (cancellationRequested.getAsBoolean()) {
            throw new CancellationException("gateway request was cancelled");
          }
          final GatewayBrokerRouter.Route latest = router.routeFor(clusterId, partitionId);
          request.setRoutingEpoch(latest.routingEpoch());
          return latest.endpoint();
        },
        request);
  }

  /** Wait only after a typed rejection proves the command was not appended. */
  public <T> ActorFuture<BrokerResponse<T>> dispatchWhenReady(
      final String clusterId,
      final BrokerRequest<T> request,
      final BooleanSupplier cancellationRequested,
      final ConcurrencyControl control,
      final Duration routingTimeout) {
    final var waiting =
        new RoutingWait<T>(clusterId, request, cancellationRequested, control, routingTimeout);
    control.run(waiting::start);
    return waiting.result;
  }

  private final class RoutingWait<T> {
    private final String clusterId;
    private final BrokerRequest<T> request;
    private final BooleanSupplier cancelled;
    private final ConcurrencyControl control;
    private final Duration timeout;
    private final CompletableActorFuture<BrokerResponse<T>> result = new CompletableActorFuture<>();
    private ScheduledTimer retry;
    private ScheduledTimer expiry;
    private boolean expired;
    private boolean inFlight;
    private Throwable lastRoutingRejection;
    private Integer partitionId;

    private RoutingWait(
        final String clusterId,
        final BrokerRequest<T> request,
        final BooleanSupplier cancelled,
        final ConcurrencyControl control,
        final Duration timeout) {
      this.clusterId = clusterId;
      this.request = request;
      this.cancelled = cancelled;
      this.control = control;
      this.timeout = timeout;
    }

    private void start() {
      if (timeout.isZero() || timeout.isNegative()) {
        result.completeExceptionally(new PartitionNotReadyException());
        return;
      }
      result.onComplete(
          (ignored, error) -> {
            if (retry != null) {
              retry.cancel();
            }
            if (expiry != null) {
              expiry.cancel();
            }
          },
          control::run);
      expiry =
          control.schedule(
              timeout.isNegative() ? Duration.ZERO : timeout,
              () -> {
                expired = true;
                if (!inFlight && !result.isDone()) {
                  result.completeExceptionally(
                      lastRoutingRejection == null
                          ? new PartitionNotReadyException()
                          : lastRoutingRejection);
                }
              });
      attempt();
    }

    private void attempt() {
      if (result.isDone()) {
        return;
      }
      if (cancelled.getAsBoolean()) {
        result.completeExceptionally(new CancellationException("gateway request was cancelled"));
        return;
      }
      if (expired) {
        result.completeExceptionally(new PartitionNotReadyException());
        return;
      }
      inFlight = true;
      try {
        if (partitionId == null) {
          request.setClusterId(clusterId);
          partitionId = router.resolve(clusterId, request).partitionId();
        }
        control.runOnCompletion(
            dispatchToPartition(clusterId, request, cancelled, partitionId),
            (response, error) -> {
              inFlight = false;
              if (error == null) {
                result.complete(response);
              } else {
                rejected(error);
              }
            });
      } catch (final RuntimeException error) {
        inFlight = false;
        rejected(error);
      }
    }

    private void rejected(final Throwable error) {
      if (result.isDone()) {
        return;
      }
      if ((error instanceof PartitionNotReadyException
              || error instanceof BrokerRequestRejectedBeforeAppendException)
          && !expired) {
        lastRoutingRejection = error;
        retry = control.schedule(Duration.ofMillis(50), this::attempt);
      } else {
        result.completeExceptionally(error);
      }
    }
  }
}
