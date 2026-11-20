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
import io.grpc.Contexts;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.taskite.broker.client.response.BrokerRequestRejectedBeforeAppendException;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.client.response.PartitionNotReadyException;
import io.taskite.gateway.api.grpc.GatewayInterceptorUtil;
import io.taskite.gateway.broker.GatewayBrokerCommandDispatcher;
import io.taskite.gateway.common.GatewayClusterResolver;
import io.taskite.gateway.protocol.ActivateJobsRequest;
import io.taskite.gateway.protocol.ActivateJobsResponse;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.Partition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Coordinates partition scans, waiting, cancellation and shutdown for activation requests. */
public final class JobActivationHandler implements AutoCloseable {
  private static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 10_000;
  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
  private final GatewayBrokerCommandDispatcher dispatcher;
  private final GatewayClusterResolver clusters;
  private final AtomicInteger offset = new AtomicInteger();
  private final Set<Activation> pending = ConcurrentHashMap.newKeySet();
  private volatile boolean closed;

  public JobActivationHandler(
      GatewayBrokerCommandDispatcher dispatcher, GatewayClusterResolver clusters) {
    this.dispatcher = Objects.requireNonNull(dispatcher);
    this.clusters = Objects.requireNonNull(clusters);
  }

  public void activate(
      ActivateJobsRequest request,
      StreamObserver<ActivateJobsResponse> observer,
      ConcurrencyControl control) {
    try {
      final var mapped = GatewayRequestMapper.toActivateJobsRequest(request).getRequestWriter();
      final var context = Context.current();
      final String clusterId = GatewayInterceptorUtil.getClusterId(context);
      if (clusterId == null || clusterId.isBlank()) {
        throw new IllegalStateException("clusterId is missing from the gRPC context");
      }
      final String currentTenant = GatewayInterceptorUtil.getTenantId(context);
      if (currentTenant == null || currentTenant.isBlank()) {
        throw new IllegalStateException("tenantId is missing from the gRPC context");
      }
      final var tenants =
          request.getTenantIdsCount() == 0
              ? List.of(currentTenant)
              : request.getTenantIdsList().stream().distinct().toList();
      if (!GatewayInterceptorUtil.getAuthorizedTenantIds(context).containsAll(tenants)) {
        throw Status.PERMISSION_DENIED
            .withDescription("tenantIds contains a tenant not authorized for this request")
            .asRuntimeException();
      }
      final var activation =
          new Activation(
              request,
              observer,
              context,
              clusterId,
              tenants,
              mapped.getMaxJobsToActivate(),
              control);
      synchronized (this) {
        if (closed) {
          throw Status.UNAVAILABLE.withDescription("Gateway is shutting down").asRuntimeException();
        }
        pending.add(activation);
      }
      control.run(activation::start);
    } catch (Exception error) {
      observer.onError(GatewayErrorMapper.toStatusException(error));
    }
  }

  @Override
  public void close() {
    final List<Activation> requests;
    synchronized (this) {
      closed = true;
      requests = List.copyOf(pending);
    }
    requests.forEach(
        request ->
            request.finish(
                Status.UNAVAILABLE
                    .withDescription("Gateway is shutting down")
                    .asRuntimeException()));
  }

  private final class Activation {
    private final ActivateJobsRequest request;
    private final StreamObserver<ActivateJobsResponse> observer;
    private final Context context;
    private final String cluster;
    private final List<String> tenants;
    private final int maxJobs;
    private final ConcurrencyControl control;
    private final long requestTimeout;
    private final Context.CancellationListener cancellation;
    private ScheduledTimer timeoutTimer;
    private ScheduledTimer pollTimer;
    private volatile boolean done;
    private boolean timedOut;
    private boolean dispatchInFlight;
    private int remaining;

    private Activation(
        ActivateJobsRequest request,
        StreamObserver<ActivateJobsResponse> observer,
        Context context,
        String cluster,
        List<String> tenants,
        int maxJobs,
        ConcurrencyControl control) {
      this.request = request;
      this.observer = observer;
      this.context = context;
      this.cluster = cluster;
      this.tenants = tenants;
      this.maxJobs = maxJobs;
      this.control = Objects.requireNonNull(control, "control");
      cancellation = ignored -> control.run(() -> finish(cancelledStatus()));
      remaining = maxJobs;
      requestTimeout =
          request.getRequestTimeout() == 0
              ? DEFAULT_REQUEST_TIMEOUT_MILLIS
              : request.getRequestTimeout();
    }

    private void start() {
      runSafely(
          () -> {
            context.addListener(cancellation, Runnable::run);
            if (requestTimeout > 0) {
              timeoutTimer =
                  control.schedule(
                      Duration.ofMillis(requestTimeout),
                      () -> {
                        timedOut = true;
                        if (!dispatchInFlight) {
                          finish(null);
                        }
                      });
            }
            scan();
          });
    }

    private void scan() {
      runSafely(
          () ->
              control.runOnCompletion(
                  clusters.resolve(cluster),
                  (topology, error) ->
                      runSafely(
                          () -> {
                            if (error != null) {
                              finish(error);
                              return;
                            }
                            final var partitions =
                                new ArrayList<>(
                                    topology.partitions().stream()
                                        .filter(
                                            p ->
                                                p.serviceReady()
                                                    && topology
                                                        .leaderEndpoint(p.partitionId())
                                                        .isPresent())
                                        .map(Partition::partitionId)
                                        .toList());
                            if (partitions.isEmpty()) {
                              visit(partitions, 0, 0);
                              return;
                            }
                            Collections.rotate(
                                partitions,
                                -Math.floorMod(offset.getAndIncrement(), partitions.size()));
                            visit(partitions, 0, 0);
                          })));
    }

    private void visit(List<Integer> partitions, int partitionIndex, int tenantIndex) {
      runSafely(
          () -> {
            if (timedOut) {
              finish(null);
              return;
            }
            if (remaining == 0 || partitionIndex == partitions.size()) {
              if (remaining < maxJobs || requestTimeout < 0) {
                finish(null);
              } else {
                pollTimer = control.schedule(POLL_INTERVAL, this::scan);
              }
              return;
            }
            final var batch =
                GatewayRequestMapper.toActivateJobsRequest(request).setMaxJobsToActivate(remaining);
            batch.setTenantId(tenants.get(tenantIndex));
            final int partitionId = partitions.get(partitionIndex);
            batch.setPartitionId(partitionId);
            dispatchInFlight = true;
            final ActorFuture<BrokerResponse<JobBatchRecord>> dispatched;
            try {
              dispatched =
                  dispatcher.dispatch(
                      cluster, batch, () -> done || context.isCancelled() || closed);
            } catch (final PartitionNotReadyException rejectedBeforeSend) {
              dispatchInFlight = false;
              visit(partitions, partitionIndex + 1, 0);
              return;
            }
            control.runOnCompletion(
                dispatched,
                (response, error) -> {
                  runSafely(
                      () -> {
                        dispatchInFlight = false;
                        if (error != null) {
                          if (error instanceof PartitionNotReadyException
                              || error instanceof BrokerRequestRejectedBeforeAppendException) {
                            visit(partitions, partitionIndex + 1, 0);
                            return;
                          }
                          finish(error);
                          return;
                        }
                        final var result = GatewayResponseMapper.toActivateJobsResponse(response);
                        remaining -= result.getJobsCount();
                        if (result.getJobsCount() > 0) {
                          observer.onNext(result);
                        }
                        if (timedOut) {
                          finish(null);
                          return;
                        }
                        final int nextTenant = tenantIndex + 1;
                        visit(
                            partitions,
                            nextTenant == tenants.size() ? partitionIndex + 1 : partitionIndex,
                            nextTenant == tenants.size() ? 0 : nextTenant);
                      });
                });
          });
    }

    private synchronized void runSafely(Runnable action) {
      if (done) {
        return;
      }
      if (context.isCancelled()) {
        finish(cancelledStatus());
        return;
      }
      if (closed) {
        finish(Status.UNAVAILABLE.withDescription("Gateway is shutting down").asRuntimeException());
        return;
      }
      try {
        action.run();
      } catch (Exception error) {
        finish(error);
      }
    }

    private synchronized void finish(Throwable error) {
      if (done) {
        return;
      }
      done = true;
      if (timeoutTimer != null) {
        timeoutTimer.cancel();
      }
      if (pollTimer != null) {
        pollTimer.cancel();
      }
      context.removeListener(cancellation);
      pending.remove(this);
      if (error == null && context.isCancelled()) {
        error = cancelledStatus();
      }
      if (error != null) {
        observer.onError(GatewayErrorMapper.toStatusException(error));
      } else {
        if (remaining == maxJobs) {
          observer.onNext(ActivateJobsResponse.getDefaultInstance());
        }
        observer.onCompleted();
      }
    }

    private RuntimeException cancelledStatus() {
      final Status status = Contexts.statusFromCancelled(context);
      return (status == null ? Status.CANCELLED : status).asRuntimeException();
    }
  }
}
