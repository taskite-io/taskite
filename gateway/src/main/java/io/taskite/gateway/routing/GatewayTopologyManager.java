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
package io.taskite.gateway.routing;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.taskite.gateway.common.ClusterTopologyUnavailableException;
import io.taskite.gateway.common.GatewayClusterResolver;
import io.taskite.gateway.common.UnknownClusterException;
import io.taskite.placementdriver.client.Client;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Lazily owns one topology watch and refresh actor for every requested Broker cluster. */
public final class GatewayTopologyManager implements GatewayClusterResolver, AutoCloseable {
  private final ConcurrencyControl concurrencyControl;
  private final GatewayTopologyCache topologyCache;
  private final Client placementDriver;
  private final Duration refreshInterval;
  private final ConcurrentMap<String, GatewayTopologyActor> actors = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ActorFuture<ClusterTopology>> inFlightLookups =
      new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  public GatewayTopologyManager(
      final ConcurrencyControl concurrencyControl,
      final GatewayTopologyCache topologyCache,
      final Client placementDriver,
      final Duration refreshInterval) {
    this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
    this.topologyCache = Objects.requireNonNull(topologyCache, "topologyCache");
    this.placementDriver = Objects.requireNonNull(placementDriver, "placementDriver");
    this.refreshInterval = Objects.requireNonNull(refreshInterval, "refreshInterval");
  }

  @Override
  public ActorFuture<ClusterTopology> resolve(final String clusterId) {
    final String requiredClusterId = requireClusterId(clusterId);
    if (closed.get()) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("gateway topology manager is closed"));
    }
    final var cached = topologyCache.find(requiredClusterId);
    if (cached.isPresent()) {
      return CompletableActorFuture.completed(cached.get());
    }
    final ActorFuture<ClusterTopology> inFlight = inFlightLookups.get(requiredClusterId);
    if (inFlight != null) {
      return inFlight;
    }

    final CompletableActorFuture<ClusterTopology> lookup = new CompletableActorFuture<>();
    final ActorFuture<ClusterTopology> raced =
        inFlightLookups.putIfAbsent(requiredClusterId, lookup);
    if (raced != null) {
      return raced;
    }
    startLookup(requiredClusterId, lookup);
    return lookup;
  }

  public ActorFuture<ClusterTopology> topology(final String clusterId) {
    return resolve(clusterId);
  }

  private void startLookup(
      final String clusterId, final CompletableActorFuture<ClusterTopology> result) {
    final ActorFuture<ClusterTopology> placementDriverLookup;
    try {
      placementDriverLookup = placementDriver.topology(clusterId);
    } catch (final Throwable error) {
      completeLookup(clusterId, result, null, error);
      return;
    }
    placementDriverLookup.onComplete(
        (topology, error) -> completeLookup(clusterId, result, topology, error), Runnable::run);
  }

  private void completeLookup(
      final String clusterId,
      final CompletableActorFuture<ClusterTopology> result,
      final ClusterTopology topology,
      final Throwable error) {
    if (error != null) {
      final Throwable unwrapped = unwrap(error);
      if (isUnknownCluster(unwrapped)) {
        failLookup(
            clusterId,
            result,
            new UnknownClusterException(clusterId, topologyCache.knownClusterIds(), unwrapped));
      } else {
        failLookup(
            clusterId, result, new ClusterTopologyUnavailableException(clusterId, unwrapped));
      }
      return;
    }
    try {
      topologyCache.update(clusterId, topology);
      actors.computeIfAbsent(clusterId, this::startActor);
      completeLookup(clusterId, result, topology);
    } catch (final Throwable updateError) {
      failLookup(
          clusterId, result, new ClusterTopologyUnavailableException(clusterId, updateError));
    }
  }

  private void completeLookup(
      final String clusterId,
      final CompletableActorFuture<ClusterTopology> result,
      final ClusterTopology topology) {
    inFlightLookups.remove(clusterId, result);
    synchronized (result) {
      if (!result.isDone()) {
        if (closed.get()) {
          result.completeExceptionally(
              new IllegalStateException("gateway topology manager is closed"));
        } else {
          result.complete(topology);
        }
      }
    }
  }

  private void failLookup(
      final String clusterId,
      final CompletableActorFuture<ClusterTopology> result,
      final RuntimeException error) {
    inFlightLookups.remove(clusterId, result);
    synchronized (result) {
      if (!result.isDone()) {
        result.completeExceptionally(
            closed.get() ? new IllegalStateException("gateway topology manager is closed") : error);
      }
    }
  }

  private static boolean isUnknownCluster(final Throwable error) {
    if (error instanceof StatusException status
        && status.getStatus().getCode() == Status.Code.NOT_FOUND) {
      return true;
    }
    return error instanceof StatusRuntimeException status
        && status.getStatus().getCode() == Status.Code.NOT_FOUND;
  }

  private static Throwable unwrap(final Throwable error) {
    Throwable current = error;
    while ((current instanceof CompletionException || current instanceof ExecutionException)
        && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  private GatewayTopologyActor startActor(final String clusterId) {
    if (closed.get()) {
      throw new IllegalStateException("gateway topology manager is closed");
    }
    final GatewayTopologyActor actor =
        new GatewayTopologyActor(
            clusterId, concurrencyControl, topologyCache, placementDriver, refreshInterval);
    actor.start();
    return actor;
  }

  private static String requireClusterId(final String clusterId) {
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId must be present");
    }
    return clusterId;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    RuntimeException failure = null;
    for (final GatewayTopologyActor actor : actors.values()) {
      try {
        actor.close();
      } catch (final RuntimeException error) {
        if (failure == null) {
          failure = error;
        } else {
          failure.addSuppressed(error);
        }
      }
    }
    actors.clear();
    for (final ActorFuture<ClusterTopology> lookup : inFlightLookups.values()) {
      synchronized (lookup) {
        if (!lookup.isDone()) {
          lookup.completeExceptionally(
              new IllegalStateException("gateway topology manager is closed"));
        }
      }
    }
    inFlightLookups.clear();
    if (failure != null) {
      throw failure;
    }
  }
}
