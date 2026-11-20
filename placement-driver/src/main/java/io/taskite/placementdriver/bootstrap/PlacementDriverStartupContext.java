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

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.api.grpc.PlacementDriverGrpcServer;
import io.taskite.placementdriver.processing.PlacementDriverProcessorDependencies;
import io.taskite.placementdriver.runtime.PlacementDriverRaftRuntime;
import io.taskite.placementdriver.runtime.PlacementDriverRuntimeComponents;
import io.taskite.placementdriver.runtime.PlacementDriverTopologyManifest;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class PlacementDriverStartupContext implements AutoCloseable {
  private final ActorScheduler scheduler;
  private final ConcurrencyControl concurrencyControl;
  private final PlacementDriverConfiguration config;
  private final int httpPort;
  private final PlacementDriverEndpointManager endpointManager;
  private final Runnable localMemberRemoved;
  private final MeterRegistry meterRegistry;
  private PlacementDriverProcessorDependencies processorDependencies;
  private TypedEventRegistry eventRegistry;
  private PlacementDriverRaftRuntime raftRuntime;
  private PlacementDriverRuntimeComponents runtimeComponents;
  private PlacementDriverGrpcServer grpcServer;
  private PlacementDriverTopologyManifest topologyManifest;
  private List<RaftPeer> bootstrapPeers = List.of();
  private String bootstrapLeaderEndpoint = "";
  private boolean schedulerStarted;
  private boolean closed;

  public PlacementDriverStartupContext(final PlacementDriverConfiguration config) {
    this(config, 0, new PlacementDriverEndpointManager(), () -> {});
  }

  public PlacementDriverStartupContext(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager) {
    this(config, httpPort, endpointManager, () -> {});
  }

  public PlacementDriverStartupContext(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager,
      final Runnable localMemberRemoved) {
    this(config, httpPort, endpointManager, localMemberRemoved, null);
  }

  public PlacementDriverStartupContext(
      final PlacementDriverConfiguration config,
      final int httpPort,
      final PlacementDriverEndpointManager endpointManager,
      final Runnable localMemberRemoved,
      final MeterRegistry meterRegistry) {
    this.config = Objects.requireNonNull(config, "config");
    this.httpPort = httpPort;
    this.endpointManager = Objects.requireNonNull(endpointManager, "endpointManager");
    this.localMemberRemoved = Objects.requireNonNull(localMemberRemoved, "localMemberRemoved");
    this.meterRegistry = meterRegistry;
    scheduler = ActorScheduler.newActorScheduler().build();
    concurrencyControl =
        Actor.newActor().name("placement-driver-startup-" + config.nodeId()).build();
  }

  public PlacementDriverConfiguration config() {
    return config;
  }

  int httpPort() {
    return httpPort;
  }

  public ActorScheduler scheduler() {
    return scheduler;
  }

  public ConcurrencyControl concurrencyControl() {
    return concurrencyControl;
  }

  public PlacementDriverEndpointManager endpointManager() {
    return endpointManager;
  }

  Runnable localMemberRemoved() {
    return localMemberRemoved;
  }

  MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  public PlacementDriverProcessorDependencies processorDependencies() {
    return Objects.requireNonNull(processorDependencies, "processorDependencies");
  }

  void processorDependencies(final PlacementDriverProcessorDependencies processorDependencies) {
    this.processorDependencies =
        Objects.requireNonNull(processorDependencies, "processorDependencies");
  }

  public TypedEventRegistry eventRegistry() {
    return Objects.requireNonNull(eventRegistry, "eventRegistry");
  }

  void eventRegistry(final TypedEventRegistry eventRegistry) {
    this.eventRegistry = Objects.requireNonNull(eventRegistry, "eventRegistry");
  }

  public Optional<PlacementDriverTopologyManifest> topologyManifest() {
    return Optional.ofNullable(topologyManifest);
  }

  void topologyManifest(final PlacementDriverTopologyManifest topologyManifest) {
    this.topologyManifest = topologyManifest;
  }

  public List<RaftPeer> bootstrapPeers() {
    return bootstrapPeers;
  }

  void bootstrapPeers(final List<RaftPeer> bootstrapPeers) {
    this.bootstrapPeers = List.copyOf(bootstrapPeers);
  }

  public Optional<String> bootstrapLeaderEndpoint() {
    return bootstrapLeaderEndpoint.isBlank()
        ? Optional.empty()
        : Optional.of(bootstrapLeaderEndpoint);
  }

  void bootstrapLeaderEndpoint(final String bootstrapLeaderEndpoint) {
    this.bootstrapLeaderEndpoint = bootstrapLeaderEndpoint == null ? "" : bootstrapLeaderEndpoint;
  }

  public boolean schedulerStarted() {
    return schedulerStarted;
  }

  public void markSchedulerStarted() {
    schedulerStarted = true;
  }

  public void markSchedulerStopped() {
    schedulerStarted = false;
  }

  public PlacementDriverRaftRuntime raftRuntime() {
    return Objects.requireNonNull(raftRuntime, "raftRuntime");
  }

  synchronized void raftRuntime(final PlacementDriverRaftRuntime raftRuntime) {
    if (closed) {
      raftRuntime.close();
      throw new IllegalStateException("placement-driver startup context is closed");
    }
    this.raftRuntime = Objects.requireNonNull(raftRuntime, "raftRuntime");
  }

  public PlacementDriverRuntimeComponents runtimeComponents() {
    return Objects.requireNonNull(runtimeComponents, "runtimeComponents");
  }

  synchronized void runtimeComponents(final PlacementDriverRuntimeComponents runtimeComponents) {
    if (closed) {
      runtimeComponents.close();
      throw new IllegalStateException("placement-driver startup context is closed");
    }
    this.runtimeComponents = Objects.requireNonNull(runtimeComponents, "runtimeComponents");
  }

  public PlacementDriverGrpcServer grpcServer() {
    return grpcServer;
  }

  synchronized void grpcServer(final PlacementDriverGrpcServer grpcServer) {
    if (closed) {
      grpcServer.close();
      throw new IllegalStateException("placement-driver startup context is closed");
    }
    this.grpcServer = Objects.requireNonNull(grpcServer, "grpcServer");
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    endpointManager.unbindRuntime();
    RuntimeException failure = null;
    for (final AutoCloseable component :
        new AutoCloseable[] {grpcServer, runtimeComponents, raftRuntime}) {
      if (component == null) {
        continue;
      }
      try {
        component.close();
      } catch (final Exception error) {
        if (failure == null) {
          failure = new IllegalStateException("failed to close placement-driver runtime", error);
        } else {
          failure.addSuppressed(error);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
