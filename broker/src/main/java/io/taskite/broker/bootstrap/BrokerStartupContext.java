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
package io.taskite.broker.bootstrap;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.taskite.broker.BrokerClusterConfig;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.api.grpc.GrpcBrokerServer;
import io.taskite.broker.common.PlacementDriverBackoff;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
import io.taskite.broker.registry.BrokerRegistrationActor;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.protocol.RenewBrokerRequest;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.LoggerFactory;

public final class BrokerStartupContext implements AutoCloseable {
  private final ActorScheduler scheduler;
  private final ConcurrencyControl concurrencyControl;
  private final BrokerConfig config;
  private final MeterRegistry meterRegistry;
  private final Runnable localBrokerRemoved;
  private final Map<Class<?>, Object> resources = new LinkedHashMap<>();
  private final Deque<RegisteredResource> closeOrder = new ArrayDeque<>();
  private volatile boolean schedulerStarted;
  private volatile boolean closed;
  private final String incarnationId = UUID.randomUUID().toString();
  private ScheduledTimer initializationRenewal;
  private volatile boolean runtimeReporting;
  private volatile Throwable initializationLeaseFailure;
  private volatile Consumer<Throwable> initializationFailure = ignored -> {};
  private Actor initializationLeaseActor;
  private final PlacementDriverBackoff initializationBackoff;
  private boolean initializationRenewalInFlight;
  private final CompletableActorFuture<Void> initializationFinished =
      new CompletableActorFuture<>();

  public void onInitializationFailure(final Consumer<Throwable> listener) {
    initializationFailure = Objects.requireNonNull(listener, "listener");
    if (initializationLeaseFailure != null) {
      listener.accept(initializationLeaseFailure);
    }
  }

  private void revokeInitializationLease(final Throwable error) {
    initializationLeaseFailure = error;
    // Complete the overall startup result before shutdown cancels the stage retry timers.
    initializationFailure.accept(error);
    localBrokerRemoved.run();
  }

  void requireInitializationLease() {
    if (initializationLeaseFailure != null) {
      throw new IllegalStateException(
          "Broker initialization registration was revoked", initializationLeaseFailure);
    }
  }

  public String incarnationId() {
    return incarnationId;
  }

  void maintainInitializationLease() {
    if (initializationLeaseActor == null) {
      initializationLeaseActor =
          Actor.newActor().name("broker-initialization-lease-" + config.brokerId()).build();
      scheduler
          .submitActor(initializationLeaseActor)
          .onComplete(
              (ignored, error) -> {
                if (error != null) {
                  revokeInitializationLease(error);
                } else {
                  initializationLeaseActor.run(this::scheduleInitializationRenewal);
                }
              });
    }
  }

  private void scheduleInitializationRenewal() {
    scheduleInitializationRenewal(config.registration().renewInterval());
  }

  private void scheduleInitializationRenewal(final Duration interval) {
    if (closed || runtimeReporting || initializationRenewal != null) {
      return;
    }
    initializationRenewal =
        initializationLeaseActor.schedule(
            interval,
            () -> {
              initializationRenewal = null;
              if (closed || runtimeReporting) {
                return;
              }
              initializationRenewalInFlight = true;
              try {
                placementDriverClient()
                    .renewBroker(
                        RenewBrokerRequest.newBuilder()
                            .setClusterId(config.clusterId())
                            .setBrokerId(config.brokerId())
                            .setIncarnationId(incarnationId)
                            .setPreservePartitionObservations(true)
                            .build())
                    .whenComplete(
                        (ignored, error) ->
                            initializationLeaseActor.run(() -> onInitializationRenewed(error)));
              } catch (final RuntimeException error) {
                onInitializationRenewed(error);
              }
            });
  }

  private void onInitializationRenewed(final Throwable error) {
    initializationRenewalInFlight = false;
    if (error != null) {
      LoggerFactory.getLogger(BrokerStartupContext.class)
          .warn("Broker {} initialization lease renewal failed", config.brokerId(), error);
      final String message = PlacementDriverErrorClassifier.errorMessage(error);
      if (message.contains("STALE_BROKER_INCARNATION")
          || message.contains("NO_BROKER_REGISTERED")
          || message.contains("NO_ONLINE_BROKER")
          || message.contains("CLUSTER_NOT_REGISTERED")) {
        revokeInitializationLease(error);
        if (!initializationFinished.isDone()) {
          initializationFinished.completeExceptionally(error);
        }
        return;
      }
    }
    if (runtimeReporting) {
      completeInitialization();
    } else {
      scheduleInitializationRenewal(initializationBackoff.nextDelay(error));
    }
  }

  ActorFuture<Void> finishInitializationLease() {
    runtimeReporting = true;
    if (initializationLeaseActor != null) {
      initializationLeaseActor.run(
          () -> {
            if (initializationRenewal != null) {
              initializationRenewal.cancel();
            }
            if (!initializationRenewalInFlight) {
              completeInitialization();
            }
          });
    } else {
      initializationFinished.complete(null);
    }
    return initializationFinished;
  }

  private void completeInitialization() {
    if (initializationFinished.isDone()) {
      return;
    }
    initializationLeaseActor.closeAsync();
    initializationFinished.complete(null);
  }

  public BrokerStartupContext(final BrokerConfig config) {
    this(config, Metrics.globalRegistry, () -> {});
  }

  public BrokerStartupContext(final BrokerConfig config, final Runnable localBrokerRemoved) {
    this(config, Metrics.globalRegistry, localBrokerRemoved);
  }

  public BrokerStartupContext(
      final BrokerConfig config,
      final MeterRegistry meterRegistry,
      final Runnable localBrokerRemoved) {
    this.config = Objects.requireNonNull(config, "config");
    initializationBackoff = new PlacementDriverBackoff(config.registration());
    this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.localBrokerRemoved = Objects.requireNonNull(localBrokerRemoved, "localBrokerRemoved");
    scheduler = ActorScheduler.newActorScheduler().build();
    concurrencyControl = Actor.newActor().name("broker-startup-" + config.brokerId()).build();
  }

  public ActorScheduler scheduler() {
    return scheduler;
  }

  public ConcurrencyControl concurrencyControl() {
    return concurrencyControl;
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

  public BrokerConfig config() {
    return config;
  }

  public MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  Runnable localBrokerRemoved() {
    return localBrokerRemoved;
  }

  public BrokerClusterConfig clusterConfig() {
    return requireResource(BrokerClusterConfig.class);
  }

  void clusterConfig(final BrokerClusterConfig clusterConfig) {
    registerValue(BrokerClusterConfig.class, clusterConfig);
  }

  public BrokerRuntime runtime() {
    return requireResource(BrokerRuntime.class);
  }

  public Optional<BrokerRuntime> initializedRuntime() {
    return findResource(BrokerRuntime.class);
  }

  void runtime(final BrokerRuntime runtime) {
    registerResource(BrokerRuntime.class, runtime, runtime);
  }

  public GrpcBrokerServer server() {
    return requireResource(GrpcBrokerServer.class);
  }

  void server(final GrpcBrokerServer server) {
    registerResource(GrpcBrokerServer.class, server, server);
  }

  void registration(final BrokerRegistrationActor registration) {
    registerResource(BrokerRegistrationActor.class, registration, registration);
  }

  public Client placementDriverClient() {
    return findResource(Client.class).orElse(null);
  }

  void placementDriverClient(final Client placementDriverClient) {
    registerResource(Client.class, placementDriverClient, placementDriverClient);
  }

  ClusterTopology bootstrapTopology() {
    return findResource(ClusterTopology.class)
        .orElseGet(() -> BrokerBootstrapTopology.bootstrapTopology(config, clusterConfig()));
  }

  void bootstrapTopology(final ClusterTopology bootstrapTopology) {
    registerValue(ClusterTopology.class, bootstrapTopology);
  }

  public <T> void registerResource(
      final Class<T> type, final T resource, final AutoCloseable closeAction) {
    register(type, resource);
    closeOrder.push(
        new RegisteredResource(type, Objects.requireNonNull(closeAction, "closeAction")));
  }

  public <T> T requireResource(final Class<T> type) {
    return findResource(type)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Broker startup resource is not initialized: " + type.getSimpleName()));
  }

  private <T> void registerValue(final Class<T> type, final T value) {
    register(type, value);
  }

  private <T> void register(final Class<T> type, final T value) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(value, "value");
    if (closed) {
      throw new IllegalStateException("Broker startup context is already closed");
    }
    if (resources.putIfAbsent(type, value) != null) {
      throw new IllegalStateException(
          "Broker startup resource is already initialized: " + type.getSimpleName());
    }
  }

  private <T> Optional<T> findResource(final Class<T> type) {
    return Optional.ofNullable(resources.get(type)).map(type::cast);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (initializationRenewal != null) {
      initializationRenewal.cancel();
    }
    if (initializationLeaseActor != null) {
      initializationLeaseActor.closeAsync();
    }
    RuntimeException failure = null;
    while (!closeOrder.isEmpty()) {
      final RegisteredResource registered = closeOrder.pop();
      try {
        registered.closeAction().close();
      } catch (final Exception error) {
        if (failure == null) {
          failure =
              new IllegalStateException(
                  "Failed to close Broker resource " + registered.type().getSimpleName(), error);
        } else {
          failure.addSuppressed(error);
        }
      }
    }
    resources.clear();
    if (failure != null) {
      throw failure;
    }
  }

  private record RegisteredResource(Class<?> type, AutoCloseable closeAction) {}
}
