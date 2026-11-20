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
package io.taskite.broker.registry;

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.common.PlacementDriverBackoff;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.broker.topology.BrokerTopologyActor;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerRegistrationActor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerRegistrationActor.class);

  private final BrokerConfig config;
  private final ConcurrencyControl concurrencyControl;
  private final Client client;
  private final BrokerPartitionStateCollector partitionStateCollector;
  private final BrokerTopologyActor topologyActor;
  private final Runnable localBrokerRemoved;
  private final PlacementDriverBackoff backoff;
  private ScheduledTimer renewTimer;
  private volatile boolean closed;
  private boolean registering;
  private boolean registered;
  private boolean reportInFlight;
  private boolean reportPending;
  private boolean renewSchedulePending;
  private boolean backpressureWaiting;
  private final String incarnationId;

  public BrokerRegistrationActor(final Configuration configuration, final Context context) {
    incarnationId = Objects.requireNonNull(configuration.incarnationId(), "incarnationId");
    config = Objects.requireNonNull(configuration.broker(), "broker");
    backoff = new PlacementDriverBackoff(config.registration());
    concurrencyControl = Objects.requireNonNull(context.concurrencyControl(), "concurrencyControl");
    client = Objects.requireNonNull(context.client(), "client");
    localBrokerRemoved = Objects.requireNonNull(context.localBrokerRemoved(), "localBrokerRemoved");
    partitionStateCollector = new BrokerPartitionStateCollector(config, context.runtime());
    topologyActor =
        new BrokerTopologyActor(
            config,
            new BrokerTopologyActor.Context(context.runtime(), concurrencyControl, client),
            new BrokerTopologyActor.Callbacks(
                this::onRegistrationLost,
                () -> reportPartitions(false),
                this::stopAfterLocalRemoval));
  }

  public record Configuration(BrokerConfig broker, String incarnationId) {}

  public record Context(
      BrokerRuntime runtime,
      ConcurrencyControl concurrencyControl,
      Client client,
      Runnable localBrokerRemoved) {}

  public void start() {
    if (config.registration().placementDrivers().isBlank()) {
      return;
    }
    // BrokerConfigurationStep completed the initial registration before runtime startup.
    registered = true;
    topologyActor.start();
    topologyActor.onBrokerRegistered();
    reportPartitions(true);
  }

  private void register() {
    if (closed || registering || backpressureWaiting) {
      return;
    }
    if (reportInFlight) {
      renewSchedulePending = true;
      return;
    }
    registering = true;
    try {
      client
          .registerBroker(
              PlacementDriverRequests.registerBroker(
                      config.clusterId(),
                      config.brokerId(),
                      config.network().host(),
                      config.network().commandPort(),
                      config.network().raftPort(),
                      config.version(),
                      config.registration().renewalTimeout().toMillis(),
                      partitionStateCollector.collectPartitionStates())
                  .toBuilder()
                  .setIncarnationId(incarnationId)
                  .build())
          .whenComplete(
              (ignored, error) ->
                  concurrencyControl.run(
                      () -> {
                        registering = false;
                        if (closed) {
                          return;
                        }
                        if (error == null) {
                          registered = true;
                          LOG.info("broker {} registered to placement-driver", config.brokerId());
                          scheduleRenew(backoff.nextDelay(null));
                          topologyActor.onBrokerRegistered();
                        } else {
                          registered = false;
                          LOG.warn(
                              "broker {} failed to register to placement-driver",
                              config.brokerId(),
                              error);
                          scheduleRegisterRetry(error);
                        }
                      }));
    } catch (final RuntimeException error) {
      LOG.warn(
          "broker {} failed to register to placement-driver synchronously",
          config.brokerId(),
          error);
      concurrencyControl.run(
          () -> {
            registering = false;
            scheduleRegisterRetry(error);
          });
    }
  }

  private void renew() {
    reportPartitions(true);
  }

  private void reportPartitions(final boolean scheduleNextRenew) {
    if (backpressureWaiting) {
      reportPending = true;
      renewSchedulePending |= scheduleNextRenew;
      return;
    }
    if (closed || !registered) {
      if (scheduleNextRenew) {
        register();
      }
      return;
    }
    if (reportInFlight) {
      reportPending = true;
      renewSchedulePending |= scheduleNextRenew;
      return;
    }
    reportInFlight = true;
    try {
      client
          .renewBroker(
              PlacementDriverRequests.renewBroker(
                      config.clusterId(),
                      config.brokerId(),
                      partitionStateCollector.collectPartitionStates())
                  .toBuilder()
                  .setIncarnationId(incarnationId)
                  .build())
          .whenComplete(
              (ignored, error) ->
                  concurrencyControl.run(() -> onReportCompleted(scheduleNextRenew, error)));
    } catch (final RuntimeException error) {
      concurrencyControl.run(() -> onReportCompleted(scheduleNextRenew, error));
    }
  }

  private void onReportCompleted(final boolean scheduleNextRenew, final Throwable error) {
    reportInFlight = false;
    if (closed) {
      return;
    }
    if (error != null) {
      LOG.warn(
          "broker {} failed to report partition states to placement-driver",
          config.brokerId(),
          error);
      if (PlacementDriverErrorClassifier.errorMessage(error).contains("STALE_BROKER_INCARNATION")) {
        stopAfterLocalRemoval();
        return;
      }
      if (shouldRegister(error)) {
        reportPending = false;
        renewSchedulePending = false;
        registered = false;
        register();
        return;
      }
    }
    final Duration nextDelay = backoff.nextDelay(error);
    if (PlacementDriverErrorClassifier.isBackpressure(error)) {
      reportPending = false;
      renewSchedulePending = false;
      backpressureWaiting = true;
      scheduleRenew(nextDelay);
      return;
    }
    if (!registered) {
      reportPending = false;
      renewSchedulePending = false;
      register();
      return;
    }

    final boolean sendPendingReport = reportPending;
    final boolean scheduleRenewAfterReport = scheduleNextRenew || renewSchedulePending;
    reportPending = false;
    renewSchedulePending = false;
    if (sendPendingReport) {
      reportPartitions(scheduleRenewAfterReport);
    } else if (scheduleRenewAfterReport) {
      scheduleRenew(nextDelay);
    }
  }

  private void scheduleRenew(final Duration delay) {
    if (closed) {
      return;
    }
    if (renewTimer != null) {
      renewTimer.cancel();
    }
    renewTimer =
        concurrencyControl.schedule(
            delay,
            () -> {
              if (backpressureWaiting) {
                reportPending = false;
                renewSchedulePending = false;
              }
              backpressureWaiting = false;
              renew();
            });
  }

  private void scheduleRegisterRetry(final Throwable error) {
    if (closed) {
      return;
    }
    if (renewTimer != null) {
      renewTimer.cancel();
    }
    backpressureWaiting = PlacementDriverErrorClassifier.isBackpressure(error);
    renewTimer =
        concurrencyControl.schedule(
            backoff.nextDelay(error),
            () -> {
              backpressureWaiting = false;
              register();
            });
  }

  private boolean shouldRegister(final Throwable error) {
    final String message = PlacementDriverErrorClassifier.unwrap(error).getMessage();
    return message != null
        && (message.contains("NO_BROKER_REGISTERED") || message.contains("NO_ONLINE_BROKER"));
  }

  private void onRegistrationLost() {
    reportPending = false;
    renewSchedulePending = false;
    registered = false;
    register();
  }

  private void stopAfterLocalRemoval() {
    if (closed) {
      return;
    }
    closed = true;
    cancelRenewTimer();
    topologyActor.close();
    try {
      localBrokerRemoved.run();
    } catch (final RuntimeException error) {
      LOG.error("failed to stop removed broker {}", config.brokerId(), error);
    }
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    cancelRenewTimer();
    topologyActor.close();
    try {
      client
          .deregisterBroker(
              PlacementDriverRequests.deregisterBroker(config.clusterId(), config.brokerId())
                  .toBuilder()
                  .setIncarnationId(incarnationId)
                  .build())
          .get(10, TimeUnit.SECONDS);
      LOG.info("broker {} deregistered from placement-driver", config.brokerId());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn(
          "broker {} interrupted while deregistering from placement-driver", config.brokerId(), e);
    } catch (Exception e) {
      LOG.warn("broker {} failed to deregister from placement-driver", config.brokerId(), e);
    }
  }

  private void cancelRenewTimer() {
    if (renewTimer != null) {
      renewTimer.cancel();
    }
  }
}
