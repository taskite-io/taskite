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
package io.taskite.broker.topology;

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.client.TopologyChangeListener;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.topology.ClusterTopology;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Maintains the Broker's committed placement-driver topology view and watch lifecycle. */
public final class BrokerTopologyActor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerTopologyActor.class);

  private final BrokerConfig config;
  private final BrokerRuntime runtime;
  private final ConcurrencyControl concurrencyControl;
  private final Client client;
  private final Runnable registrationLost;
  private final Runnable topologyObserved;
  private final Runnable localBrokerRemoved;
  private ScheduledTimer refreshTimer;
  private ScheduledTimer watchRetryTimer;
  private AutoCloseable topologyWatch;
  private ClusterTopology latestTopology;
  private boolean removalCheckInFlight;
  private volatile boolean closed;

  public BrokerTopologyActor(
      final BrokerConfig config, final Context context, final Callbacks callbacks) {
    this.config = Objects.requireNonNull(config, "config");
    runtime = Objects.requireNonNull(context.runtime(), "runtime");
    concurrencyControl = Objects.requireNonNull(context.concurrencyControl(), "concurrencyControl");
    client = Objects.requireNonNull(context.client(), "client");
    registrationLost = Objects.requireNonNull(callbacks.registrationLost(), "registrationLost");
    topologyObserved = Objects.requireNonNull(callbacks.topologyObserved(), "topologyObserved");
    localBrokerRemoved =
        Objects.requireNonNull(callbacks.localBrokerRemoved(), "localBrokerRemoved");
  }

  public record Context(
      BrokerRuntime runtime, ConcurrencyControl concurrencyControl, Client client) {}

  public record Callbacks(
      Runnable registrationLost, Runnable topologyObserved, Runnable localBrokerRemoved) {}

  public void start() {
    scheduleTopologyRefresh();
  }

  public void onBrokerRegistered() {
    startTopologyWatch();
    refreshTopology();
  }

  private void refreshTopology() {
    if (closed) {
      return;
    }
    try {
      client
          .topology(config.clusterId())
          .whenComplete(
              (topology, error) ->
                  concurrencyControl.run(
                      () -> {
                        if (closed) {
                          return;
                        }
                        if (error != null) {
                          LOG.warn(
                              "broker {} failed to refresh topology from placement-driver",
                              config.brokerId(),
                              error);
                          if (requiresRegistration(error)) {
                            registrationLost.run();
                            return;
                          }
                          scheduleTopologyRefresh();
                          return;
                        }
                        observeTopology(topology);
                        scheduleTopologyRefresh();
                      }));
    } catch (final RuntimeException error) {
      LOG.warn(
          "broker {} failed to refresh topology from placement-driver synchronously",
          config.brokerId(),
          error);
      concurrencyControl.run(this::scheduleTopologyRefresh);
    }
  }

  private void startTopologyWatch() {
    if (topologyWatch != null || closed) {
      return;
    }
    topologyWatch =
        client.watchBrokerTopology(
            config.clusterId(),
            new TopologyChangeListener() {
              @Override
              public void onTopologyChanged(final ClusterTopology topology) {
                concurrencyControl.run(
                    () -> {
                      if (!closed) {
                        observeTopology(topology);
                      }
                    });
              }

              @Override
              public void onTopologyWatchFailed(final Throwable error) {
                if (!closed) {
                  LOG.warn("broker {} topology watch disconnected", config.brokerId(), error);
                  concurrencyControl.run(
                      () -> {
                        closeTopologyWatch();
                        scheduleTopologyWatchRetry();
                      });
                }
              }
            });
  }

  private void observeTopology(final ClusterTopology topology) {
    latestTopology = topology;
    runtime.updateTopology(topology);
    if (isLocalRemovalCandidate(topology)) {
      confirmLocalRemoval();
    } else {
      topologyObserved.run();
    }
  }

  private boolean isLocalRemovalCandidate(final ClusterTopology topology) {
    if (topology.brokers().containsKey(config.brokerId())) {
      return false;
    }
    return topology.partitions().stream()
        .noneMatch(
            partition ->
                partition.voters().contains(config.brokerId())
                    || partition.learners().contains(config.brokerId()));
  }

  private void confirmLocalRemoval() {
    if (closed || removalCheckInFlight) {
      return;
    }
    removalCheckInFlight = true;
    try {
      client
          .clusterConfig(config.clusterId())
          .whenComplete(
              (clusterConfig, error) ->
                  concurrencyControl.run(
                      () -> {
                        removalCheckInFlight = false;
                        if (closed) {
                          return;
                        }
                        if (error != null) {
                          LOG.debug(
                              "broker {} failed to confirm removal from placement-driver",
                              config.brokerId(),
                              error);
                          return;
                        }
                        if (clusterConfig.removedBrokers().contains(config.brokerId())
                            && latestTopology != null
                            && isLocalRemovalCandidate(latestTopology)) {
                          stopAfterLocalRemoval();
                        }
                      }));
    } catch (final RuntimeException error) {
      removalCheckInFlight = false;
      LOG.debug(
          "broker {} failed to confirm removal from placement-driver synchronously",
          config.brokerId(),
          error);
    }
  }

  private void stopAfterLocalRemoval() {
    close();
    LOG.info("stop removed broker {} after committed membership removal", config.brokerId());
    try {
      localBrokerRemoved.run();
    } catch (final RuntimeException error) {
      LOG.error("failed to stop removed broker {}", config.brokerId(), error);
    }
  }

  private void scheduleTopologyRefresh() {
    if (closed) {
      return;
    }
    if (refreshTimer != null) {
      refreshTimer.cancel();
    }
    refreshTimer =
        concurrencyControl.schedule(
            config.registration().topologyRefreshInterval(), this::refreshTopology);
  }

  private void scheduleTopologyWatchRetry() {
    if (closed) {
      return;
    }
    if (watchRetryTimer != null) {
      watchRetryTimer.cancel();
    }
    watchRetryTimer =
        concurrencyControl.schedule(
            config.registration().topologyRefreshInterval(), this::startTopologyWatch);
  }

  private static boolean requiresRegistration(final Throwable error) {
    final String message = PlacementDriverErrorClassifier.unwrap(error).getMessage();
    return message != null
        && (message.contains("NO_BROKER_REGISTERED") || message.contains("NO_ONLINE_BROKER"));
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (refreshTimer != null) {
      refreshTimer.cancel();
    }
    if (watchRetryTimer != null) {
      watchRetryTimer.cancel();
    }
    closeTopologyWatch();
  }

  private void closeTopologyWatch() {
    if (topologyWatch == null) {
      return;
    }
    try {
      topologyWatch.close();
    } catch (final Exception error) {
      LOG.warn("broker {} failed to close topology watch", config.brokerId(), error);
    } finally {
      topologyWatch = null;
    }
  }
}
