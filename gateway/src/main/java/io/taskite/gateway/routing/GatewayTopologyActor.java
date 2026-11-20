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

import io.taskite.placementdriver.client.Client;
import io.taskite.placementdriver.client.TopologyChangeListener;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Maintains the Gateway topology read model through watch plus periodic pull. */
public final class GatewayTopologyActor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(GatewayTopologyActor.class);

  private final String clusterId;
  private final ConcurrencyControl concurrencyControl;
  private final GatewayTopologyCache topologyCache;
  private final Client placementDriver;
  private final Duration refreshInterval;
  private final AtomicBoolean closed = new AtomicBoolean();
  private ScheduledTimer refreshTimer;
  private ScheduledTimer topologyWatchRetryTimer;
  private AutoCloseable topologyWatch;

  public GatewayTopologyActor(
      final String clusterId,
      final ConcurrencyControl concurrencyControl,
      final GatewayTopologyCache topologyCache,
      final Client placementDriver,
      final Duration refreshInterval) {
    this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
    this.topologyCache = Objects.requireNonNull(topologyCache, "topologyCache");
    this.placementDriver = Objects.requireNonNull(placementDriver, "placementDriver");
    this.refreshInterval = Objects.requireNonNull(refreshInterval, "refreshInterval");
  }

  public void start() {
    startTopologyWatch();
    scheduleNextRefresh();
  }

  public void refreshOnce() {
    if (closed.get()) {
      return;
    }
    try {
      placementDriver
          .topology(clusterId)
          .whenComplete(
              (topology, error) -> {
                if (error != null) {
                  LOG.warn("gateway failed to refresh topology for cluster {}", clusterId, error);
                } else {
                  updateTopology(topology);
                }
                scheduleNextRefresh();
              });
    } catch (final RuntimeException error) {
      LOG.warn("gateway failed to refresh topology for cluster {}", clusterId, error);
      scheduleNextRefresh();
    }
  }

  private void scheduleNextRefresh() {
    if (closed.get()) {
      return;
    }
    if (refreshTimer != null) {
      refreshTimer.cancel();
    }
    refreshTimer = concurrencyControl.schedule(refreshInterval, this::refreshOnce);
  }

  private void startTopologyWatch() {
    if (closed.get() || topologyWatch != null) {
      return;
    }
    try {
      topologyWatch =
          placementDriver.watchBrokerTopology(
              clusterId,
              new TopologyChangeListener() {
                @Override
                public void onTopologyChanged(final ClusterTopology topology) {
                  if (!closed.get()) {
                    updateTopology(topology);
                  }
                }

                @Override
                public void onTopologyWatchFailed(final Throwable error) {
                  if (!closed.get()) {
                    LOG.warn(
                        "gateway topology watch disconnected for cluster {}", clusterId, error);
                    concurrencyControl.run(
                        () -> {
                          closeTopologyWatch();
                          scheduleTopologyWatchRetry();
                        });
                  }
                }
              });
    } catch (final RuntimeException error) {
      LOG.warn("gateway failed to start topology watch for cluster {}", clusterId, error);
      scheduleTopologyWatchRetry();
    }
  }

  private void updateTopology(final ClusterTopology topology) {
    try {
      topologyCache.update(clusterId, topology);
    } catch (final RuntimeException error) {
      LOG.warn("gateway rejected mismatched topology for cluster {}", clusterId, error);
    }
  }

  private void scheduleTopologyWatchRetry() {
    if (closed.get()) {
      return;
    }
    if (topologyWatchRetryTimer != null) {
      topologyWatchRetryTimer.cancel();
    }
    topologyWatchRetryTimer =
        concurrencyControl.schedule(refreshInterval, this::startTopologyWatch);
  }

  private void closeTopologyWatch() {
    if (topologyWatch == null) {
      return;
    }
    try {
      topologyWatch.close();
    } catch (final Exception error) {
      LOG.warn("gateway failed to close topology watch for cluster {}", clusterId, error);
    } finally {
      topologyWatch = null;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (refreshTimer != null) {
      refreshTimer.cancel();
    }
    if (topologyWatchRetryTimer != null) {
      topologyWatchRetryTimer.cancel();
    }
    closeTopologyWatch();
  }
}
