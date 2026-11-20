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
package io.taskite.placementdriver.runtime;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import java.util.concurrent.CopyOnWriteArrayList;

public final class PlacementDriverClusterTopologyWatchPublisher implements AutoCloseable {
  private final CopyOnWriteArrayList<PlacementDriverClusterTopologyWatchListener> listeners =
      new CopyOnWriteArrayList<>();

  public AutoCloseable subscribeToPlacementDriverClusterTopology(
      final PlacementDriverClusterTopologyWatchListener listener) {
    listeners.add(listener);
    return () -> listeners.remove(listener);
  }

  public void publishPlacementDriverClusterTopology(final MemberTopologyRecord topology) {
    listeners.forEach(listener -> listener.onPlacementDriverClusterTopology(topology));
  }

  @Override
  public void close() {
    listeners.clear();
  }

  @FunctionalInterface
  public interface PlacementDriverClusterTopologyWatchListener {
    void onPlacementDriverClusterTopology(MemberTopologyRecord topology);
  }
}
