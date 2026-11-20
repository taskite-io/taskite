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

import io.taskite.topology.ClusterTopology;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/** Concurrent per-cluster read model used by request routing and topology actors. */
public final class GatewayTopologyCache {
  private final Map<String, ClusterTopology> topologies = new ConcurrentHashMap<>();

  public void update(final ClusterTopology newTopology) {
    final ClusterTopology topology = Objects.requireNonNull(newTopology, "newTopology");
    update(topology.clusterId(), topology);
  }

  public void update(final String clusterId, final ClusterTopology newTopology) {
    final String expectedClusterId = requireClusterId(clusterId);
    final ClusterTopology topology = Objects.requireNonNull(newTopology, "newTopology");
    if (!expectedClusterId.equals(topology.clusterId())) {
      throw new IllegalArgumentException(
          "placement-driver returned topology for cluster `"
              + topology.clusterId()
              + "` while `"
              + expectedClusterId
              + "` was requested");
    }
    topologies.put(expectedClusterId, topology);
  }

  public Optional<ClusterTopology> find(final String clusterId) {
    return Optional.ofNullable(topologies.get(requireClusterId(clusterId)));
  }

  public boolean contains(final String clusterId) {
    return topologies.containsKey(requireClusterId(clusterId));
  }

  public Set<String> knownClusterIds() {
    return Collections.unmodifiableSet(new LinkedHashSet<>(new TreeSet<>(topologies.keySet())));
  }

  public ClusterTopology current(final String clusterId) {
    final String requiredClusterId = requireClusterId(clusterId);
    final ClusterTopology current = topologies.get(requiredClusterId);
    if (current == null) {
      throw new IllegalStateException(
          "topology cache is not initialized for cluster `" + requiredClusterId + "`");
    }
    return current;
  }

  private static String requireClusterId(final String clusterId) {
    if (clusterId == null || clusterId.isBlank()) {
      throw new IllegalArgumentException("clusterId must be present");
    }
    return clusterId;
  }
}
