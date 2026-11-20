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
package io.taskite.placementdriver.broker.topology;

import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class BrokerTopologyWatchPublisher implements AutoCloseable {
  private final Map<String, CopyOnWriteArrayList<BrokerTopologyWatchListener>> listeners =
      new ConcurrentHashMap<>();
  private final Map<String, String> publishedSignatures = new ConcurrentHashMap<>();

  public AutoCloseable subscribeToBrokerTopology(
      final String clusterId, final BrokerTopologyWatchListener listener) {
    listeners.computeIfAbsent(clusterId, ignored -> new CopyOnWriteArrayList<>()).add(listener);
    return () -> removeListener(clusterId, listener);
  }

  public void publishBrokerTopology(final ClusterTopology topology) {
    final CopyOnWriteArrayList<BrokerTopologyWatchListener> subscribers =
        listeners.get(topology.clusterId());
    if (subscribers == null || subscribers.isEmpty()) {
      return;
    }
    final String signature = signature(topology);
    if (signature.equals(publishedSignatures.put(topology.clusterId(), signature))) {
      return;
    }
    for (final BrokerTopologyWatchListener listener : subscribers) {
      listener.onBrokerTopology(topology);
    }
  }

  public void closeBrokerTopology(final String clusterId) {
    publishedSignatures.remove(clusterId);
    final CopyOnWriteArrayList<BrokerTopologyWatchListener> subscribers =
        listeners.remove(clusterId);
    if (subscribers != null) {
      subscribers.forEach(listener -> listener.onBrokerTopologyUnavailable(clusterId));
    }
  }

  @Override
  public void close() {
    listeners.keySet().forEach(this::closeBrokerTopology);
    publishedSignatures.clear();
  }

  private void removeListener(final String clusterId, final BrokerTopologyWatchListener listener) {
    final CopyOnWriteArrayList<BrokerTopologyWatchListener> subscribers = listeners.get(clusterId);
    if (subscribers != null) {
      subscribers.remove(listener);
      if (subscribers.isEmpty()) {
        listeners.remove(clusterId, subscribers);
        publishedSignatures.remove(clusterId);
      }
    }
  }

  private static String signature(final ClusterTopology topology) {
    final StringBuilder signature =
        new StringBuilder()
            .append("partitionSize:")
            .append(topology.partitionSize())
            .append(";replicationFactor:")
            .append(topology.replicationFactor())
            .append(';');
    topology.brokers().entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry ->
                signature
                    .append("broker:")
                    .append(entry.getKey())
                    .append('@')
                    .append(entry.getValue().host())
                    .append(':')
                    .append(entry.getValue().port())
                    .append(':')
                    .append(entry.getValue().raftPort())
                    .append(';'));
    topology.partitions().stream()
        .sorted(Comparator.comparingInt(Partition::partitionId))
        .forEach(
            partition ->
                signature
                    .append("partition:")
                    .append(partition.partitionId())
                    .append(':')
                    .append(partition.leaderId())
                    .append(':')
                    .append(partition.status())
                    .append(':')
                    .append(partition.term())
                    .append(':')
                    .append(partition.recoveryEpoch())
                    .append(':')
                    .append(partition.replicas().stream().sorted().toList())
                    .append(':')
                    .append(partition.learners().stream().sorted().toList())
                    .append(':')
                    .append(
                        partition.priorities().entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .toList())
                    .append(';'));
    return signature.toString();
  }
}
