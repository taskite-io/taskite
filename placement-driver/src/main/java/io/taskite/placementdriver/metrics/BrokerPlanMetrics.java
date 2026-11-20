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
package io.taskite.placementdriver.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.Partition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Publishes actor-sampled durable execution state without change IDs as metric labels. */
final class BrokerPlanMetrics implements AutoCloseable {
  private final Map<String, MultiGauge> gauges = new LinkedHashMap<>();

  BrokerPlanMetrics(final MeterRegistry registry, final Tags commonTags) {
    for (final String name :
        List.of(
            "generation",
            "authority.index",
            "blocked.seconds",
            "blocked.reason",
            "queued.partitions",
            "executing.partitions",
            "serving.partitions",
            "service.ready",
            "reserved.learner.slots",
            "learner.lag")) {
      gauges.put(
          name,
          MultiGauge.builder("taskite.pd.broker.partition.change." + name)
              .tags(commonTags)
              .register(registry));
    }
  }

  void update(final ProcessingState state) {
    final long now = System.currentTimeMillis();
    final Map<String, List<MultiGauge.Row<?>>> rows = new LinkedHashMap<>();
    gauges.keySet().forEach(name -> rows.put(name, new ArrayList<>()));
    for (final String cluster : state.getBrokerClusterState().clusterIds()) {
      final var pending = state.getBrokerTopologyChangeState().pendingChangePlans(cluster);
      final Tags clusterTags = Tags.of("cluster", cluster);
      rows.get("queued.partitions")
          .add(
              MultiGauge.Row.of(
                  clusterTags,
                  pending.stream()
                      .filter(plan -> plan.status() == BrokerPartitionChangeStatus.QUEUED)
                      .count()));
      rows.get("executing.partitions")
          .add(
              MultiGauge.Row.of(
                  clusterTags, pending.stream().filter(BrokerMigrationBudget::admitted).count()));
      final Map<String, Integer> incoming = new LinkedHashMap<>();
      pending.stream()
          .filter(BrokerMigrationBudget::admitted)
          .forEach(
              plan ->
                  BrokerMigrationBudget.incomingLearners(plan)
                      .forEach(broker -> incoming.merge(broker, 1, Integer::sum)));
      state
          .getBrokerRegistrationState()
          .registrations(cluster)
          .forEach(
              broker ->
                  rows.get("reserved.learner.slots")
                      .add(
                          MultiGauge.Row.of(
                              clusterTags.and("broker", broker.brokerId()),
                              incoming.getOrDefault(broker.brokerId(), 0))));
      long serving = 0;
      try {
        final var topology =
            new BrokerTopologyCalculator(state).calculateBrokerTopology(cluster, Instant.EPOCH);
        serving = topology.partitions().stream().filter(Partition::serviceReady).count();
        topology
            .partitions()
            .forEach(
                partition ->
                    rows.get("service.ready")
                        .add(
                            MultiGauge.Row.of(
                                clusterTags.and("partition", "" + partition.partitionId()),
                                partition.serviceReady() ? 1 : 0)));
      } catch (final RuntimeException unavailable) {
        // No routable topology at bootstrap or when every Broker is offline.
      }
      rows.get("serving.partitions").add(MultiGauge.Row.of(clusterTags, serving));
      state
          .getBrokerPartitionTopologyState()
          .partitionTopologies(cluster)
          .forEach(
              partition ->
                  partition
                      .learners()
                      .forEach(
                          learner -> {
                            final var replica = partition.replicaStates().get(learner);
                            if (replica != null) {
                              rows.get("learner.lag")
                                  .add(
                                      MultiGauge.Row.of(
                                          clusterTags.and(
                                              "partition",
                                              "" + partition.partitionId(),
                                              "broker",
                                              learner),
                                          replica.lag()));
                            }
                          }));
      for (final var plan : pending) {
        final Tags tags = Tags.of("cluster", cluster, "partition", "" + plan.partitionId());
        rows.get("generation").add(MultiGauge.Row.of(tags, plan.execution().generation()));
        rows.get("authority.index").add(MultiGauge.Row.of(tags, plan.execution().authorityIndex()));
        final long since = plan.execution().blockedSinceEpochMillis();
        rows.get("blocked.seconds")
            .add(MultiGauge.Row.of(tags, since <= 0 ? 0 : Math.max(0, now - since) / 1000D));
        if (plan.execution().blockedReason() != PlanBlockedReason.NONE) {
          rows.get("blocked.reason")
              .add(
                  MultiGauge.Row.of(
                      tags.and("reason", plan.execution().blockedReason().wireValue()), 1));
        }
      }
    }
    gauges.forEach((name, gauge) -> gauge.register(rows.get(name), true));
  }

  @Override
  public void close() {
    gauges.values().forEach(gauge -> gauge.register(List.of(), true));
  }
}
