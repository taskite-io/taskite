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
import io.taskite.placementdriver.broker.registry.BrokerPartitionReports;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.topology.ReplicaLifecycleState;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Actor-sampled election diagnostics; scrapes never access RocksDB or live Raft nodes. */
final class BrokerElectionMetrics implements AutoCloseable {
  private final Map<String, MultiGauge> gauges = new LinkedHashMap<>();
  private final Map<String, Long> leaderMissingSince = new LinkedHashMap<>();

  BrokerElectionMetrics(final MeterRegistry registry, final Tags tags) {
    for (final String name :
        List.of(
            "leader.missing.seconds",
            "available.voters",
            "eligible.voters",
            "local.priority",
            "desired.priority",
            "target.priority",
            "priority.mismatch",
            "service.ready",
            "observation.age.seconds",
            "configuration.index",
            "failure.reason")) {
      gauges.put(
          name,
          MultiGauge.builder("taskite.pd.partition.election." + name)
              .tags(tags)
              .register(registry));
    }
  }

  void update(final ProcessingState state) {
    final long now = System.currentTimeMillis();
    final Map<String, List<MultiGauge.Row<?>>> rows = new LinkedHashMap<>();
    gauges.keySet().forEach(name -> rows.put(name, new ArrayList<>()));
    final Set<String> partitions = new HashSet<>();
    for (final String cluster : state.getBrokerClusterState().clusterIds()) {
      final Map<String, BrokerRegistration> registrations = new LinkedHashMap<>();
      state
          .getBrokerRegistrationState()
          .registrations(cluster)
          .forEach(broker -> registrations.put(broker.brokerId(), broker));
      for (final var topology :
          state.getBrokerPartitionTopologyState().partitionTopologies(cluster)) {
        final String key = cluster + ":" + topology.partitionId();
        partitions.add(key);
        final Tags tags = Tags.of("cluster", cluster, "partition", "" + topology.partitionId());
        int available = 0;
        int eligible = 0;
        boolean leader = false;
        boolean faulted = false;
        boolean unknown = false;
        for (final String voter : topology.currentVoters()) {
          final var broker = registrations.get(voter);
          final var replica = topology.replicaStates().get(voter);
          final Tags replicaTags = tags.and("broker", voter);
          final var report =
              broker == null
                  ? null
                  : broker.partitions().stream()
                      .filter(partition -> partition.partitionId() == topology.partitionId())
                      .filter(partition -> BrokerPartitionReports.current(broker, partition))
                      .findFirst()
                      .orElse(null);
          final boolean fresh =
              report != null
                  && replica != null
                  && replica.lastReportTimeEpochMillis()
                      == report.election().observedAtEpochMillis();
          if (fresh) {
            available++;
          }
          final boolean known = fresh && !replica.election().configurationDigest().isBlank();
          final boolean qualified =
              known
                  && replica.priority() > 0
                  && replica.role() != MemberRole.LEARNER
                  && (replica.election().nodeState() == RaftNodeState.STATE_FOLLOWER
                      || replica.election().nodeState() == RaftNodeState.STATE_CANDIDATE
                      || replica.election().nodeState() == RaftNodeState.STATE_LEADER);
          if (qualified) {
            eligible++;
          }
          if (!known) {
            unknown = true;
          }
          if (fresh && replica.lifecycle() == ReplicaLifecycleState.FAULTED) {
            faulted = true;
          }
          leader |=
              qualified && replica.role() == MemberRole.LEADER && replica.election().serviceReady();
          final int desired = topology.priorities().getOrDefault(voter, -1);
          add(rows, "local.priority", replicaTags, known ? replica.priority() : -1);
          add(rows, "desired.priority", replicaTags, desired);
          add(
              rows,
              "target.priority",
              replicaTags,
              known ? replica.election().targetPriority() : -1);
          add(
              rows,
              "priority.mismatch",
              replicaTags,
              known && replica.priority() != desired ? 1 : 0);
          add(
              rows,
              "service.ready",
              replicaTags,
              known && replica.election().serviceReady() ? 1 : 0);
          add(
              rows,
              "configuration.index",
              replicaTags,
              known ? replica.election().configurationIndex() : 0);
          add(
              rows,
              "observation.age.seconds",
              replicaTags,
              replica == null
                  ? -1
                  : Math.max(0, now - replica.lastReportTimeEpochMillis()) / 1000D);
        }
        if (leader) {
          leaderMissingSince.remove(key);
        } else {
          leaderMissingSince.putIfAbsent(key, now);
        }
        add(
            rows,
            "leader.missing.seconds",
            tags,
            leader ? 0 : (now - leaderMissingSince.get(key)) / 1000D);
        add(rows, "available.voters", tags, available);
        add(rows, "eligible.voters", tags, eligible);
        final String reason =
            leader
                ? "ready"
                : available < topology.currentVoters().size() / 2 + 1
                    ? "quorum_unavailable"
                    : faulted
                        ? "faulted"
                        : unknown
                            ? "group_unready"
                            : eligible == 0 ? "no_eligible_voter" : "electing";
        add(rows, "failure.reason", tags.and("reason", reason), 1);
      }
    }
    leaderMissingSince.keySet().retainAll(partitions);
    gauges.forEach((name, gauge) -> gauge.register(rows.get(name), true));
  }

  private static void add(
      final Map<String, List<MultiGauge.Row<?>>> rows,
      final String name,
      final Tags tags,
      final Number value) {
    rows.get(name).add(MultiGauge.Row.of(tags, value));
  }

  @Override
  public void close() {
    gauges.values().forEach(gauge -> gauge.register(List.of(), true));
    leaderMissingSince.clear();
  }
}
