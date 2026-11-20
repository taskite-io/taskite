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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.appliers.*;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.*;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerElectionMetricsTest {
  @Test
  void metricsUseCompleteReportsAndPdLeaseStateInsteadOfClientTimeout(
      @TempDir final Path directory) {
    final var registry = new SimpleMeterRegistry();
    try (var storage = new PartitionStorage(directory)) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getBrokerPartitionTopologyState()
          .putPartitionTopology(
              new PartitionTopology(
                  new PartitionTopology.Identity("demo", 1),
                  new PartitionTopology.Placement(1, List.of("b0"), "b0"),
                  new PartitionTopology.Membership(List.of("b0"), List.of(), Map.of("b0", 10), 0),
                  PartitionStatus.ACTIVE,
                  Map.of()));
      final var report =
          new BrokerRegistrationRecord.PartitionStateRecord()
              .setPartitionId(1)
              .setRole(MemberRole.LEADER)
              .setTerm(1)
              .setLifecycleState(BrokerPartitionLifecycle.ACTIVE)
              .setPriority(10)
              .setMatchIndex(5)
              .setElection(
                  new BrokerPartitionElectionState(
                      new RaftConfigurationReference(5, "config"),
                      new BrokerPartitionElectionState.Priorities(10, 10),
                      new BrokerPartitionElectionState.Observation(
                          RaftNodeState.STATE_LEADER, "b0", true, 999999)));
      final var registration =
          new BrokerRegistrationRecord()
              .setClusterId("demo")
              .setBrokerId("b0")
              .setHost("127.0.0.1")
              .setPort(26501)
              .setRaftPort(26502)
              .setRenewalTimeoutMillis(1)
              .setPartitions(List.of(report));
      state.getBrokerClusterState().remember("demo", 1);
      new BrokerRegisteredApplier(state, () -> Instant.ofEpochMilli(1000))
          .applyState(1, registration);
      try (var metrics = new BrokerElectionMetrics(registry, Tags.empty())) {
        metrics.update(state);
        assertEquals(
            1,
            gauge(registry, "eligible.voters"),
            "client timeout does not override the PD online state");
        assertEquals(0, gauge(registry, "leader.missing.seconds"));
        new BrokerRenewedApplier(state, () -> Instant.ofEpochMilli(2000))
            .applyState(
                2,
                new BrokerRegistrationRecord()
                    .setClusterId("demo")
                    .setBrokerId("b0")
                    .setPreservePartitionObservations(true));
        metrics.update(state);
        assertEquals(0, gauge(registry, "eligible.voters"));
        new BrokerRenewedApplier(state, () -> Instant.ofEpochMilli(3000))
            .applyState(3, registration);
        metrics.update(state);
        assertEquals(1, gauge(registry, "eligible.voters"));
        new BrokerMarkedOfflineApplier(state, () -> Instant.ofEpochMilli(4000))
            .applyState(4, registration);
        metrics.update(state);
        assertEquals(0, gauge(registry, "eligible.voters"));
      }
    } finally {
      registry.close();
    }
  }

  private static double gauge(final SimpleMeterRegistry registry, final String name) {
    return registry
        .get("taskite.pd.partition.election." + name)
        .tags("cluster", "demo", "partition", "1")
        .gauge()
        .value();
  }
}
