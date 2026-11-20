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
package io.taskite.placementdriver.state.appliers;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

final class PartitionActivationTest {

  @TempDir Path tempDir;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "broker-3",
        "broker-3,broker-4",
        "broker-4,broker-3",
        "broker-3,unexpected",
        "excluded",
        "broker-3,broker-4,unexpected"
      })
  void bootstrapRequiresExactlyThePlannedLearners(final String learners) {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("bootstrap-learners"))) {
      final var state = state(storage);
      new BrokerClusterCreatedApplier(state)
          .applyState(
              1,
              new ClusterConfigRecord()
                  .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                  .setClusterId("demo")
                  .setClusterSize(3)
                  .setPartitionSize(5)
                  .setReplicationFactor(3));
      for (int i = 0; i < 5; i++) {
        new BrokerRegisteredApplier(state, () -> Instant.EPOCH)
            .applyState(
                i,
                new BrokerRegistrationRecord()
                    .setClusterId("demo")
                    .setBrokerId("broker-" + i)
                    .setHost("127.0.0.1")
                    .setPort(26501)
                    .setRaftPort(26502)
                    .setVersion("test"));
      }
      final boolean excluded = learners.equals("excluded");
      if (excluded) {
        state.getBrokerRegistrationState().excludeFromPlacement("demo", "broker-4");
      }
      final var voters = List.of("broker-0", "broker-1", "broker-2");
      final var expectedLearners = excluded ? List.of("broker-3") : List.of("broker-3", "broker-4");
      final var currentLearners =
          excluded
              ? List.of("broker-3")
              : learners.isEmpty() ? List.<String>of() : List.of(learners.split(","));
      final var topology =
          new PartitionTopology(
              new PartitionTopology.Identity("demo", 1),
              new PartitionTopology.Placement(3, voters, "broker-0"),
              new PartitionTopology.Membership(
                  voters,
                  currentLearners,
                  PartitionTopology.Membership.initial(voters, expectedLearners, "broker-0")
                      .priorities(),
                  0),
              PartitionStatus.REBALANCING,
              Map.of(),
              true);
      final var completed = PartitionActivation.finishBootstrap(state, topology);
      // Keep priorities equal to the plan to test membership independently of the priority gate.
      if (Set.copyOf(currentLearners).equals(Set.copyOf(expectedLearners))) {
        assertFalse(completed.initializing());
      } else {
        assertTrue(completed.initializing());
      }
    }
  }

  @Test
  void redistributionIsIncompleteWhileTargetClusterDeploymentIsPending() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("pending-runtime"))) {
      final MutableProcessingState state = state(storage);
      state.getCommandDistributionState().putDistribution(deployment(11, "demo"));
      state.getCommandDistributionState().markPending(11, "demo", 2);

      assertFalse(PartitionActivation.redistributionComplete(state, "demo", 2));

      state.getCommandDistributionState().acknowledge(11, "demo", 2);
      assertTrue(PartitionActivation.redistributionComplete(state, "demo", 2));
    }
  }

  @Test
  void redistributionIgnoresOtherClustersAndPartitions() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("isolated-runtime"))) {
      final MutableProcessingState state = state(storage);
      state.getCommandDistributionState().putDistribution(deployment(21, "other"));
      state.getCommandDistributionState().markPending(21, "other", 2);
      state.getCommandDistributionState().putDistribution(deployment(22, "demo"));
      state.getCommandDistributionState().markPending(22, "demo", 1);

      assertTrue(PartitionActivation.redistributionComplete(state, "demo", 2));
    }
  }

  @Test
  void redistributionIgnoresPendingDeletion() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("deletion-runtime"))) {
      final MutableProcessingState state = state(storage);
      state.getCommandDistributionState().putDistribution(deletion(31));
      state.getCommandDistributionState().markPending(31, "demo", 2);

      assertTrue(PartitionActivation.redistributionComplete(state, "demo", 2));
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> deployment(
      final long distributionKey, final String clusterId) {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(distributionKey)
        .setClusterId(clusterId)
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId(clusterId)
                .setTenantId("tenant")
                .setName("subscription-" + distributionKey)
                .setJobType("process")
                .setVariables("<xml />"));
  }

  private static CommandDistributionRecord<JobSubscriptionDeletionRecord> deletion(
      final long distributionKey) {
    return new CommandDistributionRecord<JobSubscriptionDeletionRecord>()
        .setDistributionKey(distributionKey)
        .setClusterId("demo")
        .setValueType(ValueType.JOB_SUBSCRIPTION_DELETION)
        .setIntent(JobSubscriptionIntent.DELETE)
        .setValue(
            new JobSubscriptionDeletionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setSubscriptionKey(11));
  }
}
