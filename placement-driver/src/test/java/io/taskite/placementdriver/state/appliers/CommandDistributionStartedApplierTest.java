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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class CommandDistributionStartedApplierTest {
  private static final long DISTRIBUTION_KEY = 101;

  @TempDir Path tempDir;

  @Test
  void createsPendingOnlyForExplicitPartition() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("explicit-partition"))) {
      final MutableProcessingState state = state(storage);
      final CommandDistributionRecord<JobSubscriptionRecord> distribution =
          distribution().setPartitionId(2);

      new CommandDistributionStartedApplier(state).applyState(DISTRIBUTION_KEY, distribution);

      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "demo", 1));
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "demo", 2));
      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "demo", 3));
      final var target = state.getCommandDistributionState().targets(DISTRIBUTION_KEY).getFirst();
      assertEquals(2, target.getExpectedPartitionCount());
      assertEquals(1, target.getAcknowledgedPartitionCount());

      state.getCommandDistributionState().acknowledge(DISTRIBUTION_KEY, "demo", 2);

      assertTrue(state.getCommandDistributionState().isTargetComplete(DISTRIBUTION_KEY, "demo"));
    }
  }

  @Test
  void replayDoesNotRecreateAcknowledgedPendingState() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("replay"))) {
      final MutableProcessingState state = state(storage);
      registerTopology(state, 2);
      final CommandDistributionRecord<JobSubscriptionRecord> distribution = distribution();
      final CommandDistributionStartedApplier applier =
          new CommandDistributionStartedApplier(state);

      applier.applyState(DISTRIBUTION_KEY, distribution);
      state.getCommandDistributionState().acknowledge(DISTRIBUTION_KEY, "demo", 1);
      applier.applyState(DISTRIBUTION_KEY, distribution);

      assertFalse(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "demo", 1));
      assertTrue(state.getCommandDistributionState().isPending(DISTRIBUTION_KEY, "demo", 2));
    }
  }

  private static MutableProcessingState state(final PartitionStorage storage) {
    return new MutableProcessingState(new StateStoreProcessingStore(storage));
  }

  private static void registerTopology(
      final MutableProcessingState state, final int partitionSize) {
    new BrokerClusterCreatedApplier(state)
        .applyState(
            1,
            new ClusterConfigRecord()
                .setClusterKey(state.getBrokerClusterState().latestClusterKey() + 1)
                .setClusterId("demo")
                .setClusterSize(1)
                .setPartitionSize(partitionSize)
                .setReplicationFactor(1));
  }

  private static CommandDistributionRecord<JobSubscriptionRecord> distribution() {
    return new CommandDistributionRecord<JobSubscriptionRecord>()
        .setDistributionKey(DISTRIBUTION_KEY)
        .setClusterId("demo")
        .setValueType(ValueType.JOB_SUBSCRIPTION)
        .setIntent(JobSubscriptionIntent.CREATE)
        .setValue(
            new JobSubscriptionRecord()
                .setClusterId("demo")
                .setTenantId("tenant")
                .setName("charge")
                .setJobType("process")
                .setVariables("<xml />"));
  }
}
