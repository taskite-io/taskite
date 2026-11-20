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

import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState.Entry;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.TaskType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class BrokerTopologyCoordinatedApplier
    implements TypedEventApplier<TopologyIntent, BrokerTopologyCoordinationRecord> {
  private final MutableProcessingState state;

  public BrokerTopologyCoordinatedApplier(final MutableProcessingState state) {
    this.state = state;
  }

  @Override
  public void applyState(final long key, final BrokerTopologyCoordinationRecord value) {
    final var scheduling = state.getBrokerTopologyCoordinatorState();
    scheduling.advanceTime(value.getCoordinatedAt());
    if (value.getRevision() == 0 && value.getTaskType() == TaskType.RECOVERY_SCAN) {
      scheduling.startRecoveryScan();
      return;
    }
    final var current =
        scheduling.getTask(value.getClusterId(), value.getPartitionId()).orElse(null);
    if (current == null || current.revision() != value.getRevision()) {
      return;
    }
    for (final var resource : value.getResources()) {
      scheduling.markPartition(resource.clusterId(), resource.partitionId());
    }
    if (value.isFinished()) {
      scheduling.removeTask(value.getClusterId(), value.getPartitionId());
    } else {
      scheduling.putTask(
          new Entry(
              value.getClusterId(),
              value.getPartitionId(),
              current.revision(),
              value.getCursorCluster(),
              value.getCursorPartition(),
              value.getDueAt(),
              value.isObservingSubmit()));
    }
  }
}
