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
package io.taskite.placementdriver.state.immutable;

import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord.TaskType;
import java.util.List;
import java.util.Optional;

public interface BrokerTopologyCoordinatorState {
  /** Reads a schedulable task; global metadata is not a task identity. */
  Optional<Entry> getTask(String clusterId, int partitionId);

  /** The durable cursor for the scan across all clusters after recovery. */
  Optional<Entry> getRecoveryScan();

  /** Last deterministic event time observed by this queue, independent of task deadlines. */
  long getQueueTime();

  List<Entry> dueBefore(long now, int limit);

  long nextDueAt();

  int reservedPartitions(long now, int limit);

  record Entry(
      String clusterId,
      int partitionId,
      long revision,
      String cursorCluster,
      int cursorPartition,
      long dueAt,
      boolean observingSubmit) {
    public TaskType taskType() {
      return TaskType.forIdentity(clusterId, partitionId);
    }
  }
}
