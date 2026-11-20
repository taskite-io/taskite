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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState;

public interface MutableBrokerTopologyCoordinatorState extends BrokerTopologyCoordinatorState {
  /** Enqueues or restarts the cluster expansion using the persisted queue time. */
  void markCluster(String clusterId);

  void markCluster(String clusterId, long enqueuedAt);

  /** Enqueues a partition while preserving its waiting age or SUBMIT observation deadline. */
  void markPartition(String clusterId, int partitionId);

  void markPartition(String clusterId, int partitionId, long enqueuedAt);

  /** Starts a global recovery scan only if one is not already pending. */
  void startRecoveryScan();

  void advanceTime(long coordinatedAt);

  void putTask(Entry entry);

  void removeTask(String clusterId, int partitionId);

  void deleteCluster(String clusterId);
}
