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

import io.taskite.placementdriver.state.immutable.CommandDistributionState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;

public interface MutableCommandDistributionState extends CommandDistributionState {
  @Override
  MutableBrokerDistributionScheduleState scheduling();

  void rebuildPendingIndex();

  void putDistribution(CommandDistributionRecord<?> record);

  void putDedupeKey(String dedupeKey, long distributionKey);

  void putTarget(long distributionKey, String clusterId, int partitionCount);

  void resetTarget(long distributionKey, String clusterId, int partitionCount);

  void markPending(long distributionKey, String clusterId, int partitionId);

  boolean acknowledge(long distributionKey, String clusterId, int partitionId);

  void deleteCluster(String clusterId);
}
