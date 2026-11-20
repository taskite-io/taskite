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

import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionTargetRecord;
import java.util.List;
import java.util.Optional;

public interface CommandDistributionState {
  BrokerDistributionScheduleState scheduling();

  Optional<CommandDistributionRecord<?>> distribution(long distributionKey);

  Optional<Long> distributionKeyByDedupeKey(String dedupeKey);

  List<CommandDistributionRecord<?>> distributions();

  record PendingTarget(long distributionKey, String clusterId, int partitionId) {
    public static final PendingTarget START = new PendingTarget(0, "", 0);
  }

  List<PendingTarget> pendingTargetsAfter(PendingTarget cursor, int limit);

  boolean isPending(long distributionKey, String clusterId, int partitionId);

  List<Integer> pendingPartitions(long distributionKey, String clusterId);

  List<CommandDistributionTargetRecord> targets(long distributionKey);

  boolean isTarget(long distributionKey, String clusterId);

  boolean isTargetComplete(long distributionKey, String clusterId);

  boolean isPlanComplete(long distributionKey);
}
