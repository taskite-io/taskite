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
package io.taskite.placementdriver;

import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.appliers.PlacementDriverEventAppliers;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;

/** Applies the current operation through its production event registration. */
public final class PartitionOperationFixture {
  private PartitionOperationFixture() {}

  public static void applyOperation(
      final MutableProcessingState state, final long key, final ChangePlan plan) {
    final var intent =
        switch (plan.nextOperation().type()) {
          case ADD -> BrokerPartitionChangeIntent.CREATED;
          case JOIN -> BrokerPartitionChangeIntent.JOINED;
          case PROMOTE_LEARNER -> BrokerPartitionChangeIntent.PROMOTED;
          case RECONFIGURE_PRIORITY -> BrokerPartitionChangeIntent.PRIORITY_CHANGED;
          case TRANSFER_LEADER -> BrokerPartitionChangeIntent.LEADER_TRANSFERRED;
          case STEP_DOWN_IF_NOT_PRIMARY -> BrokerPartitionChangeIntent.LEADER_STEPPED_DOWN;
          case LEAVE -> BrokerPartitionChangeIntent.LEFT;
          case RESET_PEERS -> BrokerPartitionChangeIntent.PEERS_RESET;
        };
    PlacementDriverEventAppliers.create(state)
        .applyState(key, intent, BrokerPartitionChangeRecordMapper.fromPlan(plan), 1);
  }
}
