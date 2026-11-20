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
package io.taskite.placementdriver.processing.topology;

import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Only reads durable deadlines and emits EXECUTE; all business decisions belong to the Processor.
 */
public final class TopologyMembershipChangeChecker implements StreamProcessorLifecycleAware {
  private final ProcessingState state;
  private final BooleanSupplier leader;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public TopologyMembershipChangeChecker(
      final ProcessingState state, final BooleanSupplier leader, final LongSupplier clock) {
    this.state = state;
    this.leader = leader;
    this.clock = clock;
    checker = new DueDateChecker(Duration.ofMillis(100), this::check, clock);
  }

  private long check(final CommandWriter writer) {
    if (!leader.getAsBoolean()) {
      return -1;
    }
    final long now = clock.getAsLong();
    for (final var entry :
        state
            .getTopologyMembershipChangeState()
            .dueBefore(now, Math.min(100, writer.remainingCapacity()))) {
      final var key = entry.key();
      if (key.scope() == Scope.BROKER_PARTITION) {
        final var plan =
            state.getBrokerTopologyChangeState().changePlan(key.clusterId(), key.changeId());
        if (plan.isPresent()
            && !writer.tryAppend(
                0,
                ValueType.PARTITION_CHANGE,
                BrokerPartitionChangeIntent.EXECUTE,
                BrokerPartitionChangeRecordMapper.fromPlan(plan.get()))) {
          return now;
        }
      } else {
        final var scope =
            key.scope() == Scope.BROKER_MEMBER
                ? MemberChangeScope.BROKER
                : MemberChangeScope.PLACEMENT_DRIVER;
        final var change = state.getMemberChangeState().get(scope, key.clusterId(), key.changeId());
        if (change.isPresent()
            && !writer.tryAppend(
                0, ValueType.MEMBER_CHANGE, MemberChangeIntent.EXECUTE, change.get())) {
          return now;
        }
      }
    }
    final long next = state.getTopologyMembershipChangeState().nextDueAt();
    return next >= 0 && next <= now ? now + 100 : next;
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> record) {
    final long next = state.getTopologyMembershipChangeState().nextDueAt();
    if (next >= 0) {
      checker.schedule(next);
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }
}
