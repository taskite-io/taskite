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
package io.taskite.placementdriver.processing.broker.distribution;

import io.taskite.placementdriver.state.immutable.CommandDistributionState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** B scheduling reads only the durable deadline index; no pending cursor or in-flight map. */
public final class BrokerDistributionScheduler implements StreamProcessorLifecycleAware {
  static final int MAX_RESERVED_TARGETS = 256;
  private final CommandDistributionState state;
  private final BooleanSupplier leader;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public BrokerDistributionScheduler(
      final CommandDistributionState state,
      final BooleanSupplier leader,
      final LongSupplier clock) {
    this.state = state;
    this.leader = leader;
    this.clock = clock;
    checker = new DueDateChecker(Duration.ofMillis(100), this::check, clock);
  }

  long check(final CommandWriter writer) {
    if (!leader.getAsBoolean()) {
      return -1;
    }
    final long now = clock.getAsLong();
    final int available =
        MAX_RESERVED_TARGETS - state.scheduling().reservedTargets(now, MAX_RESERVED_TARGETS);
    final int limit = Math.min(100, Math.min(available, writer.remainingCapacity()));
    if (limit > 0) {
      for (final var entry : state.scheduling().dueBefore(now, limit)) {
        final var target = entry.target();
        final var command =
            new CommandDistributionRecord<>()
                .setDistributionKey(target.distributionKey())
                .setClusterId(target.clusterId())
                .setPartitionId(target.partitionId())
                .setGeneration(entry.generation())
                .setDispatchAttemptId(entry.attemptId());
        if (!writer.tryAppend(
            target.distributionKey(),
            ValueType.COMMAND_DISTRIBUTION,
            CommandDistributionIntent.DISPATCH,
            command)) {
          return now;
        }
      }
    }
    return state.scheduling().nextDueAt();
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.COMMAND_DISTRIBUTION
        || command.getValueType() == ValueType.BROKER_REGISTRATION
        || command.getValueType() == ValueType.TOPOLOGY
        || command.getValueType() == ValueType.JOB_SUBSCRIPTION_MIRROR
        || command.getValueType() == ValueType.PARTITION_CHANGE) {
      final long next = state.scheduling().nextDueAt();
      if (next >= 0) {
        checker.schedule(next);
      }
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }
}
