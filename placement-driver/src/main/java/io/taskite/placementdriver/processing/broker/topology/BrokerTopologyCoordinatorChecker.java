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
package io.taskite.placementdriver.processing.broker.topology;

import io.taskite.placementdriver.state.immutable.BrokerTopologyCoordinatorState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerTopologyCoordinationRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Reads durable dirty deadlines and submits commands, without placement calculations or writes. */
public final class BrokerTopologyCoordinatorChecker implements StreamProcessorLifecycleAware {
  private final BrokerTopologyCoordinatorState state;
  private final BooleanSupplier leader;
  private final LongSupplier clock;
  private final DueDateChecker checker;
  private boolean recoveryCommandPending;

  public BrokerTopologyCoordinatorChecker(
      final BrokerTopologyCoordinatorState state,
      final BooleanSupplier leader,
      final LongSupplier clock) {
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
    if (recoveryCommandPending) {
      recoveryCommandPending =
          !writer.tryAppend(
              0,
              ValueType.TOPOLOGY,
              TopologyIntent.COORDINATE,
              new BrokerTopologyCoordinationRecord());
      return now;
    }
    for (final var entry : state.dueBefore(now, Math.min(100, writer.remainingCapacity()))) {
      if (!writer.tryAppend(
          0,
          ValueType.TOPOLOGY,
          TopologyIntent.COORDINATE,
          new BrokerTopologyCoordinationRecord()
              .setClusterId(entry.clusterId())
              .setPartitionId(entry.partitionId())
              .setRevision(entry.revision()))) {
        return now;
      }
    }
    return state.nextDueAt();
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    // Lifecycle seed only; the business scan cursor is stored in RocksDB.
    recoveryCommandPending = true;
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.TOPOLOGY
        || command.getValueType() == ValueType.BROKER_REGISTRATION
        || command.getValueType() == ValueType.PARTITION_CHANGE
        || command.getValueType() == ValueType.COMMAND_DISTRIBUTION
        || command.getValueType() == ValueType.MEMBER_CHANGE) {
      final long next = state.nextDueAt();
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
