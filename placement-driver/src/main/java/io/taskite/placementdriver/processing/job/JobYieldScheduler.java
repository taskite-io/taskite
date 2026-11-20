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
package io.taskite.placementdriver.processing.job;

import io.taskite.placementdriver.state.immutable.JobYieldState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Read-only Yield scheduling; all cursors, retries and reservations live in RocksDB. */
public final class JobYieldScheduler implements StreamProcessorLifecycleAware {
  static final int MAX_RESERVED = 16;
  static final int MAX_SYNC_RESERVED = 8;
  private final JobYieldState state;
  private final BooleanSupplier leader;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public JobYieldScheduler(
      final JobYieldState state, final BooleanSupplier leader, final LongSupplier clock) {
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
    final int syncReserved = state.synchronization().reservedTargets(now, MAX_SYNC_RESERVED);
    int available = MAX_RESERVED - syncReserved - state.reservedTargets(now, MAX_RESERVED);
    final int syncLimit =
        Math.max(
            0,
            Math.min(
                writer.remainingCapacity(), Math.min(available, MAX_SYNC_RESERVED - syncReserved)));
    for (final var entry : state.synchronization().dueBefore(now, syncLimit)) {
      if (!writer.tryAppend(
          0,
          ValueType.JOB_YIELD,
          JobYieldIntent.SYNC_PARTITION,
          new JobYieldRecord()
              .setClusterId(entry.clusterId())
              .setPartitionId(entry.partitionId())
              .setDispatchAttemptId(entry.attemptId())
              .setPreviousSyncCursor(entry.cursor())
              .setNextAttemptAtEpochMillis(entry.dueAt()))) {
        return now;
      }
      available--;
    }
    final int limit = Math.max(0, Math.min(available, writer.remainingCapacity()));
    for (final var target : state.dueBefore(now, limit)) {
      if (!writer.tryAppend(
          target.yieldId(),
          ValueType.JOB_YIELD,
          JobYieldIntent.DISPATCH,
          new JobYieldRecord()
              .setYieldId(target.yieldId())
              .setClusterId(target.clusterId())
              .setPartitionId(target.partitionId())
              .setNextAttemptAtEpochMillis(target.dueAt()))) {
        return now;
      }
    }
    return nextDueAt();
  }

  private long nextDueAt() {
    final long yield = state.nextDueAt();
    final long sync = state.synchronization().nextDueAt();
    return yield < 0 ? sync : sync < 0 ? yield : Math.min(yield, sync);
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.JOB_YIELD
        || command.getValueType() == ValueType.BROKER_REGISTRATION
        || command.getValueType() == ValueType.WORKER_REGISTRATION
        || command.getValueType() == ValueType.TOPOLOGY
        || command.getValueType() == ValueType.PARTITION_CHANGE) {
      final long due = nextDueAt();
      if (due >= 0) {
        checker.schedule(due);
      }
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }
}
