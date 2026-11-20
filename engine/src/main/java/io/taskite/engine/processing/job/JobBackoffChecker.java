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
package io.taskite.engine.processing.job;

import io.taskite.engine.state.immutable.JobState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.LongSupplier;

/** Reads the ordered failed-job backoff index and produces recurrence commands. */
public final class JobBackoffChecker implements StreamProcessorLifecycleAware {
  private static final int BATCH_SIZE = 100;
  private final JobState jobState;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public JobBackoffChecker(final JobState jobState, final LongSupplier clock) {
    this.jobState = jobState;
    this.clock = clock;
    checker = new DueDateChecker(Duration.ofMillis(100), this::check, clock);
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.JOB
        || command.getValueType() == ValueType.JOB_BATCH
        || command.getValueType() == ValueType.INCIDENT) {
      checker.schedule(clock.getAsLong());
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }

  long check(final CommandWriter commands) {
    final long now = clock.getAsLong();
    if (commands.hasRemainingCapacity()) {
      final int limit = Math.min(BATCH_SIZE, commands.remainingCapacity());
      for (final var job : jobState.findBackedOffJobs(now, limit)) {
        final var record =
            new JobRecord().setTenantId(job.tenantId()).setRecurringTime(job.dueDate());
        if (!commands.tryAppend(job.key(), ValueType.JOB, JobIntent.RECUR, record)) {
          break;
        }
      }
    }
    final long next = jobState.getNextBackoffDueDate();
    // Re-read due entries after the commands have been applied, yielding at checker resolution.
    return next == Long.MAX_VALUE ? -1 : next;
  }
}
