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

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import java.time.Duration;

/** Resumes persisted pending work after recovery, yielding between bounded rounds. */
public final class JobYieldChecker implements StreamProcessorLifecycleAware {
  private static final int QUERY_PAGE_SIZE = 100;
  private final ProcessingState state;
  private ProcessingScheduleService scheduling;
  private ScheduledTimer timer;
  private boolean closed;
  private long cursor;

  public JobYieldChecker(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    this.scheduling = scheduling;
    if (state instanceof final MutableProcessingState mutable) {
      mutable.getJobYieldState().resetSessionsReady();
    }
    schedule();
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.JOB_YIELD
        || (command.getValueType() == ValueType.JOB && command.getIntent() == JobIntent.YIELD)) {
      schedule();
    }
  }

  @Override
  public void onClose() {
    closed = true;
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }

  private void check(final ProcessingScheduleService.CommandWriter writer) {
    timer = null;
    if (closed || !state.workerJobLeaseTokenEnabled()) {
      return;
    }
    if (!state.getJobState().workerIndexReady()) {
      writer.tryAppend(0, ValueType.JOB_YIELD, JobYieldIntent.MIGRATE_INDEX, new JobYieldRecord());
      schedule();
      return;
    }
    if (!writer.hasRemainingCapacity()) {
      schedule();
      return;
    }
    final int pageSize = Math.min(QUERY_PAGE_SIZE, writer.remainingCapacity());
    var plans = state.getJobYieldState().pending(cursor, pageSize);
    if (plans.isEmpty()) {
      cursor = 0;
      plans = state.getJobYieldState().pending(cursor, pageSize);
    }
    for (final var plan : plans) {
      if (!writer.hasRemainingCapacity()) {
        break;
      }
      final var jobs =
          state
              .getJobState()
              .findActivatedByWorker(
                  plan.getTenantId(),
                  plan.getWorkerId(),
                  plan.getWorkerLeaseToken(),
                  Math.min(QUERY_PAGE_SIZE, writer.remainingCapacity()));
      if (jobs.isEmpty()) {
        // Do not carry diagnostic fields or synchronization pages into execution commands.
        final var completion =
            new JobYieldRecord()
                .setYieldId(plan.getYieldId())
                .setClusterId(plan.getClusterId())
                .setPartitionId(plan.getPartitionId())
                .setTenantId(plan.getTenantId())
                .setWorkerId(plan.getWorkerId())
                .setWorkerLeaseToken(plan.getWorkerLeaseToken())
                .setReason(plan.getReason());

        if (!writer.tryAppend(0, ValueType.JOB_YIELD, JobYieldIntent.COMPLETE_YIELD, completion)) {
          break;
        }
      } else {
        for (final long key : jobs) {
          final var record = new JobRecord().setTenantId(plan.getTenantId());
          if (!writer.tryAppend(key, ValueType.JOB, JobIntent.YIELD, record)) {
            break;
          }
        }
      }
      cursor = plan.getYieldId();
    }
    if (!plans.isEmpty()) {
      schedule();
    }
  }

  private void schedule() {
    if (!closed && state.workerJobLeaseTokenEnabled() && scheduling != null && timer == null) {
      timer = scheduling.schedule(Duration.ZERO, this::check);
    }
  }
}
