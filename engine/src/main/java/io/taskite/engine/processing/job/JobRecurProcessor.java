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
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class JobRecurProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String PROCESSING_TIME_REQUIRED =
      "Expected to construct job recurrence processor, but the processing time supplier was null";
  private static final String JOB_NOT_FOUND =
      "Expected to recur job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_RECURRENCE =
      "Expected to recur job with key '%d' for tenant '%s' in state 'FAILED' with positive retries and elapsed matching"
          + " backoff, but state is '%s', retries are '%d', stored recurring time is '%d', and requested"
          + " recurring time is '%d'";

  private final JobState jobs;
  private final Writers writers;
  private final LongSupplier processingTime;

  public JobRecurProcessor(
      final JobState jobs, final Writers writers, final LongSupplier processingTime) {
    this.jobs = jobs;
    this.writers = writers;
    this.processingTime = Objects.requireNonNull(processingTime, PROCESSING_TIME_REQUIRED);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final var job = jobs.getJob(command.getKey());
    if (job == null || !Objects.equals(job.getTenantId(), command.getValue().getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, command.getKey(), command.getValue().getTenantId()));
    } else if (jobs.getState(command.getKey()) != JobState.State.FAILED
        || job.getRetries() <= 0
        || job.getRecurringTime() <= 0
        || (command.getValue().getRecurringTime() > 0
            && command.getValue().getRecurringTime() != job.getRecurringTime())
        || processingTime.getAsLong() < job.getRecurringTime()) {
      reject(
          command,
          RejectionType.INVALID_STATE,
          String.format(
              INVALID_RECURRENCE,
              command.getKey(),
              command.getValue().getTenantId(),
              jobs.getState(command.getKey()),
              job.getRetries(),
              job.getRecurringTime(),
              command.getValue().getRecurringTime()));
    } else {
      final var recurred = job.copy().setRecurringTime(0).setRetryBackoff(0);
      writers
          .state()
          .appendFollowUpEvent(command.getKey(), JobIntent.RECURRED_AFTER_BACKOFF, recurred);
      writers
          .response()
          .writeEventOnCommand(
              command.getKey(), JobIntent.RECURRED_AFTER_BACKOFF, recurred, command);
    }
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
