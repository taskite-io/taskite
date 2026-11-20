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

/** Checks the persisted deadline against the command timestamp; retries are unchanged. */
public final class JobTimeOutProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String PROCESSING_TIME_REQUIRED =
      "Expected to construct job timeout processor, but the processing time supplier was null";
  private static final String JOB_NOT_FOUND =
      "Expected to time out job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_TIMEOUT =
      "Expected to time out job with key '%d' for tenant '%s' in state 'ACTIVATED' after its deadline with matching"
          + " worker session, but state is '%s', stored deadline is '%d', requested deadline is '%d',"
          + " stored Worker is '%s' with lease token '%d', and requested Worker is '%s' with lease token '%d'";

  private final JobState jobs;
  private final Writers writers;
  private final LongSupplier processingTime;

  public JobTimeOutProcessor(
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
    } else if (jobs.getState(command.getKey()) != JobState.State.ACTIVATED
        || (command.getValue().getDeadline() > 0
            && command.getValue().getDeadline() != job.getDeadline())
        || command.getValue().getWorkerLeaseToken() != job.getWorkerLeaseToken()
        || !Objects.equals(command.getValue().getWorkerId(), job.getWorkerId())
        || processingTime.getAsLong() < job.getDeadline()) {
      reject(
          command,
          RejectionType.INVALID_STATE,
          String.format(
              INVALID_TIMEOUT,
              command.getKey(),
              command.getValue().getTenantId(),
              jobs.getState(command.getKey()),
              job.getDeadline(),
              command.getValue().getDeadline(),
              job.getWorkerId(),
              job.getWorkerLeaseToken(),
              command.getValue().getWorkerId(),
              command.getValue().getWorkerLeaseToken()));
    } else {
      final var timedOut = job.copy().setDeadline(0);
      writers.state().appendFollowUpEvent(command.getKey(), JobIntent.TIMED_OUT, timedOut);
      writers
          .response()
          .writeEventOnCommand(command.getKey(), JobIntent.TIMED_OUT, timedOut, command);
    }
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
