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

import io.taskite.engine.processing.common.WorkerJobLeaseCheck;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Resets an activated job's deadline relative to the command processing time. */
public final class JobUpdateTimeoutProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String PROCESSING_TIME_REQUIRED =
      "Expected to construct job timeout update processor, but the processing time supplier was null";
  private static final String JOB_NOT_FOUND =
      "Expected to update timeout for job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_JOB_STATE =
      "Expected to update timeout for job with key '%d' for tenant '%s' in state 'ACTIVATED' with a deadline, but state is '%s' and deadline is '%d'";
  private static final String INVALID_WORKER_SESSION =
      "Expected to update timeout for job with key '%d' for tenant '%s' using its active worker session, but requested Worker is '%s' with lease token '%d', stored Worker is '%s' with lease token '%d', and worker session recovery readiness is '%s'";
  private static final String INVALID_TIMEOUT =
      "Expected to update timeout for job with key '%d' for tenant '%s' to a positive value, but received '%d'";
  private static final String INVALID_DEADLINE =
      "Expected to update timeout for job with key '%d' for tenant '%s' with a supported deadline for timeout '%d', but validation failed: %s";

  private final JobState jobs;
  private final WorkerJobLeaseCheck leaseCheck;
  private final Writers writers;
  private final LongSupplier processingTime;

  public JobUpdateTimeoutProcessor(
      final ProcessingState state, final Writers writers, final LongSupplier processingTime) {
    jobs = state.getJobState();
    leaseCheck = new WorkerJobLeaseCheck(state);
    this.writers = writers;
    this.processingTime = Objects.requireNonNull(processingTime, PROCESSING_TIME_REQUIRED);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final long key = command.getKey();
    final var request = command.getValue();
    final var job = jobs.getJob(key);
    if (job == null || !Objects.equals(job.getTenantId(), request.getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, key, request.getTenantId()));
      return;
    }
    final var status = jobs.getState(key);
    if (status != JobState.State.ACTIVATED || job.getDeadline() <= 0) {
      reject(
          command,
          RejectionType.INVALID_STATE,
          String.format(INVALID_JOB_STATE, key, request.getTenantId(), status, job.getDeadline()));
      return;
    }
    if (!leaseCheck.canFinish(key, job, request)) {
      reject(
          command,
          leaseCheck.ready()
              ? RejectionType.STALE_WORKER_SESSION
              : RejectionType.WORKER_SESSION_NOT_READY,
          String.format(
              INVALID_WORKER_SESSION,
              key,
              request.getTenantId(),
              request.getWorkerId(),
              request.getWorkerLeaseToken(),
              job.getWorkerId(),
              job.getWorkerLeaseToken(),
              leaseCheck.ready()));
      return;
    }

    if (request.getTimeout() <= 0) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(INVALID_TIMEOUT, key, request.getTenantId(), request.getTimeout()));
      return;
    }

    final long deadline;
    try {
      deadline = Math.addExact(processingTime.getAsLong(), request.getTimeout());
    } catch (final ArithmeticException error) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_DEADLINE,
              key,
              request.getTenantId(),
              request.getTimeout(),
              error.getMessage()));
      return;
    }

    final var updated = job.copy().setDeadline(deadline);
    writers.state().appendFollowUpEvent(key, JobIntent.TIMEOUT_UPDATED, updated);
    writers.response().writeEventOnCommand(key, JobIntent.TIMEOUT_UPDATED, updated, command);
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
