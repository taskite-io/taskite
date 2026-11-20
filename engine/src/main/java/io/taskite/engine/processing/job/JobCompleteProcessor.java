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
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Map;
import java.util.Objects;

public final class JobCompleteProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String JOB_NOT_FOUND =
      "Expected to complete job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_WORKER_SESSION =
      "Expected to complete job with key '%d' for tenant '%s' using its active Worker session, but"
          + " requested Worker is '%s' with lease token '%d', stored Worker is '%s' with lease token"
          + " '%d', job state is '%s', and worker session recovery readiness is '%s'";
  private static final String INVALID_JOB_STATE =
      "Expected to complete job with key '%d' for tenant '%s' in state 'ACTIVATABLE' or 'ACTIVATED', but it is in state '%s'";
  private static final String VARIABLES_REQUIRED =
      "Expected to complete job with key '%d' for tenant '%s' with a JSON object of variables, but variables were null";
  private static final String INVALID_VARIABLES =
      "Expected to complete job with key '%d' for tenant '%s' with a JSON object of variables, but validation failed: %s";

  private final Writers writers;
  private final JobState jobState;
  private final WorkerJobLeaseCheck leaseCheck;

  public JobCompleteProcessor(
      final KeyGenerator keyGenerator,
      final Writers writers,
      final ProcessingState processingState) {
    this.writers = writers;
    jobState = processingState.getJobState();
    leaseCheck = new WorkerJobLeaseCheck(processingState);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> record) {
    final long jobKey = record.getKey();
    final JobRecord job = jobState.getJob(jobKey);
    if (job == null || !Objects.equals(job.getTenantId(), record.getValue().getTenantId())) {
      appendRejection(
          record,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, jobKey, record.getValue().getTenantId()));
      return;
    }

    if (!leaseCheck.canFinish(jobKey, job, record.getValue())) {
      appendRejection(
          record,
          leaseCheck.ready()
              ? RejectionType.STALE_WORKER_SESSION
              : RejectionType.WORKER_SESSION_NOT_READY,
          String.format(
              INVALID_WORKER_SESSION,
              jobKey,
              record.getValue().getTenantId(),
              record.getValue().getWorkerId(),
              record.getValue().getWorkerLeaseToken(),
              job.getWorkerId(),
              job.getWorkerLeaseToken(),
              jobState.getState(jobKey),
              leaseCheck.ready()));
      return;
    }
    final JobState.State state = jobState.getState(jobKey);
    if (state != JobState.State.ACTIVATED && state != JobState.State.ACTIVATABLE) {
      appendRejection(
          record,
          RejectionType.INVALID_STATE,
          String.format(INVALID_JOB_STATE, jobKey, record.getValue().getTenantId(), state));
      return;
    }

    final var completed = job.copy().setVariablesJson(record.getValue().getVariablesJson());
    final Map<String, Object> resultVariables;
    try {
      resultVariables = completed.getVariables();
      if (resultVariables == null) {
        throw new IllegalArgumentException(
            String.format(VARIABLES_REQUIRED, jobKey, record.getValue().getTenantId()));
      }
    } catch (final RuntimeException error) {
      appendRejection(
          record,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_VARIABLES, jobKey, record.getValue().getTenantId(), error.getMessage()));
      return;
    }
    writers.state().appendFollowUpEvent(jobKey, JobIntent.COMPLETED, completed);
    writers.response().writeEventOnCommand(jobKey, JobIntent.COMPLETED, completed, record);
  }

  private void appendRejection(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
