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

/** Yield restores availability without consuming retries or simulating a timeout. */
public final class JobYieldProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String JOB_NOT_FOUND =
      "Expected to yield job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_JOB_STATE =
      "Expected to yield job with key '%d' for tenant '%s' in state 'ACTIVATED', but it is in state '%s'";

  private final JobState jobs;
  private final Writers writers;

  public JobYieldProcessor(final JobState jobs, final Writers writers) {
    this.jobs = jobs;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final long jobKey = command.getKey();
    final var request = command.getValue();
    final var job = jobs.getJob(jobKey);
    if (job == null || !Objects.equals(job.getTenantId(), request.getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, jobKey, request.getTenantId()));
      return;
    }

    final var status = jobs.getState(jobKey);
    if (status != JobState.State.ACTIVATED) {
      reject(
          command,
          RejectionType.INVALID_STATE,
          String.format(INVALID_JOB_STATE, jobKey, request.getTenantId(), status));
      return;
    }
    final var yielded = job.copy().setDeadline(0);
    writers.state().appendFollowUpEvent(jobKey, JobIntent.YIELDED, yielded);
    writers.response().writeEventOnCommand(jobKey, JobIntent.YIELDED, yielded, command);
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
