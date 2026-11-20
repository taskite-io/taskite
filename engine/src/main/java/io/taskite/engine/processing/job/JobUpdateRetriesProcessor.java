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

public final class JobUpdateRetriesProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String JOB_NOT_FOUND =
      "Expected to update retries for job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_RETRIES =
      "Expected to update retries for job with key '%d' for tenant '%s' to a positive value, but received '%d'";

  private final JobState jobs;
  private final Writers writers;

  public JobUpdateRetriesProcessor(final JobState jobs, final Writers writers) {
    this.jobs = jobs;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final var job = jobs.getJob(command.getKey());
    if (job == null || !Objects.equals(job.getTenantId(), command.getValue().getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, command.getKey(), command.getValue().getTenantId()));
    } else if (command.getValue().getRetries() <= 0) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_RETRIES,
              command.getKey(),
              command.getValue().getTenantId(),
              command.getValue().getRetries()));
    } else {
      final var updated = job.copy().setRetries(command.getValue().getRetries());
      writers.state().appendFollowUpEvent(command.getKey(), JobIntent.RETRIES_UPDATED, updated);
      writers
          .response()
          .writeEventOnCommand(command.getKey(), JobIntent.RETRIES_UPDATED, updated, command);
    }
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
