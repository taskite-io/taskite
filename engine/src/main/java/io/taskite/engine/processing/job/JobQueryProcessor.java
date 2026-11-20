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

import io.taskite.engine.processing.common.JobActivationBehavior;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Objects;

public final class JobQueryProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String JOB_NOT_FOUND =
      "Expected to query job with key '%d' for tenant '%s', but no such job was found";

  private final Writers writers;
  private final JobState jobState;
  private final JobActivationBehavior jobDetails;

  public JobQueryProcessor(final Writers writers, final ProcessingState processingState) {
    this.writers = writers;
    jobState = processingState.getJobState();
    jobDetails = new JobActivationBehavior(processingState);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> record) {
    final long jobKey = record.getKey();
    final JobRecord job = jobState.getJob(jobKey);
    if (job == null || !Objects.equals(job.getTenantId(), record.getValue().getTenantId())) {
      final String reason = String.format(JOB_NOT_FOUND, jobKey, record.getValue().getTenantId());
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.NOT_FOUND, reason);
      return;
    }
    final JobRecord queryResult = jobDetails.withVariables(jobKey, job);
    writers.state().appendFollowUpEvent(jobKey, JobIntent.QUERIED, queryResult);
    writers.response().writeEventOnCommand(jobKey, JobIntent.QUERIED, queryResult, record);
  }
}
