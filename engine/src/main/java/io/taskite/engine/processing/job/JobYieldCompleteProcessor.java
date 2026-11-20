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
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;

public final class JobYieldCompleteProcessor implements TypedRecordProcessor<JobYieldRecord> {
  private final ProcessingState state;
  private final Writers writers;

  public JobYieldCompleteProcessor(final ProcessingState state, final Writers writers) {
    this.state = state;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<JobYieldRecord> command) {
    if (!state.workerJobLeaseTokenEnabled()) {
      return;
    }
    final var plan = state.getJobYieldState().get(command.getValue().getYieldId());
    if (plan != null
        && plan.sameIdentity(command.getValue())
        && plan.getStatus() == JobYieldStatus.PENDING
        && state
            .getJobState()
            .findActivatedByWorker(
                plan.getTenantId(), plan.getWorkerId(), plan.getWorkerLeaseToken(), 1)
            .isEmpty()) {
      writers
          .state()
          .appendFollowUpEvent(
              command.getKey(),
              JobYieldIntent.YIELD_COMPLETED,
              plan.setStatus(JobYieldStatus.COMPLETED));
    }
  }
}
