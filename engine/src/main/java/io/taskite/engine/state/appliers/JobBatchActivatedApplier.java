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
package io.taskite.engine.state.appliers;

import io.taskite.engine.state.mutable.MutableJobState;
import io.taskite.protocol.impl.record.value.broker.JobBatchRecord;
import io.taskite.protocol.record.intent.broker.JobBatchIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

final class JobBatchActivatedApplier implements TypedEventApplier<JobBatchIntent, JobBatchRecord> {
  private final MutableJobState jobs;

  JobBatchActivatedApplier(MutableJobState jobs) {
    this.jobs = jobs;
  }

  @Override
  public void applyState(long key, JobBatchRecord value) {
    for (final var activated : value.getJobs()) {
      final var previous = jobs.getJob(activated.key());
      if (previous == null) {
        throw new IllegalStateException("Activated job does not exist: " + activated.key());
      }
      // Projection affects the response only. A later activation may request all variables.
      jobs.activate(
          activated.key(),
          previous
              .copy()
              .setWorker(activated.record().getWorker())
              .setWorkerId(activated.record().getWorkerId())
              .setWorkerLeaseToken(activated.record().getWorkerLeaseToken())
              .setDeadline(activated.record().getDeadline())
              .setTimeout(activated.record().getTimeout()));
    }
  }
}
