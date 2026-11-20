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
package io.taskite.placementdriver.processing.job;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;

final class JobYieldProgressValidation {
  private JobYieldProgressValidation() {}

  static boolean valid(final ProcessingState state, final JobYieldRecord progress) {
    final var plan =
        state.getJobYieldState().plan(progress.getClusterId(), progress.getYieldId()).orElse(null);
    return plan != null
        && plan.clusterId().equals(progress.getClusterId())
        && plan.tenantId().equals(progress.getTenantId())
        && plan.workerId().equals(progress.getWorkerId())
        && plan.workerLeaseToken() == progress.getWorkerLeaseToken()
        && plan.reason() == progress.getReason()
        && progress.getReleasedJobCount() >= 0
        && progress.getAttempts() > 0
        && plan.partitions().stream()
            .anyMatch(partition -> partition.partitionId() == progress.getPartitionId());
  }
}
