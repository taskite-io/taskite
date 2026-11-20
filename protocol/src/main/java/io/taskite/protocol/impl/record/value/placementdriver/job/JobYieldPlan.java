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
package io.taskite.protocol.impl.record.value.placementdriver.job;

import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.protocol.record.value.broker.JobYieldStatus;
import java.util.List;

/** The complete exit intent captured in the Worker event; no mutable topology is read on replay. */
public record JobYieldPlan(
    long yieldId,
    String clusterId,
    String tenantId,
    String workerId,
    long workerLeaseToken,
    JobYieldReason reason,
    long createdAtEpochMillis,
    long completedAtEpochMillis,
    WorkerRegistrationRecord workerSnapshot,
    List<Partition> partitions) {
  public JobYieldPlan {
    partitions = List.copyOf(partitions);
  }

  public record Partition(
      int partitionId,
      JobYieldStatus status,
      long releasedJobCount,
      int attempts,
      long lastAttemptAtEpochMillis,
      long nextAttemptAtEpochMillis,
      long acknowledgedAtEpochMillis,
      String lastError,
      String pauseReason) {
    public static Partition pending(int id) {
      return new Partition(id, JobYieldStatus.PENDING, 0, 0, 0, 0, 0, "", "");
    }
  }
}
