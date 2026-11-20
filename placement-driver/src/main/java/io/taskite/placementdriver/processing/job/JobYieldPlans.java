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
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.streamplatform.api.state.KeyGenerator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public final class JobYieldPlans {
  private JobYieldPlans() {}

  public static List<JobYieldPlan> capture(
      final ProcessingState state,
      final KeyGenerator keys,
      final WorkerRegistrationRecord command,
      final JobYieldReason reason,
      final long timestamp) {
    final List<JobYieldPlan> plans = new ArrayList<>();
    for (final WorkerRegistration current : state.getWorkerRegistrationState().registrations()) {
      if (!current.clusterId().equals(command.getClusterId())
          || !current.workerId().equals(command.getWorkerId())
          || current.status() != WorkerStatus.ONLINE
          || !command.getTenantIds().contains(current.tenantId())
          || current.workerLeaseToken() <= 0) {
        continue;
      }
      final WorkerRegistrationRecord snapshot =
          new WorkerRegistrationRecord()
              .setClusterId(current.clusterId())
              .setTenantIds(List.of(current.tenantId()))
              .setWorkerId(current.workerId())
              .setName(current.name())
              .setJobType(current.jobType())
              .setMaxJobsActive(current.maxJobsActive())
              .setActiveJobs(current.activeJobs())
              .setConcurrency(current.concurrency())
              .setTimeoutMillis(current.timeoutMillis())
              .setRequestTimeoutMillis(current.requestTimeoutMillis())
              .setPollIntervalMillis(current.pollIntervalMillis())
              .setFetchVariables(current.fetchVariables())
              .setClientVersion(current.clientVersion())
              .setRegisteredAtEpochMillis(current.registeredAt().toEpochMilli())
              .setExpectedLastRenewalEpochMillis(current.lastRenewal().toEpochMilli())
              .setWorkerLeaseToken(current.workerLeaseToken())
              .setLeaseDeadlineEpochMillis(current.leaseDeadlineEpochMillis());
      final var config =
          state.getBrokerClusterConfigState().findClusterConfig(current.clusterId()).orElseThrow();
      final Set<Integer> partitions = new TreeSet<>();
      for (int id = 1; id <= config.partitionSize(); id++) {
        partitions.add(id);
      }
      state
          .getBrokerPartitionTopologyState()
          .partitionTopologies(current.clusterId())
          .forEach(partition -> partitions.add(partition.partitionId()));
      plans.add(
          new JobYieldPlan(
              keys.nextKey(),
              current.clusterId(),
              current.tenantId(),
              current.workerId(),
              current.workerLeaseToken(),
              reason,
              timestamp,
              0,
              snapshot,
              partitions.stream().map(JobYieldPlan.Partition::pending).toList()));
    }
    return plans;
  }
}
