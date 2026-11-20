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
package io.taskite.placementdriver.worker.registry;

import java.time.Instant;
import java.util.List;

public record WorkerRegistration(
    String clusterId,
    String tenantId,
    String workerId,
    String name,
    String jobType,
    int maxJobsActive,
    int activeJobs,
    int concurrency,
    long timeoutMillis,
    long requestTimeoutMillis,
    long pollIntervalMillis,
    List<String> fetchVariables,
    String clientVersion,
    Instant registeredAt,
    Instant lastRenewal,
    Instant statusChangedAt,
    long renewalTimeoutMillis,
    WorkerStatus status,
    long workerLeaseToken,
    long leaseDeadlineEpochMillis,
    List<String> tenantIds) {

  public WorkerRegistration {
    fetchVariables = List.copyOf(fetchVariables == null ? List.of() : fetchVariables);
    tenantIds = List.copyOf(tenantIds == null ? List.of(tenantId) : tenantIds);
    if (renewalTimeoutMillis <= 0) {
      throw new IllegalArgumentException("renewalTimeoutMillis must be positive");
    }
  }

  public WorkerRegistration(
      String clusterId,
      String tenantId,
      String workerId,
      String name,
      String jobType,
      int maxJobsActive,
      int activeJobs,
      int concurrency,
      long timeoutMillis,
      long requestTimeoutMillis,
      long pollIntervalMillis,
      List<String> fetchVariables,
      String clientVersion,
      Instant registeredAt,
      Instant lastRenewal,
      Instant statusChangedAt,
      long renewalTimeoutMillis,
      WorkerStatus status) {
    this(
        clusterId,
        tenantId,
        workerId,
        name,
        jobType,
        maxJobsActive,
        activeJobs,
        concurrency,
        timeoutMillis,
        requestTimeoutMillis,
        pollIntervalMillis,
        fetchVariables,
        clientVersion,
        registeredAt,
        lastRenewal,
        statusChangedAt,
        renewalTimeoutMillis,
        status,
        0,
        0,
        List.of(tenantId));
  }

  public WorkerRegistryKey key() {
    return new WorkerRegistryKey(clusterId, tenantId, workerId);
  }

  public WorkerRegistration markOffline(final Instant changedAt) {
    return new WorkerRegistration(
        clusterId,
        tenantId,
        workerId,
        name,
        jobType,
        maxJobsActive,
        activeJobs,
        concurrency,
        timeoutMillis,
        requestTimeoutMillis,
        pollIntervalMillis,
        fetchVariables,
        clientVersion,
        registeredAt,
        lastRenewal,
        changedAt,
        renewalTimeoutMillis,
        WorkerStatus.OFFLINE,
        workerLeaseToken,
        leaseDeadlineEpochMillis,
        tenantIds);
  }
}
