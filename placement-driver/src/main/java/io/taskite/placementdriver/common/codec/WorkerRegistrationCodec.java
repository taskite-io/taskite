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
package io.taskite.placementdriver.common.codec;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import java.time.Instant;
import java.util.List;

/** Versioned persisted representation of one worker registration. */
public final class WorkerRegistrationCodec {
  private static final int VERSION = 2;

  private WorkerRegistrationCodec() {}

  public static byte[] toBytes(final WorkerRegistration registration) {
    return MsgPackConverter.convertToMsgPack(
        new PersistedWorkerRegistration(
            VERSION,
            registration.clusterId(),
            registration.tenantId(),
            registration.workerId(),
            registration.name(),
            registration.jobType(),
            registration.maxJobsActive(),
            registration.activeJobs(),
            registration.concurrency(),
            registration.timeoutMillis(),
            registration.requestTimeoutMillis(),
            registration.pollIntervalMillis(),
            registration.fetchVariables(),
            registration.clientVersion(),
            registration.registeredAt().toEpochMilli(),
            registration.lastRenewal().toEpochMilli(),
            registration.statusChangedAt().toEpochMilli(),
            registration.renewalTimeoutMillis(),
            registration.status(),
            registration.workerLeaseToken(),
            registration.leaseDeadlineEpochMillis(),
            registration.tenantIds()));
  }

  public static WorkerRegistration fromBytes(final byte[] encoded) {
    final PersistedWorkerRegistration registration =
        MsgPackConverter.convertToObject(PersistedWorkerRegistration.class, encoded);
    if (registration.version() != 1 && registration.version() != VERSION) {
      throw new IllegalArgumentException(
          "unsupported worker registration version: " + registration.version());
    }
    return new WorkerRegistration(
        registration.clusterId(),
        registration.tenantId(),
        registration.workerId(),
        registration.name(),
        registration.jobType(),
        registration.maxJobsActive(),
        registration.activeJobs(),
        registration.concurrency(),
        registration.timeoutMillis(),
        registration.requestTimeoutMillis(),
        registration.pollIntervalMillis(),
        registration.fetchVariables(),
        registration.clientVersion(),
        Instant.ofEpochMilli(registration.registeredAtEpochMillis()),
        Instant.ofEpochMilli(registration.lastRenewalEpochMillis()),
        Instant.ofEpochMilli(registration.statusChangedAtEpochMillis()),
        registration.renewalTimeoutMillis(),
        registration.status(),
        registration.workerLeaseToken(),
        registration.leaseDeadlineEpochMillis(),
        registration.tenantIds());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record PersistedWorkerRegistration(
      int version,
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
      long registeredAtEpochMillis,
      @JsonAlias("lastHeartbeatEpochMillis") long lastRenewalEpochMillis,
      long statusChangedAtEpochMillis,
      long renewalTimeoutMillis,
      WorkerStatus status,
      long workerLeaseToken,
      long leaseDeadlineEpochMillis,
      List<String> tenantIds) {}
}
