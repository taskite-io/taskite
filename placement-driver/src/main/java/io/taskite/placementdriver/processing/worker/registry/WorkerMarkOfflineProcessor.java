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
package io.taskite.placementdriver.processing.worker.registry;

import io.taskite.placementdriver.processing.job.JobYieldPlans;
import io.taskite.placementdriver.registry.RegistrationDeadlineDates;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.protocol.record.value.broker.JobYieldReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.Objects;

public final class WorkerMarkOfflineProcessor
    implements TypedRecordProcessor<WorkerRegistrationRecord> {
  private static final String INVALID_LEASE_TIMEOUT =
      "Expected worker lease timeout to be positive, but was '%s'";
  private static final String MISSING_LEASE_TIMEOUT =
      "Expected to construct the worker offline processor, but leaseTimeout is null";
  private static final String INVALID_IDENTITY =
      "INVALID_WORKER_IDENTITY: Expected to mark worker '%s' offline in cluster '%s' for tenants '%s', but identity validation failed: %s";
  private static final String SESSION_CHANGED =
      "Expected to mark worker '%s' offline in cluster '%s' for tenant '%s' with lease token '%d' and last renewal '%d', but its lease was renewed or it is no longer online (current status '%s', token '%s', last renewal '%s')";
  private static final String LEASE_NOT_EXPIRED =
      "Expected to mark worker '%s' offline in cluster '%s' for tenant '%s', but its lease has not expired (deadline '%d', timestamp '%d')";
  private final Writers writers;
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;
  private final Duration leaseTimeout;

  public WorkerMarkOfflineProcessor(
      final TypedRecordProcessorContext context, final Duration leaseTimeout) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
    Objects.requireNonNull(leaseTimeout, MISSING_LEASE_TIMEOUT);
    if (leaseTimeout.isZero() || leaseTimeout.isNegative()) {
      throw new IllegalArgumentException(INVALID_LEASE_TIMEOUT.formatted(leaseTimeout));
    }
    this.leaseTimeout = leaseTimeout;
  }

  @Override
  public void processRecord(final TypedRecord<WorkerRegistrationRecord> record) {
    final WorkerRegistrationRecord worker = record.getValue();
    final String validationError = WorkerRegistryValidation.validateIdentity(worker);
    if (validationError != null) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_IDENTITY.formatted(
                  worker.getWorkerId(),
                  worker.getClusterId(),
                  worker.getTenantIds(),
                  validationError));
      return;
    }
    final long expectedLastRenewal = worker.getExpectedLastRenewalEpochMillis();
    for (final String tenantId : worker.getTenantIds()) {
      final var current =
          state
              .getWorkerRegistrationState()
              .registration(
                  new WorkerRegistryKey(worker.getClusterId(), tenantId, worker.getWorkerId()))
              .orElse(null);
      if (current == null
          || current.status() != WorkerStatus.ONLINE
          || current.workerLeaseToken() != worker.getExpectedWorkerLeaseToken()
          || current.lastRenewal().toEpochMilli() != expectedLastRenewal) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                SESSION_CHANGED.formatted(
                    worker.getWorkerId(),
                    worker.getClusterId(),
                    tenantId,
                    worker.getExpectedWorkerLeaseToken(),
                    expectedLastRenewal,
                    current == null ? null : current.status(),
                    current == null ? null : current.workerLeaseToken(),
                    current == null ? null : current.lastRenewal()));
        return;
      }
      if (record.getTimestamp() < RegistrationDeadlineDates.workerLease(current, leaseTimeout)) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                LEASE_NOT_EXPIRED.formatted(
                    worker.getWorkerId(),
                    worker.getClusterId(),
                    tenantId,
                    RegistrationDeadlineDates.workerLease(current, leaseTimeout),
                    record.getTimestamp()));
        return;
      }
    }
    worker.setYieldPlans(
        JobYieldPlans.capture(
            state,
            keyGenerator,
            worker,
            JobYieldReason.WORKER_LEASE_EXPIRED,
            record.getTimestamp()));
    writers
        .state()
        .appendFollowUpEvent(
            worker.getYieldPlans().stream()
                .mapToLong(JobYieldPlan::yieldId)
                .max()
                .orElse(record.getKey()),
            WorkerIntent.MARKED_OFFLINE,
            worker.setEventTimestampEpochMillis(record.getTimestamp()));
  }
}
