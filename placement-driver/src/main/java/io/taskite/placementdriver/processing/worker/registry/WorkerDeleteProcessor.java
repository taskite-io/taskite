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

import io.taskite.placementdriver.registry.RegistrationDeadlineDates;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.Objects;

public final class WorkerDeleteProcessor implements TypedRecordProcessor<WorkerRegistrationRecord> {
  private static final String INVALID_RETENTION =
      "Expected worker offline retention to be nonnegative, but was '%s'";
  private static final String MISSING_RETENTION =
      "Expected to construct the worker deletion processor, but offlineRetention is null";
  private static final String INVALID_IDENTITY =
      "INVALID_WORKER_IDENTITY: Expected to delete worker '%s' in cluster '%s' for tenants '%s', but identity validation failed: %s";
  private static final String SESSION_CHANGED =
      "Expected to delete worker '%s' in cluster '%s' for tenant '%s' with lease token '%d' and offline status timestamp '%d', but its session is missing, renewed, or has a different offline state (current status '%s', token '%s', status changed at '%s')";
  private static final String RETENTION_NOT_EXPIRED =
      "Expected to delete worker '%s' in cluster '%s' for tenant '%s', but its offline retention has not expired (deadline '%d', timestamp '%d')";
  private final Writers writers;
  private final ProcessingState state;
  private final Duration offlineRetention;

  public WorkerDeleteProcessor(
      final TypedRecordProcessorContext context, final Duration offlineRetention) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    Objects.requireNonNull(offlineRetention, MISSING_RETENTION);
    if (offlineRetention.isNegative()) {
      throw new IllegalArgumentException(INVALID_RETENTION.formatted(offlineRetention));
    }
    this.offlineRetention = offlineRetention;
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
    final long expectedStatusChangedAt = worker.getExpectedStatusChangedAtEpochMillis();
    for (final String tenantId : worker.getTenantIds()) {
      final var current =
          state
              .getWorkerRegistrationState()
              .registration(
                  new WorkerRegistryKey(worker.getClusterId(), tenantId, worker.getWorkerId()))
              .orElse(null);
      if (current == null
          || current.status() != WorkerStatus.OFFLINE
          || current.workerLeaseToken() != worker.getExpectedWorkerLeaseToken()
          || current.statusChangedAt().toEpochMilli() != expectedStatusChangedAt) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                SESSION_CHANGED.formatted(
                    worker.getWorkerId(),
                    worker.getClusterId(),
                    tenantId,
                    worker.getExpectedWorkerLeaseToken(),
                    expectedStatusChangedAt,
                    current == null ? null : current.status(),
                    current == null ? null : current.workerLeaseToken(),
                    current == null ? null : current.statusChangedAt()));
        return;
      }
      if (record.getTimestamp()
          < RegistrationDeadlineDates.workerOfflineRetention(current, offlineRetention)) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                RETENTION_NOT_EXPIRED.formatted(
                    worker.getWorkerId(),
                    worker.getClusterId(),
                    tenantId,
                    RegistrationDeadlineDates.workerOfflineRetention(current, offlineRetention),
                    record.getTimestamp()));
        return;
      }
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            WorkerIntent.DELETED,
            worker.setEventTimestampEpochMillis(record.getTimestamp()));
  }
}
