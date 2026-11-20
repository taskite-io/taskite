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

import io.taskite.placementdriver.PlacementDriverConfiguration;
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
import java.util.HashSet;

public final class WorkerRenewProcessor implements TypedRecordProcessor<WorkerRegistrationRecord> {
  private static final String INVALID_RENEWAL =
      "INVALID_WORKER_RENEWAL: Expected to renew worker '%s' in cluster '%s' for tenants '%s', but registration validation failed: %s";
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to renew worker '%s' for tenants '%s', but cluster '%s' is not registered";
  private static final String SESSION_EXPIRED =
      "WORKER_SESSION_EXPIRED: Expected to renew worker '%s' in cluster '%s' for tenant '%s' with lease token '%d' and tenants '%s', but its session is missing, offline, expired, or has a different lease token or tenant set (current status '%s', token '%s', tenants '%s', deadline '%s', timestamp '%d')";
  private final Writers writers;
  private final ProcessingState state;
  private final Duration leaseTimeout;

  public WorkerRenewProcessor(final TypedRecordProcessorContext context) {
    this(context, PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT);
  }

  public WorkerRenewProcessor(
      final TypedRecordProcessorContext context, final Duration leaseTimeout) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    this.leaseTimeout = leaseTimeout;
  }

  @Override
  public void processRecord(final TypedRecord<WorkerRegistrationRecord> record) {
    final WorkerRegistrationRecord worker = record.getValue();
    final String validationError = WorkerRegistryValidation.validateRegistration(worker);
    if (validationError != null) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_RENEWAL.formatted(
              worker.getWorkerId(), worker.getClusterId(), worker.getTenantIds(), validationError));
      return;
    }
    if (state.getBrokerClusterConfigState().findClusterConfig(worker.getClusterId()).isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          CLUSTER_NOT_FOUND.formatted(
              worker.getWorkerId(), worker.getTenantIds(), worker.getClusterId()));
      return;
    }
    for (final String tenant : worker.getTenantIds()) {
      final var current =
          state
              .getWorkerRegistrationState()
              .registration(
                  new WorkerRegistryKey(worker.getClusterId(), tenant, worker.getWorkerId()))
              .orElse(null);
      if (current == null
          || current.status() != WorkerStatus.ONLINE
          || worker.getWorkerLeaseToken() <= 0
          || current.workerLeaseToken() != worker.getWorkerLeaseToken()
          || record.getTimestamp() >= current.leaseDeadlineEpochMillis()
          || !new HashSet<>(current.tenantIds()).equals(new HashSet<>(worker.getTenantIds()))) {
        reject(
            record,
            RejectionType.STALE_WORKER_SESSION,
            SESSION_EXPIRED.formatted(
                worker.getWorkerId(),
                worker.getClusterId(),
                tenant,
                worker.getWorkerLeaseToken(),
                worker.getTenantIds(),
                current == null ? null : current.status(),
                current == null ? null : current.workerLeaseToken(),
                current == null ? null : current.tenantIds(),
                current == null ? null : current.leaseDeadlineEpochMillis(),
                record.getTimestamp()));
        return;
      }
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            WorkerIntent.RENEWED,
            worker
                .setEventTimestampEpochMillis(record.getTimestamp())
                .setLeaseDeadlineEpochMillis(
                    Math.addExact(record.getTimestamp(), leaseTimeout.toMillis())));
    writers.response().writeEventOnCommand(record.getKey(), WorkerIntent.RENEWED, worker, record);
  }

  private void reject(
      final TypedRecord<WorkerRegistrationRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
