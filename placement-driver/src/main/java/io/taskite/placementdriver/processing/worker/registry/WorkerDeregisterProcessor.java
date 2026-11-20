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
import java.util.HashSet;

public final class WorkerDeregisterProcessor
    implements TypedRecordProcessor<WorkerRegistrationRecord> {
  private static final String INVALID_IDENTITY =
      "INVALID_WORKER_IDENTITY: Expected to deregister worker '%s' in cluster '%s' for tenants '%s', but identity validation failed: %s";
  private static final String SESSION_EXPIRED =
      "WORKER_SESSION_EXPIRED: Expected to deregister worker '%s' in cluster '%s' for tenant '%s' with lease token '%d' and tenants '%s', but its session is missing, offline, or has a different lease token or tenant set (current status '%s', token '%s', tenants '%s')";
  private final Writers writers;
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;

  public WorkerDeregisterProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
  }

  @Override
  public void processRecord(final TypedRecord<WorkerRegistrationRecord> record) {
    final WorkerRegistrationRecord worker = record.getValue();
    final String validationError = WorkerRegistryValidation.validateIdentity(worker);
    if (validationError != null) {
      final String reason =
          INVALID_IDENTITY.formatted(
              worker.getWorkerId(), worker.getClusterId(), worker.getTenantIds(), validationError);
      writers.rejection().appendRejection(RejectionType.INVALID_ARGUMENT, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_ARGUMENT, reason);
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
          || !new HashSet<>(current.tenantIds()).equals(new HashSet<>(worker.getTenantIds()))) {
        final String reason =
            SESSION_EXPIRED.formatted(
                worker.getWorkerId(),
                worker.getClusterId(),
                tenant,
                worker.getWorkerLeaseToken(),
                worker.getTenantIds(),
                current == null ? null : current.status(),
                current == null ? null : current.workerLeaseToken(),
                current == null ? null : current.tenantIds());
        writers.rejection().appendRejection(RejectionType.STALE_WORKER_SESSION, reason);
        writers
            .response()
            .writeRejectionOnCommand(record, RejectionType.STALE_WORKER_SESSION, reason);
        return;
      }
    }
    worker.setYieldPlans(
        JobYieldPlans.capture(
            state,
            keyGenerator,
            worker,
            JobYieldReason.WORKER_DEREGISTERED,
            record.getTimestamp()));
    final long eventKey =
        worker.getYieldPlans().stream()
            .mapToLong(JobYieldPlan::yieldId)
            .max()
            .orElse(record.getKey());
    writers
        .state()
        .appendFollowUpEvent(
            eventKey,
            WorkerIntent.DEREGISTERED,
            worker.setEventTimestampEpochMillis(record.getTimestamp()));
    writers.response().writeEventOnCommand(eventKey, WorkerIntent.DEREGISTERED, worker, record);
  }
}
