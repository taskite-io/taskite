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
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.HashSet;
import java.util.stream.Collectors;

public final class WorkerRegisterProcessor
    implements TypedRecordProcessor<WorkerRegistrationRecord> {
  private static final String INVALID_REGISTRATION =
      "INVALID_WORKER_REGISTRATION: Expected to register worker '%s' in cluster '%s' for tenants '%s', but registration validation failed: %s";
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to register worker '%s' for tenants '%s', but cluster '%s' is not registered";
  private static final String SESSION_ID_REUSED =
      "WORKER_SESSION_ID_REUSED: Expected to register worker '%s' in cluster '%s' for tenants '%s', but the worker id already belongs to a session with a different tenant set or invalid lease token";
  private static final String SESSION_EXPIRED =
      "WORKER_SESSION_EXPIRED: Expected to retry registration for worker '%s' in cluster '%s' for tenant '%s', but its session is invalid or expired (status '%s', lease token '%d', expected token '%d', deadline '%d', timestamp '%d')";
  private final Writers writers;
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;
  private final Duration leaseTimeout;

  public WorkerRegisterProcessor(final TypedRecordProcessorContext context) {
    this(context, PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT);
  }

  public WorkerRegisterProcessor(
      final TypedRecordProcessorContext context, final Duration leaseTimeout) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
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
          INVALID_REGISTRATION.formatted(
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
    final var currentSession =
        state.getWorkerRegistrationState().registrations().stream()
            .filter(
                current ->
                    current.clusterId().equals(worker.getClusterId())
                        && current.workerId().equals(worker.getWorkerId()))
            .toList();
    if (!currentSession.isEmpty()) {
      final var tenants = new HashSet<>(worker.getTenantIds());
      final long retryToken = currentSession.getFirst().workerLeaseToken();
      long retryDeadline = Long.MAX_VALUE;
      if (currentSession.size() != tenants.size()
          || retryToken <= 0
          || !currentSession.stream()
              .map(current -> current.tenantId())
              .collect(Collectors.toSet())
              .equals(tenants)) {
        reject(
            record,
            RejectionType.STALE_WORKER_SESSION,
            SESSION_ID_REUSED.formatted(
                worker.getWorkerId(), worker.getClusterId(), worker.getTenantIds()));
        return;
      }
      for (final var current : currentSession) {
        if (current.status() != WorkerStatus.ONLINE
            || current.workerLeaseToken() != retryToken
            || record.getTimestamp() >= current.leaseDeadlineEpochMillis()
            || !new HashSet<>(current.tenantIds()).equals(tenants)) {
          reject(
              record,
              RejectionType.STALE_WORKER_SESSION,
              SESSION_EXPIRED.formatted(
                  worker.getWorkerId(),
                  worker.getClusterId(),
                  current.tenantId(),
                  current.status(),
                  current.workerLeaseToken(),
                  retryToken,
                  current.leaseDeadlineEpochMillis(),
                  record.getTimestamp()));
          return;
        }
        retryDeadline = Math.min(retryDeadline, current.leaseDeadlineEpochMillis());
      }
      // Response only: retries check the whole session and never extend its lease.
      writers
          .response()
          .writeEventOnCommand(
              retryToken,
              WorkerIntent.REGISTERED,
              worker.setWorkerLeaseToken(retryToken).setLeaseDeadlineEpochMillis(retryDeadline),
              record);
      return;
    }
    // A new Open uses a new Worker instance id; retries above never allocate another token.
    final long token = keyGenerator.nextKey();
    writers
        .state()
        .appendFollowUpEvent(
            token,
            WorkerIntent.REGISTERED,
            worker
                .setEventTimestampEpochMillis(record.getTimestamp())
                .setWorkerLeaseToken(token)
                .setLeaseDeadlineEpochMillis(
                    Math.addExact(record.getTimestamp(), leaseTimeout.toMillis())));
    writers.response().writeEventOnCommand(token, WorkerIntent.REGISTERED, worker, record);
  }

  private void reject(
      final TypedRecord<WorkerRegistrationRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
