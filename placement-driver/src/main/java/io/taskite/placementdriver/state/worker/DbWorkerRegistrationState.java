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
package io.taskite.placementdriver.state.worker;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableRegistrationDeadlineState;
import io.taskite.placementdriver.state.mutable.MutableWorkerRegistrationState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DbWorkerRegistrationState implements MutableWorkerRegistrationState {
  private static final Comparator<WorkerRegistration> REGISTRATION_ORDER =
      Comparator.comparing(WorkerRegistration::clusterId)
          .thenComparing(WorkerRegistration::tenantId)
          .thenComparing(WorkerRegistration::name)
          .thenComparing(WorkerRegistration::workerId);

  // [clusterId, tenantId, workerId] joined with U+001F by WorkerRegistryKey.dbKey().
  private final DbString workerKey;
  private final WorkerRegistrationRecordValue workerValue;
  // PD_WORKER_REGISTRATIONS: [clusterId U+001F tenantId U+001F workerId]
  // -> WorkerRegistrationCodec bytes, including lifecycle status and lease fence.
  private final ColumnFamily<DbString, WorkerRegistrationRecordValue> registrationsColumnFamilies;
  private final MutableRegistrationDeadlineState deadlines;

  public DbWorkerRegistrationState(
      final ProcessingStateStore storage, final MutableRegistrationDeadlineState deadlines) {
    workerKey = new DbString();
    workerValue = new WorkerRegistrationRecordValue();

    this.deadlines = deadlines;
    registrationsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_WORKER_REGISTRATIONS, workerKey, workerValue);
  }

  @Override
  public List<WorkerRegistration> registrations() {
    final List<WorkerRegistration> result = new ArrayList<>();
    registrationsColumnFamilies.forEach(value -> result.add(value.getRecord()));
    result.sort(REGISTRATION_ORDER);
    return result;
  }

  @Override
  public Optional<WorkerRegistration> registration(final WorkerRegistryKey key) {
    workerKey.wrapString(key.dbKey());
    final var stored =
        registrationsColumnFamilies.get(workerKey, WorkerRegistrationRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public void registerWorker(
      final WorkerRegistrationRecord registration, final String tenantId, final Instant now) {
    upsertWorker(registration, tenantId, now, true);
  }

  @Override
  public void renewWorker(
      final WorkerRegistrationRecord renewal, final String tenantId, final Instant now) {
    upsertWorker(renewal, tenantId, now, false);
  }

  private void upsertWorker(
      final WorkerRegistrationRecord renewal,
      final String tenantId,
      final Instant now,
      final boolean registering) {
    final WorkerRegistryKey key =
        new WorkerRegistryKey(renewal.getClusterId(), tenantId, renewal.getWorkerId());
    final WorkerRegistration current = registration(key).orElse(null);
    final Instant registeredAt = current == null || registering ? now : current.registeredAt();
    final Instant statusChangedAt =
        registering || current == null || current.status() != WorkerStatus.ONLINE
            ? now
            : current.statusChangedAt();
    put(
        new WorkerRegistration(
            renewal.getClusterId(),
            tenantId,
            renewal.getWorkerId(),
            renewal.getName(),
            renewal.getJobType(),
            renewal.getMaxJobsActive(),
            renewal.getActiveJobs(),
            renewal.getConcurrency(),
            renewal.getTimeoutMillis(),
            renewal.getRequestTimeoutMillis(),
            renewal.getPollIntervalMillis(),
            renewal.getFetchVariables(),
            renewal.getClientVersion(),
            registeredAt,
            now,
            statusChangedAt,
            renewal.getRenewalTimeoutMillis(),
            WorkerStatus.ONLINE,
            renewal.getWorkerLeaseToken(),
            renewal.getLeaseDeadlineEpochMillis(),
            renewal.getTenantIds()));
  }

  @Override
  public void markWorkerOffline(final WorkerRegistryKey key, final Instant now) {
    registration(key).map(registration -> registration.markOffline(now)).ifPresent(this::put);
  }

  @Override
  public void removeWorker(final WorkerRegistryKey key) {
    final WorkerRegistration existing = registration(key).orElse(null);
    if (existing == null) {
      return;
    }
    workerKey.wrapString(key.dbKey());
    registrationsColumnFamilies.deleteIfExists(workerKey);
    deadlines.removeWorker(existing);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    registrations().stream()
        .filter(registration -> clusterId.equals(registration.clusterId()))
        .map(WorkerRegistration::key)
        .toList()
        .forEach(this::removeWorker);
  }

  private void put(final WorkerRegistration registration) {
    final WorkerRegistration previous = registration(registration.key()).orElse(null);
    workerKey.wrapString(registration.key().dbKey());
    workerValue.setRecord(registration);
    registrationsColumnFamilies.upsert(workerKey, workerValue);
    deadlines.updateWorker(previous, registration);
  }
}
