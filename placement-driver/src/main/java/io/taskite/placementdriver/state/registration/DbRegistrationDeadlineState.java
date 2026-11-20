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
package io.taskite.placementdriver.state.registration;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbNil;
import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.registry.RegistrationDeadline;
import io.taskite.placementdriver.registry.RegistrationDeadlineDates;
import io.taskite.placementdriver.registry.RegistrationDeadlineType;
import io.taskite.placementdriver.state.mutable.MutableRegistrationDeadlineState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;

public final class DbRegistrationDeadlineState implements MutableRegistrationDeadlineState {
  private final RegistrationPolicy registrationPolicy;
  // [dueDate, registrationType, clusterId, tenantId, resourceId].
  // resourceId is brokerId/workerId; Broker entries use an empty tenantId.
  private final RegistrationDeadlineKey deadlineKey;
  // PD_REGISTRATION_DEADLINES: [dueDate, registrationType, clusterId, tenantId, resourceId] -> nil.
  // Orders Broker leases, Worker leases and offline Worker retention by deadline.
  private final ColumnFamily<RegistrationDeadlineKey, DbNil> deadlinesColumnFamilies;

  public DbRegistrationDeadlineState(
      final ProcessingStateStore storage, final RegistrationPolicy registrationPolicy) {
    deadlineKey = new RegistrationDeadlineKey();

    this.registrationPolicy = registrationPolicy;
    deadlinesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_REGISTRATION_DEADLINES, deadlineKey, DbNil.INSTANCE);
  }

  @Override
  public void updateBroker(final BrokerRegistration previous, final BrokerRegistration current) {
    if (previous != null) {
      removeBroker(previous);
    }
    if (current != null && current.status() == BrokerStatus.ONLINE) {
      upsert(
          RegistrationDeadline.broker(
              RegistrationDeadlineDates.brokerLease(
                  current, registrationPolicy.brokerLeaseTimeout()),
              new BrokerRegistryKey(current.clusterId(), current.brokerId())));
    }
  }

  @Override
  public void removeBroker(final BrokerRegistration registration) {
    if (registration.status() == BrokerStatus.ONLINE) {
      delete(
          RegistrationDeadline.broker(
              RegistrationDeadlineDates.brokerLease(
                  registration, registrationPolicy.brokerLeaseTimeout()),
              new BrokerRegistryKey(registration.clusterId(), registration.brokerId())));
    }
  }

  @Override
  public void updateWorker(final WorkerRegistration previous, final WorkerRegistration current) {
    if (previous != null) {
      removeWorker(previous);
    }
    if (current == null) {
      return;
    }
    final RegistrationDeadlineType type =
        current.status() == WorkerStatus.ONLINE
            ? RegistrationDeadlineType.WORKER_LEASE
            : RegistrationDeadlineType.WORKER_OFFLINE_RETENTION;
    final long dueDate =
        type == RegistrationDeadlineType.WORKER_LEASE
            ? RegistrationDeadlineDates.workerLease(
                current, registrationPolicy.workerLeaseTimeout())
            : RegistrationDeadlineDates.workerOfflineRetention(
                current, registrationPolicy.workerOfflineRetention());
    upsert(RegistrationDeadline.worker(dueDate, type, current.key()));
  }

  @Override
  public void removeWorker(final WorkerRegistration registration) {
    final RegistrationDeadlineType type =
        registration.status() == WorkerStatus.ONLINE
            ? RegistrationDeadlineType.WORKER_LEASE
            : RegistrationDeadlineType.WORKER_OFFLINE_RETENTION;
    final long dueDate =
        type == RegistrationDeadlineType.WORKER_LEASE
            ? RegistrationDeadlineDates.workerLease(
                registration, registrationPolicy.workerLeaseTimeout())
            : RegistrationDeadlineDates.workerOfflineRetention(
                registration, registrationPolicy.workerOfflineRetention());
    delete(RegistrationDeadline.worker(dueDate, type, registration.key()));
  }

  @Override
  public void rebuild(
      final List<BrokerRegistration> brokerRegistrations,
      final List<WorkerRegistration> workerRegistrations) {
    final List<RegistrationDeadline> existing = new ArrayList<>();
    deadlinesColumnFamilies.forEach((key, ignored) -> existing.add(key.deadline()));
    existing.forEach(this::delete);
    brokerRegistrations.forEach(registration -> updateBroker(null, registration));
    workerRegistrations.forEach(registration -> updateWorker(null, registration));
  }

  @Override
  public List<RegistrationDeadline> findDue(final long timestamp, final int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("Deadline limit must be positive");
    }
    final List<RegistrationDeadline> result = new ArrayList<>();
    deadlinesColumnFamilies.whileTrue(
        (key, ignored) -> {
          if (key.dueDate() > timestamp || result.size() >= limit) {
            return false;
          }
          result.add(key.deadline());
          return result.size() < limit;
        });
    return result;
  }

  @Override
  public long getNextDueDate() {
    final long[] next = {Long.MAX_VALUE};
    deadlinesColumnFamilies.whileTrue(
        (key, ignored) -> {
          next[0] = key.dueDate();
          return false;
        });
    return next[0];
  }

  private void upsert(final RegistrationDeadline deadline) {
    deadlinesColumnFamilies.upsert(deadlineKey.wrapDeadline(deadline), DbNil.INSTANCE);
  }

  private void delete(final RegistrationDeadline deadline) {
    deadlinesColumnFamilies.deleteIfExists(deadlineKey.wrapDeadline(deadline));
  }
}
