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
package io.taskite.placementdriver.processing.registry;

import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.registry.RegistrationDeadline;
import io.taskite.placementdriver.registry.RegistrationDeadlineDates;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.worker.WorkerRegistrationRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/** Reads ordered Broker/Worker deadlines and produces fenced commands without changing state. */
public final class RegistrationDeadlineChecker implements StreamProcessorLifecycleAware {
  private static final int BATCH_SIZE = 256;

  private final MutableProcessingState state;
  private final RegistrationPolicy registrationPolicy;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public RegistrationDeadlineChecker(
      final MutableProcessingState state, final RegistrationPolicy registrationPolicy) {
    this(state, registrationPolicy, System::currentTimeMillis);
  }

  RegistrationDeadlineChecker(
      final MutableProcessingState state,
      final RegistrationPolicy registrationPolicy,
      final LongSupplier clock) {
    this.state = state;
    this.registrationPolicy = registrationPolicy;
    this.clock = clock;
    checker = new DueDateChecker(Duration.ofMillis(100), this::check, clock);
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    final List<BrokerRegistration> brokers = new ArrayList<>();
    state
        .getBrokerClusterState()
        .clusterIds()
        .forEach(
            clusterId ->
                brokers.addAll(state.getBrokerRegistrationState().registrations(clusterId)));
    state
        .getRegistrationDeadlineState()
        .rebuild(brokers, state.getWorkerRegistrationState().registrations());
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    final ValueType type = command.getValueType();
    if (type == ValueType.BROKER_REGISTRATION
        || type == ValueType.WORKER_REGISTRATION
        || type == ValueType.TOPOLOGY
        || type == ValueType.PARTITION_CHANGE
        || type == ValueType.MEMBER_CHANGE) {
      checker.schedule(clock.getAsLong());
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }

  long check(final CommandWriter commands) {
    final long now = clock.getAsLong();
    if (commands.hasRemainingCapacity()) {
      final int limit = Math.min(BATCH_SIZE, commands.remainingCapacity());
      for (final var deadline : state.getRegistrationDeadlineState().findDue(now, limit)) {
        if (!commands.hasRemainingCapacity()) {
          break;
        }
        appendCommand(deadline, commands);
      }
    }
    final long next = state.getRegistrationDeadlineState().getNextDueDate();
    // Commands have not been applied yet; keep due entries scheduled until their Appliers remove
    // them.
    return next == Long.MAX_VALUE ? -1 : next;
  }

  private boolean appendCommand(final RegistrationDeadline deadline, final CommandWriter commands) {
    return switch (deadline.type()) {
      case BROKER_LEASE -> appendBrokerOffline(deadline, commands);
      case WORKER_LEASE -> appendWorkerOffline(deadline, commands);
      case WORKER_OFFLINE_RETENTION -> appendWorkerDelete(deadline, commands);
    };
  }

  private boolean appendBrokerOffline(
      final RegistrationDeadline deadline, final CommandWriter commands) {
    final BrokerRegistration registration =
        state
            .getBrokerRegistrationState()
            .registration(new BrokerRegistryKey(deadline.clusterId(), deadline.resourceId()))
            .orElse(null);
    if (registration == null
        || registration.status() != BrokerStatus.ONLINE
        || RegistrationDeadlineDates.brokerLease(
                registration, registrationPolicy.brokerLeaseTimeout())
            != deadline.dueDate()) {
      return false;
    }
    return commands.tryAppend(
        0,
        ValueType.BROKER_REGISTRATION,
        BrokerIntent.MARK_OFFLINE,
        new BrokerRegistrationRecord()
            .setClusterId(registration.clusterId())
            .setBrokerId(registration.brokerId())
            .setExpectedLastRenewalEpochMillis(registration.lastRenewal().toEpochMilli()));
  }

  private boolean appendWorkerOffline(
      final RegistrationDeadline deadline, final CommandWriter commands) {
    final WorkerRegistration registration =
        state.getWorkerRegistrationState().registration(deadline.workerKey()).orElse(null);
    if (registration == null
        || registration.status() != WorkerStatus.ONLINE
        || RegistrationDeadlineDates.workerLease(
                registration, registrationPolicy.workerLeaseTimeout())
            != deadline.dueDate()) {
      return false;
    }
    return commands.tryAppend(
        0,
        ValueType.WORKER_REGISTRATION,
        WorkerIntent.MARK_OFFLINE,
        workerCommand(registration)
            .setExpectedLastRenewalEpochMillis(registration.lastRenewal().toEpochMilli()));
  }

  private boolean appendWorkerDelete(
      final RegistrationDeadline deadline, final CommandWriter commands) {
    final WorkerRegistration registration =
        state.getWorkerRegistrationState().registration(deadline.workerKey()).orElse(null);
    if (registration == null
        || registration.status() != WorkerStatus.OFFLINE
        || RegistrationDeadlineDates.workerOfflineRetention(
                registration, registrationPolicy.workerOfflineRetention())
            != deadline.dueDate()) {
      return false;
    }
    return commands.tryAppend(
        0,
        ValueType.WORKER_REGISTRATION,
        WorkerIntent.DELETE,
        workerCommand(registration)
            .setExpectedStatusChangedAtEpochMillis(registration.statusChangedAt().toEpochMilli()));
  }

  private static WorkerRegistrationRecord workerCommand(final WorkerRegistration registration) {
    return new WorkerRegistrationRecord()
        .setClusterId(registration.clusterId())
        .setTenantIds(List.of(registration.tenantId()))
        .setName(registration.name())
        .setWorkerId(registration.workerId())
        .setWorkerLeaseToken(registration.workerLeaseToken())
        .setExpectedWorkerLeaseToken(registration.workerLeaseToken());
  }
}
