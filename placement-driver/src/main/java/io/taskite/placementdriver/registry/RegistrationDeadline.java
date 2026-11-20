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
package io.taskite.placementdriver.registry;

import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.worker.registry.WorkerRegistryKey;
import java.util.Objects;

public record RegistrationDeadline(
    long dueDate,
    RegistrationDeadlineType type,
    String clusterId,
    String tenantId,
    String resourceId) {

  public RegistrationDeadline {
    if (dueDate < 0) {
      throw new IllegalArgumentException("dueDate must not be negative");
    }
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(clusterId, "clusterId");
    Objects.requireNonNull(tenantId, "tenantId");
    Objects.requireNonNull(resourceId, "resourceId");
  }

  public static RegistrationDeadline broker(final long dueDate, final BrokerRegistryKey brokerKey) {
    return new RegistrationDeadline(
        dueDate,
        RegistrationDeadlineType.BROKER_LEASE,
        brokerKey.clusterId(),
        "",
        brokerKey.brokerId());
  }

  public static RegistrationDeadline worker(
      final long dueDate, final RegistrationDeadlineType type, final WorkerRegistryKey workerKey) {
    if (type == RegistrationDeadlineType.BROKER_LEASE) {
      throw new IllegalArgumentException("Worker deadline cannot use BROKER_LEASE");
    }
    return new RegistrationDeadline(
        dueDate, type, workerKey.clusterId(), workerKey.tenantId(), workerKey.workerId());
  }

  public BrokerRegistryKey brokerKey() {
    if (type != RegistrationDeadlineType.BROKER_LEASE) {
      throw new IllegalStateException("Deadline does not identify a Broker");
    }
    return new BrokerRegistryKey(clusterId, resourceId);
  }

  public WorkerRegistryKey workerKey() {
    if (type == RegistrationDeadlineType.BROKER_LEASE) {
      throw new IllegalStateException("Deadline does not identify a Worker");
    }
    return new WorkerRegistryKey(clusterId, tenantId, resourceId);
  }
}
