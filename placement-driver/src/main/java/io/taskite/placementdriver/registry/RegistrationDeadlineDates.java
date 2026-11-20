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

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import java.time.Duration;
import java.util.Objects;

public final class RegistrationDeadlineDates {
  private RegistrationDeadlineDates() {}

  public static long brokerLease(
      final BrokerRegistration registration, final Duration leaseTimeout) {
    return add(registration.lastRenewal().toEpochMilli(), leaseTimeout);
  }

  public static long workerLease(
      final WorkerRegistration registration, final Duration leaseTimeout) {
    return add(registration.lastRenewal().toEpochMilli(), leaseTimeout);
  }

  public static long workerOfflineRetention(
      final WorkerRegistration registration, final Duration offlineRetention) {
    return add(registration.statusChangedAt().toEpochMilli(), offlineRetention);
  }

  private static long add(final long timestamp, final Duration duration) {
    Objects.requireNonNull(duration, "duration");
    if (timestamp < 0 || duration.isNegative()) {
      throw new IllegalArgumentException("Deadline timestamp and duration must not be negative");
    }
    final long durationMillis = duration.toMillis();
    return timestamp > Long.MAX_VALUE - durationMillis
        ? Long.MAX_VALUE
        : timestamp + durationMillis;
  }
}
