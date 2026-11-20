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
package io.taskite.placementdriver;

import java.time.Duration;
import java.util.Objects;

/** Authoritative lease and retention policy applied by the placement driver. */
public record RegistrationPolicy(
    Duration brokerLeaseTimeout, Duration workerLeaseTimeout, Duration workerOfflineRetention) {

  public RegistrationPolicy {
    requirePositive(brokerLeaseTimeout, "brokerLeaseTimeout");
    requirePositive(workerLeaseTimeout, "workerLeaseTimeout");
    Objects.requireNonNull(workerOfflineRetention, "workerOfflineRetention");
    if (workerOfflineRetention.isNegative()) {
      throw new IllegalArgumentException("workerOfflineRetention must not be negative");
    }
  }

  private static void requirePositive(final Duration value, final String name) {
    Objects.requireNonNull(value, name);
    if (value.isNegative() || value.toMillis() < 1) {
      throw new IllegalArgumentException(name + " must be at least 1ms");
    }
  }
}
