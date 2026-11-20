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
package io.taskite.protocol.record.value.placementdriver.topology;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/** Retryable plan barriers. NONE retains the historical empty wire value. */
public enum PlanBlockedReason {
  NONE,
  CONFIGURATION_CONFLICT,
  TARGET_UNAVAILABLE,
  PLAN_DEADLINE_EXCEEDED,
  JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT,
  NO_QUORUM,
  INSUFFICIENT_CAPACITY,
  LEADER_UNAVAILABLE,
  CONFIGURATION_READ_UNAVAILABLE,
  AUTHORITY_INSTALL_UNAVAILABLE,
  EXECUTION_AUTHORITY_CONFLICT,
  INCOMPLETE_SOURCE_CONFIGURATION,
  INVALID_SUCCESSOR_AUTHORITY,
  BROKER_UNAVAILABLE;

  @JsonCreator
  public static PlanBlockedReason parse(final String value) {
    return value == null || value.isBlank() ? NONE : valueOf(value);
  }

  /** Adapts external exception messages; unrecognized failures retain the retry fallback. */
  public static PlanBlockedReason fromFailure(final String message) {
    try {
      return parse(message);
    } catch (final IllegalArgumentException ignored) {
      return CONFIGURATION_READ_UNAVAILABLE;
    }
  }

  @JsonValue
  public String wireValue() {
    return this == NONE ? "" : name();
  }
}
