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

/** Reasons a change cannot be cancelled; NONE retains the empty public value. */
public enum ChangeCancellationReason {
  NONE,
  CHANGE_TERMINAL,
  CHANGE_SUPERSEDED,
  CHANGE_RECONCILING,
  CHANGE_RECOVERY_REQUIRED,
  CHANGE_NOT_CURRENT,
  UNSAFE_CONFIGURATION_CANCELLATION;

  @JsonCreator
  public static ChangeCancellationReason parse(final String value) {
    return value == null || value.isBlank() ? NONE : valueOf(value);
  }

  @JsonValue
  public String wireValue() {
    return this == NONE ? "" : name();
  }
}
