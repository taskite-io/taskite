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
package io.taskite.topology;

import com.fasterxml.jackson.annotation.JsonCreator;

/** Durable execution status of the last topology operation observed by a broker partition. */
public enum BrokerTopologyOperationStatus {
  NONE,
  IN_PROGRESS,
  APPLIED,
  FAILED;

  @JsonCreator
  public static BrokerTopologyOperationStatus parse(final String value) {
    if (value == null || value.isBlank()) {
      return NONE;
    }
    try {
      return valueOf(value);
    } catch (final IllegalArgumentException ignored) {
      return NONE;
    }
  }
}
