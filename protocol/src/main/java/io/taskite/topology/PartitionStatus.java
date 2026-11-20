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

/**
 * Lifecycle status of a partition within the cluster topology. Kept in the shared value model so it
 * can travel on the wire in later phases; in Phase 1 it is materialized PD-side only and defaults
 * to {@link #ACTIVE} for the round-robin assignment.
 *
 * <ul>
 *   <li>{@code CREATING} - the partition is being bootstrapped and must not take traffic yet.
 *   <li>{@code ACTIVE} - the partition has a stable voter set and can serve reads/writes.
 *   <li>{@code REBALANCING} - membership is changing (join/leave in progress).
 *   <li>{@code DELETING} - the partition is being torn down.
 * </ul>
 */
public enum PartitionStatus {
  CREATING,
  ACTIVE,
  REBALANCING,
  DELETING;

  @JsonCreator
  public static PartitionStatus parse(final String value) {
    return value == null || value.isBlank() ? null : valueOf(value);
  }
}
