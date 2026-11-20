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
package io.taskite.placementdriver.state.immutable;

import java.util.List;

/**
 * In-memory projection of a cluster's persisted topology configuration (the SBE {@code
 * BrokerClusterConfigurationRecordCodec}, id 321). {@code voterRoster} is the canonical, sticky set
 * of broker ids eligible to be voters (T9): partition placement derives desired voters from it
 * rather than from "whoever is online this second". An empty roster means "not yet initialised" and
 * the planners fall back to the legacy online-capped-at-clusterSize behaviour, so an un-migrated
 * cluster is byte-for- byte identical to the pre-roster logic. {@code rosterEpoch} is a monotonic
 * version for audit and idempotency of roster transitions (T10).
 */
public record BrokerClusterConfig(
    int clusterSize,
    int partitionSize,
    int replicationFactor,
    List<String> voterRoster,
    long rosterEpoch) {
  public static final long MANAGED_ROSTER_EPOCH = -1L;

  public BrokerClusterConfig {
    voterRoster = voterRoster == null ? List.of() : List.copyOf(voterRoster);
  }

  public boolean managedRoster() {
    return rosterEpoch == MANAGED_ROSTER_EPOCH;
  }

  /**
   * Backwards-compatible sizing-only config with an empty voter roster (epoch 0). Existing callers
   * that only know cluster/partition/replication sizing stay unchanged and select voters via the
   * legacy fallback until a roster is initialised.
   */
  public BrokerClusterConfig(
      final int clusterSize, final int partitionSize, final int replicationFactor) {
    this(clusterSize, partitionSize, replicationFactor, List.of(), 0L);
  }
}
