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
package io.taskite.protocol.impl.record.value.placementdriver.job;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.Objects;

/** Durable progress and scheduling state for one partition's job yield synchronization. */
public final class JobYieldSyncRecord extends UnifiedRecordValue {
  @JsonProperty private String clusterId;
  @JsonProperty private int partitionId;
  @JsonProperty private long cursor;
  @JsonProperty private long attemptId;
  @JsonProperty private long dueAt;
  @JsonProperty private boolean inFlight;

  public JobYieldSyncRecord() {}

  @JsonCreator
  public JobYieldSyncRecord(
      @JsonProperty("clusterId") String clusterId,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("cursor") long cursor,
      @JsonProperty("attemptId") long attemptId,
      @JsonProperty("dueAt") long dueAt,
      @JsonProperty("inFlight") boolean inFlight) {
    this.clusterId = clusterId;
    this.partitionId = partitionId;
    this.cursor = cursor;
    this.attemptId = attemptId;
    this.dueAt = dueAt;
    this.inFlight = inFlight;
  }

  public String clusterId() {
    return clusterId;
  }

  public int partitionId() {
    return partitionId;
  }

  public long cursor() {
    return cursor;
  }

  public long attemptId() {
    return attemptId;
  }

  public long dueAt() {
    return dueAt;
  }

  public boolean inFlight() {
    return inFlight;
  }

  @Override
  public boolean equals(final Object value) {
    return value instanceof JobYieldSyncRecord other
        && Objects.equals(clusterId, other.clusterId)
        && partitionId == other.partitionId
        && cursor == other.cursor
        && attemptId == other.attemptId
        && dueAt == other.dueAt
        && inFlight == other.inFlight;
  }

  @Override
  public int hashCode() {
    int hash = Objects.hashCode(clusterId);
    hash = 31 * hash + Integer.hashCode(partitionId);
    hash = 31 * hash + Long.hashCode(cursor);
    hash = 31 * hash + Long.hashCode(attemptId);
    hash = 31 * hash + Long.hashCode(dueAt);
    return 31 * hash + Boolean.hashCode(inFlight);
  }

  @Override
  public String toString() {
    return "JobYieldSyncRecord[clusterId="
        + clusterId
        + ", partitionId="
        + partitionId
        + ", cursor="
        + cursor
        + ", attemptId="
        + attemptId
        + ", dueAt="
        + dueAt
        + ", inFlight="
        + inFlight
        + "]";
  }
}
