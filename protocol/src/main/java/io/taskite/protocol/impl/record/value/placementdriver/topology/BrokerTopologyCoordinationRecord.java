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
package io.taskite.protocol.impl.record.value.placementdriver.topology;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.List;

/** Internal topology coordination command/result; independent of partition execution plans. */
public final class BrokerTopologyCoordinationRecord extends UnifiedRecordValue {
  private String clusterId = "";
  private int partitionId;
  private long revision;
  private int cursorPartition;
  private String cursorCluster = "";
  private long dueAt;
  private long coordinatedAt;
  private boolean finished;
  private boolean observingSubmit;
  private List<Resource> resources = List.of();

  public String getClusterId() {
    return clusterId;
  }

  public BrokerTopologyCoordinationRecord setClusterId(final String value) {
    clusterId = value;
    return this;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public BrokerTopologyCoordinationRecord setPartitionId(final int value) {
    partitionId = value;
    return this;
  }

  /** Derived from the existing task identity; does not add a persisted record field. */
  @JsonIgnore
  public TaskType getTaskType() {
    return TaskType.forIdentity(clusterId, partitionId);
  }

  public long getRevision() {
    return revision;
  }

  public BrokerTopologyCoordinationRecord setRevision(final long value) {
    revision = value;
    return this;
  }

  public int getCursorPartition() {
    return cursorPartition;
  }

  public BrokerTopologyCoordinationRecord setCursorPartition(final int value) {
    cursorPartition = value;
    return this;
  }

  public String getCursorCluster() {
    return cursorCluster;
  }

  public BrokerTopologyCoordinationRecord setCursorCluster(final String value) {
    cursorCluster = value;
    return this;
  }

  public long getDueAt() {
    return dueAt;
  }

  public BrokerTopologyCoordinationRecord setDueAt(final long value) {
    dueAt = value;
    return this;
  }

  /** Log command time used to advance the coordination queue without changing Applier APIs. */
  public long getCoordinatedAt() {
    return coordinatedAt;
  }

  public BrokerTopologyCoordinationRecord setCoordinatedAt(final long value) {
    coordinatedAt = value;
    return this;
  }

  public boolean isFinished() {
    return finished;
  }

  public BrokerTopologyCoordinationRecord setFinished(final boolean value) {
    finished = value;
    return this;
  }

  public List<Resource> getResources() {
    return resources;
  }

  public boolean isObservingSubmit() {
    return observingSubmit;
  }

  public BrokerTopologyCoordinationRecord setObservingSubmit(final boolean value) {
    observingSubmit = value;
    return this;
  }

  public BrokerTopologyCoordinationRecord setResources(final List<Resource> value) {
    resources = List.copyOf(value);
    return this;
  }

  public record Resource(String clusterId, int partitionId) {}

  public enum TaskType {
    RECOVERY_SCAN,
    CLUSTER_EXPANSION,
    PARTITION_COORDINATION;

    public static TaskType forIdentity(final String clusterId, final int partitionId) {
      validateTaskIdentity(clusterId, partitionId);
      if (clusterId.isEmpty()) {
        return RECOVERY_SCAN;
      }
      return partitionId == 0 ? CLUSTER_EXPANSION : PARTITION_COORDINATION;
    }

    /** Rejects identities that cannot represent a schedulable topology task. */
    public static void validateTaskIdentity(final String clusterId, final int partitionId) {
      if (partitionId < 0 || (clusterId.isEmpty() && partitionId != 0)) {
        throw new IllegalArgumentException(
            "Invalid topology coordination task: clusterId="
                + clusterId
                + ", partitionId="
                + partitionId);
      }
    }
  }
}
