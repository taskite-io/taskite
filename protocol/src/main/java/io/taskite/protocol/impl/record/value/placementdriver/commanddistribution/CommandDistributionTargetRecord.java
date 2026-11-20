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
package io.taskite.protocol.impl.record.value.placementdriver.commanddistribution;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.taskite.protocol.record.UnifiedRecordValue;

public final class CommandDistributionTargetRecord extends UnifiedRecordValue {
  private long distributionKey;
  private String clusterId = "";
  private int expectedPartitionCount;
  private int acknowledgedPartitionCount;

  public long getDistributionKey() {
    return distributionKey;
  }

  public CommandDistributionTargetRecord setDistributionKey(final long distributionKey) {
    this.distributionKey = Math.max(0L, distributionKey);
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public CommandDistributionTargetRecord setClusterId(final String clusterId) {
    this.clusterId = clusterId == null ? "" : clusterId;
    return this;
  }

  public int getExpectedPartitionCount() {
    return expectedPartitionCount;
  }

  public CommandDistributionTargetRecord setExpectedPartitionCount(
      final int expectedPartitionCount) {
    this.expectedPartitionCount = Math.max(0, expectedPartitionCount);
    return this;
  }

  public int getAcknowledgedPartitionCount() {
    return acknowledgedPartitionCount;
  }

  public CommandDistributionTargetRecord setAcknowledgedPartitionCount(
      final int acknowledgedPartitionCount) {
    this.acknowledgedPartitionCount = Math.max(0, acknowledgedPartitionCount);
    return this;
  }

  @JsonIgnore
  public boolean isCompleted() {
    return expectedPartitionCount > 0 && acknowledgedPartitionCount >= expectedPartitionCount;
  }
}
