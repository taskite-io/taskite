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

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

public final class CommandDistributionListRecord extends UnifiedRecordValue {
  private String jobType = "";
  private long distributionKey;
  private String clusterId = "";
  private int total;
  private List<CommandDistributionQueryRecord> subscriptions = new ArrayList<>();

  public String getJobType() {
    return jobType;
  }

  public CommandDistributionListRecord setJobType(final String value) {
    jobType = value == null ? "" : value.trim();
    return this;
  }

  public long getDistributionKey() {
    return distributionKey;
  }

  public CommandDistributionListRecord setDistributionKey(final long value) {
    distributionKey = value;
    return this;
  }

  public String getClusterId() {
    return clusterId;
  }

  public CommandDistributionListRecord setClusterId(final String value) {
    clusterId = value == null ? "" : value.trim();
    return this;
  }

  public int getTotal() {
    return total;
  }

  public CommandDistributionListRecord setTotal(final int value) {
    total = value;
    return this;
  }

  public List<CommandDistributionQueryRecord> getSubscriptions() {
    return subscriptions;
  }

  public CommandDistributionListRecord setSubscriptions(
      final List<CommandDistributionQueryRecord> value) {
    subscriptions = new ArrayList<>(value);
    return this;
  }
}
