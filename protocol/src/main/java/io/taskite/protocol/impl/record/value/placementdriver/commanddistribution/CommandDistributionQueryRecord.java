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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class CommandDistributionQueryRecord extends UnifiedRecordValue {
  private long distributionKey;
  private long subscriptionKey;
  private long deletionDistributionKey;
  private String originClusterId = "";
  private String clusterId = "";
  private JobSubscriptionCreationType creationType;
  private String valueType = "";
  private JobSubscriptionDistributionStatus status;
  private List<Target> targets = new ArrayList<>();
  private String name = "";
  private String jobType = "";
  private String tenantId = "";
  private JobSubscriptionRecord subscription;
  private long generation;
  private long lastRedistributedAt;

  public String getClusterId() {
    return clusterId;
  }

  public CommandDistributionQueryRecord setClusterId(final String value) {
    clusterId = value;
    return this;
  }

  public JobSubscriptionCreationType getCreationType() {
    return creationType;
  }

  public CommandDistributionQueryRecord setCreationType(final JobSubscriptionCreationType value) {
    creationType = value;
    return this;
  }

  public String getName() {
    return name;
  }

  public CommandDistributionQueryRecord setName(final String value) {
    name = value;
    return this;
  }

  public String getJobType() {
    return jobType;
  }

  public CommandDistributionQueryRecord setJobType(final String value) {
    jobType = value;
    return this;
  }

  public String getTenantId() {
    return tenantId;
  }

  public CommandDistributionQueryRecord setTenantId(final String value) {
    tenantId = value;
    return this;
  }

  public JobSubscriptionRecord getSubscription() {
    return subscription;
  }

  public CommandDistributionQueryRecord setSubscription(final JobSubscriptionRecord value) {
    subscription = value;
    return this;
  }

  public long getGeneration() {
    return generation;
  }

  public CommandDistributionQueryRecord setGeneration(final long value) {
    generation = value;
    return this;
  }

  public long getLastRedistributedAt() {
    return lastRedistributedAt;
  }

  public CommandDistributionQueryRecord setLastRedistributedAt(final long value) {
    lastRedistributedAt = value;
    return this;
  }

  public long getDistributionKey() {
    return distributionKey;
  }

  public long getSubscriptionKey() {
    return subscriptionKey;
  }

  public CommandDistributionQueryRecord setSubscriptionKey(final long value) {
    subscriptionKey = value;
    return this;
  }

  public long getDeletionDistributionKey() {
    return deletionDistributionKey;
  }

  public CommandDistributionQueryRecord setDeletionDistributionKey(final long value) {
    deletionDistributionKey = value;
    return this;
  }

  public CommandDistributionQueryRecord setDistributionKey(final long distributionKey) {
    this.distributionKey = Math.max(0L, distributionKey);
    return this;
  }

  public String getOriginClusterId() {
    return originClusterId;
  }

  public CommandDistributionQueryRecord setOriginClusterId(final String originClusterId) {
    this.originClusterId = originClusterId == null ? "" : originClusterId;
    return this;
  }

  public String getValueType() {
    return valueType;
  }

  public CommandDistributionQueryRecord setValueType(final String valueType) {
    this.valueType = valueType == null ? "" : valueType;
    return this;
  }

  public JobSubscriptionDistributionStatus getStatus() {
    return status;
  }

  public CommandDistributionQueryRecord setStatus(final JobSubscriptionDistributionStatus status) {
    this.status = status;
    return this;
  }

  public List<Target> getTargets() {
    return targets;
  }

  public CommandDistributionQueryRecord setTargets(final List<Target> targets) {
    this.targets = targets == null ? new ArrayList<>() : new ArrayList<>(targets);
    return this;
  }

  public static final class Target extends UnifiedRecordValue {
    private String clusterId = "";
    private int expectedPartitions;
    private int acknowledgedPartitions;
    private List<Integer> pendingPartitions = new ArrayList<>();
    private JobSubscriptionDistributionStatus status;
    private List<Integer> confirmedPartitionIds = new ArrayList<>();
    private long mirrorKey;

    public List<Integer> getConfirmedPartitionIds() {
      return confirmedPartitionIds;
    }

    public Target setConfirmedPartitionIds(final List<Integer> value) {
      confirmedPartitionIds = new ArrayList<>(value);
      return this;
    }

    public long getMirrorKey() {
      return mirrorKey;
    }

    public Target setMirrorKey(final long value) {
      mirrorKey = value;
      return this;
    }

    public String getClusterId() {
      return clusterId;
    }

    public Target setClusterId(final String clusterId) {
      this.clusterId = clusterId == null ? "" : clusterId;
      return this;
    }

    public int getExpectedPartitions() {
      return expectedPartitions;
    }

    public Target setExpectedPartitions(final int expectedPartitions) {
      this.expectedPartitions = Math.max(0, expectedPartitions);
      return this;
    }

    public int getAcknowledgedPartitions() {
      return acknowledgedPartitions;
    }

    public Target setAcknowledgedPartitions(final int acknowledgedPartitions) {
      this.acknowledgedPartitions = Math.max(0, acknowledgedPartitions);
      return this;
    }

    public List<Integer> getPendingPartitions() {
      return pendingPartitions;
    }

    public Target setPendingPartitions(final List<Integer> pendingPartitions) {
      this.pendingPartitions =
          pendingPartitions == null ? new ArrayList<>() : new ArrayList<>(pendingPartitions);
      return this;
    }

    public JobSubscriptionDistributionStatus getStatus() {
      return status;
    }

    public Target setStatus(final JobSubscriptionDistributionStatus status) {
      this.status = status;
      return this;
    }
  }
}
