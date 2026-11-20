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

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

public final class BrokerClustersRecord extends UnifiedRecordValue {
  private int count;
  private List<Cluster> clusters = new ArrayList<>();

  public int getCount() {
    return count;
  }

  public BrokerClustersRecord setCount(final int count) {
    this.count = Math.max(0, count);
    return this;
  }

  public List<Cluster> getClusters() {
    return clusters;
  }

  public BrokerClustersRecord setClusters(final List<Cluster> clusters) {
    this.clusters = clusters == null ? new ArrayList<>() : new ArrayList<>(clusters);
    return this;
  }

  public static final class Cluster extends UnifiedRecordValue {
    private String clusterId = "";
    private long clusterKey = -1L;
    private int clusterSize;
    private int partitionSize;
    private int replicationFactor;
    private int registeredBrokerCount;
    private int onlineBrokerCount;

    public String getClusterId() {
      return clusterId;
    }

    public long getClusterKey() {
      return clusterKey;
    }

    public Cluster setClusterKey(final long clusterKey) {
      this.clusterKey = clusterKey;
      return this;
    }

    public Cluster setClusterId(final String clusterId) {
      this.clusterId = clusterId == null ? "" : clusterId;
      return this;
    }

    public int getClusterSize() {
      return clusterSize;
    }

    public Cluster setClusterSize(final int clusterSize) {
      this.clusterSize = Math.max(0, clusterSize);
      return this;
    }

    public int getPartitionSize() {
      return partitionSize;
    }

    public Cluster setPartitionSize(final int partitionSize) {
      this.partitionSize = Math.max(0, partitionSize);
      return this;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }

    public Cluster setReplicationFactor(final int replicationFactor) {
      this.replicationFactor = Math.max(0, replicationFactor);
      return this;
    }

    public int getRegisteredBrokerCount() {
      return registeredBrokerCount;
    }

    public Cluster setRegisteredBrokerCount(final int registeredBrokerCount) {
      this.registeredBrokerCount = Math.max(0, registeredBrokerCount);
      return this;
    }

    public int getOnlineBrokerCount() {
      return onlineBrokerCount;
    }

    public Cluster setOnlineBrokerCount(final int onlineBrokerCount) {
      this.onlineBrokerCount = Math.max(0, onlineBrokerCount);
      return this;
    }
  }
}
