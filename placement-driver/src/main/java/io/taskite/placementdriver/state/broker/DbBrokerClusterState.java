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
package io.taskite.placementdriver.state.broker;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableBrokerClusterState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.Protocol;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.List;

/** Broker cluster id to stable key namespace, plus the non-recyclable allocation high-water. */
public final class DbBrokerClusterState implements MutableBrokerClusterState {
  private static final String LATEST_CLUSTER_KEY = "cluster-key/latestId";
  private static final String UNKNOWN_CLUSTER = "No clusterKey is registered for cluster '%s'";
  private static final String INVALID_ALLOCATION =
      "Expected clusterKey %d after the committed allocation high-water, got %d for cluster '%s'";
  // [clusterId]
  private final DbString clusterIdKey;
  private final ColumnFamily<DbString, DbLong> clusterIdsColumnFamilies;
  private final ColumnFamily<DbString, DbLong> metadataColumnFamilies;
  private final DbString latestClusterKey = new DbString();
  private final DbLong value = new DbLong();

  public DbBrokerClusterState(final ProcessingStateStore storage) {
    clusterIdKey = new DbString();

    clusterIdsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_CLUSTER_IDS, clusterIdKey, new DbLong());
    metadataColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.KEY, new DbString(), new DbLong());
    latestClusterKey.wrapString(LATEST_CLUSTER_KEY);
  }

  @Override
  public void remember(final String clusterId, final long clusterKey) {
    Protocol.validateClusterKey(clusterKey);
    final long expected = latestClusterKey() + 1;
    if (clusterKey != expected) {
      throw new IllegalStateException(
          INVALID_ALLOCATION.formatted(expected, clusterKey, clusterId));
    }
    clusterIdKey.wrapString(clusterId);
    value.wrapLong(clusterKey);
    clusterIdsColumnFamilies.upsert(clusterIdKey, value);
    metadataColumnFamilies.upsert(latestClusterKey, value);
  }

  @Override
  public long clusterKey(final String clusterId) {
    clusterIdKey.wrapString(clusterId);
    final DbLong mapping = clusterIdsColumnFamilies.get(clusterIdKey);
    if (mapping == null || mapping.getValue() == Protocol.PLACEMENT_DRIVER_CLUSTER_KEY) {
      throw new IllegalStateException(UNKNOWN_CLUSTER.formatted(clusterId));
    }
    Protocol.validateClusterKey(mapping.getValue());
    return mapping.getValue();
  }

  @Override
  public long latestClusterKey() {
    final DbLong latest = metadataColumnFamilies.get(latestClusterKey);
    if (latest == null) {
      return Protocol.PLACEMENT_DRIVER_CLUSTER_KEY;
    }
    Protocol.validateClusterKey(latest.getValue());
    return latest.getValue();
  }

  @Override
  public void delete(final String clusterId) {
    clusterIdKey.wrapString(clusterId);
    clusterIdsColumnFamilies.deleteIfExists(clusterIdKey);
  }

  @Override
  public List<String> clusterIds() {
    final List<String> clusterIds = new ArrayList<>();
    clusterIdsColumnFamilies.forEach((key, clusterKey) -> clusterIds.add(key.toString()));
    clusterIds.sort(String::compareTo);
    return clusterIds;
  }
}
