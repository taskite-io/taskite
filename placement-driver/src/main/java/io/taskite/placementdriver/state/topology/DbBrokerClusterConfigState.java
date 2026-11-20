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
package io.taskite.placementdriver.state.topology;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableBrokerClusterConfigState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.List;
import java.util.Optional;

public final class DbBrokerClusterConfigState implements MutableBrokerClusterConfigState {
  // [clusterId]
  private final DbString topologyKey;
  private final BrokerClusterConfigurationRecordValue topologyValue;
  // PD_TOPOLOGY_CONFIGS: [clusterId] -> BrokerClusterConfigurationRecordCodec bytes.
  // Includes cluster/partition size, replication factor, voter roster and roster epoch.
  private final ColumnFamily<DbString, BrokerClusterConfigurationRecordValue>
      topologyConfigsColumnFamilies;

  public DbBrokerClusterConfigState(final ProcessingStateStore storage) {
    topologyKey = new DbString();
    topologyValue = new BrokerClusterConfigurationRecordValue();

    topologyConfigsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_TOPOLOGY_CONFIGS, topologyKey, topologyValue);
  }

  @Override
  public BrokerClusterConfig clusterConfig(final String clusterId) {
    return findClusterConfig(clusterId).orElse(new BrokerClusterConfig(1, 1, 1));
  }

  @Override
  public Optional<BrokerClusterConfig> findClusterConfig(final String clusterId) {
    topologyKey.wrapString(clusterId);
    final var stored =
        topologyConfigsColumnFamilies.get(topologyKey, BrokerClusterConfigurationRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public void putClusterConfig(final String clusterId, final BrokerClusterConfig config) {
    topologyKey.wrapString(clusterId);
    topologyValue.setRecord(clusterId, config);
    topologyConfigsColumnFamilies.upsert(topologyKey, topologyValue);
  }

  @Override
  public void delete(final String clusterId) {
    topologyKey.wrapString(clusterId);
    topologyConfigsColumnFamilies.deleteIfExists(topologyKey);
  }

  @Override
  public boolean updateVoterRoster(final String clusterId, final List<String> voterRoster) {
    final Optional<BrokerClusterConfig> existing = findClusterConfig(clusterId);
    if (existing.isEmpty()) {
      return false;
    }
    final BrokerClusterConfig config = existing.get();
    final List<String> next = voterRoster == null ? List.of() : voterRoster;
    if (config.voterRoster().equals(next)) {
      return false;
    }
    putClusterConfig(
        clusterId,
        new BrokerClusterConfig(
            config.clusterSize(),
            config.partitionSize(),
            config.replicationFactor(),
            next,
            config.rosterEpoch() + 1));
    return true;
  }
}
