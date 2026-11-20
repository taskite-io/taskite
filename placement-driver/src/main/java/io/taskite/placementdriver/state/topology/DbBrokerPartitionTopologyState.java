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
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.mutable.MutableBrokerPartitionTopologyState;
import io.taskite.placementdriver.state.mutable.MutableJobYieldSyncState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DbBrokerPartitionTopologyState implements MutableBrokerPartitionTopologyState {
  // [clusterId, partitionId] encoded as "clusterId:partitionId".
  private final DbString partitionKey;
  // [clusterId, partitionId]: typed topology index key and recovery cursor.
  private final DbCompositeKey<DbString, DbInt> indexedKey;
  private final PartitionTopologyRecordValue partitionValue;
  // PD_PARTITION_TOPOLOGY: ["clusterId:partitionId"] -> PartitionTopologyRecordCodec bytes.
  private final ColumnFamily<DbString, PartitionTopologyRecordValue>
      partitionTopologyColumnFamilies;
  // PD_PARTITION_TOPOLOGY_INDEX: [clusterId, partitionId] -> nil.
  // Typed partition IDs support ordered, bounded recovery scans across cluster boundaries.
  private final ColumnFamily<DbCompositeKey<DbString, DbInt>, DbNil> topologyIndexColumnFamilies;
  private final MutableJobYieldSyncState jobYieldSynchronization;

  public DbBrokerPartitionTopologyState(
      final ProcessingStateStore storage, final MutableJobYieldSyncState jobYieldSynchronization) {
    partitionKey = new DbString();
    indexedKey = new DbCompositeKey<>(new DbString(), new DbInt());
    partitionValue = new PartitionTopologyRecordValue();

    this.jobYieldSynchronization = jobYieldSynchronization;
    partitionTopologyColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PARTITION_TOPOLOGY, partitionKey, partitionValue);
    topologyIndexColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PARTITION_TOPOLOGY_INDEX, indexedKey, DbNil.INSTANCE);
  }

  @Override
  public List<PartitionTopology> partitionTopologiesAfter(
      final String clusterId, final int partitionId, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final List<PartitionTopology> result = new ArrayList<>();
    final var start = wrapIndexedKey(clusterId, partitionId);
    topologyIndexColumnFamilies.whileTrue(
        start,
        (key, value) -> {
          if (!key.first().toString().equals(clusterId) || key.second().getValue() != partitionId) {
            partitionTopology(key.first().toString(), key.second().getValue())
                .ifPresent(result::add);
          }
          return result.size() < limit;
        });
    return result;
  }

  public void rebuildTopologyIndex() {
    topologyIndexColumnFamilies.whileTrue(
        (key, value) -> {
          topologyIndexColumnFamilies.deleteIfExists(key);
          return true;
        });
    partitionTopologyColumnFamilies.forEach(
        (key, value) -> {
          final var topology = value.getRecord();
          topologyIndexColumnFamilies.upsert(
              wrapIndexedKey(topology.clusterId(), topology.partitionId()), DbNil.INSTANCE);
        });
  }

  // [clusterId, partitionId]: typed key used by PD_PARTITION_TOPOLOGY_INDEX.
  private DbCompositeKey<DbString, DbInt> wrapIndexedKey(
      final String clusterId, final int partitionId) {
    indexedKey.first().wrapString(clusterId);
    indexedKey.second().wrapInt(partitionId);
    return indexedKey;
  }

  @Override
  public Optional<PartitionTopology> partitionTopology(
      final String clusterId, final int partitionId) {
    partitionKey.wrapString(key(clusterId, partitionId));
    final var stored =
        partitionTopologyColumnFamilies.get(partitionKey, PartitionTopologyRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public List<PartitionTopology> partitionTopologies(final String clusterId) {
    final String prefix = clusterId + ":";
    final List<PartitionTopology> topologies = new ArrayList<>();
    partitionTopologyColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            topologies.add(value.getRecord());
          }
        });
    topologies.sort(Comparator.comparingInt(PartitionTopology::partitionId));
    return topologies;
  }

  @Override
  public void putPartitionTopology(final PartitionTopology topology) {
    partitionKey.wrapString(key(topology.clusterId(), topology.partitionId()));
    partitionValue.setRecord(topology);
    partitionTopologyColumnFamilies.upsert(partitionKey, partitionValue);
    topologyIndexColumnFamilies.upsert(
        wrapIndexedKey(topology.clusterId(), topology.partitionId()), DbNil.INSTANCE);
    jobYieldSynchronization.ensurePartition(topology.clusterId(), topology.partitionId());
  }

  @Override
  public void deletePartitionTopology(final String clusterId, final int partitionId) {
    partitionKey.wrapString(key(clusterId, partitionId));
    partitionTopologyColumnFamilies.deleteIfExists(partitionKey);
    topologyIndexColumnFamilies.deleteIfExists(wrapIndexedKey(clusterId, partitionId));
    jobYieldSynchronization.delete(clusterId, partitionId);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    for (final PartitionTopology topology : partitionTopologies(clusterId)) {
      deletePartitionTopology(clusterId, topology.partitionId());
    }
  }

  // ["clusterId:partitionId"]: primary topology key.
  private static String key(final String clusterId, final int partitionId) {
    return clusterId + ":" + partitionId;
  }
}
