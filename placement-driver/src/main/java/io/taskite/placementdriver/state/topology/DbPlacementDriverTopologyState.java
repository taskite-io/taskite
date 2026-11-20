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
import io.taskite.placementdriver.state.mutable.MutablePlacementDriverTopologyState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.Optional;

public final class DbPlacementDriverTopologyState implements MutablePlacementDriverTopologyState {
  private static final String TOPOLOGY_KEY = "placement-driver";

  // ["placement-driver"]: fixed native topology identity, separate from the business clusterId.
  private final DbString topologyKey;
  private final MemberTopologyRecordValue topologyValue;
  // PD_MEMBER_TOPOLOGY: ["placement-driver"] -> MemberTopologyRecord bytes.
  private final ColumnFamily<DbString, MemberTopologyRecordValue> topologyColumnFamilies;

  public DbPlacementDriverTopologyState(final ProcessingStateStore storage) {
    topologyKey = new DbString();
    topologyValue = new MemberTopologyRecordValue();

    topologyColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_MEMBER_TOPOLOGY, topologyKey, topologyValue);
  }

  @Override
  public Optional<MemberTopologyRecord> topology() {
    topologyKey.wrapString(TOPOLOGY_KEY);
    final var stored = topologyColumnFamilies.get(topologyKey, MemberTopologyRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public void updateTopology(final MemberTopologyRecord topology) {
    topologyKey.wrapString(TOPOLOGY_KEY);
    topologyValue.setRecord(topology);
    topologyColumnFamilies.upsert(topologyKey, topologyValue);
  }
}
