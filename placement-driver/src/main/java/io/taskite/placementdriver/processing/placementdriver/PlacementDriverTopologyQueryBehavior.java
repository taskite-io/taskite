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
package io.taskite.placementdriver.processing.placementdriver;

import io.taskite.placementdriver.processing.query.TopologyQueryBehavior;
import io.taskite.placementdriver.processing.query.TopologyQueryRejection;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;

public final class PlacementDriverTopologyQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;
  private static final String TOPOLOGY_UNAVAILABLE =
      "PLACEMENT_DRIVER_TOPOLOGY_NOT_AVAILABLE: Expected to query PD topology for member '%s', but no committed PD topology is available";

  public PlacementDriverTopologyQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.PLACEMENT_DRIVER_TOPOLOGY;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    return PlacementDriverMemberTopologyView.forMember(
        state
            .getPlacementDriverTopologyState()
            .topology()
            .orElseThrow(
                () ->
                    TopologyQueryRejection.unavailable(
                        TOPOLOGY_UNAVAILABLE.formatted(query.getRequestingMemberId()))),
        query.getRequestingMemberId());
  }
}
