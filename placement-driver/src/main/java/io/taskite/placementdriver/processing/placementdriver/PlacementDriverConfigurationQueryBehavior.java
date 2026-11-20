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
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberConfigurationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.topology.MemberRole;
import java.util.List;

public final class PlacementDriverConfigurationQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;
  private static final String TOPOLOGY_UNAVAILABLE =
      "PLACEMENT_DRIVER_TOPOLOGY_NOT_AVAILABLE: Expected to query PD configuration, but no committed PD topology is available";

  public PlacementDriverConfigurationQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.PLACEMENT_DRIVER_CONFIGURATION;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    final MemberTopologyRecord topology =
        state
            .getPlacementDriverTopologyState()
            .topology()
            .orElseThrow(() -> TopologyQueryRejection.unavailable(TOPOLOGY_UNAVAILABLE));
    final List<String> actualVoters =
        topology.getMembers().stream()
            .filter(PlacementDriverConfigurationQueryBehavior::isVoter)
            .map(MemberTopologyRecord.MemberRecord::getMemberId)
            .toList();
    final List<String> learners =
        topology.getMembers().stream()
            .filter(member -> member.getRole() == MemberRole.LEARNER)
            .map(MemberTopologyRecord.MemberRecord::getMemberId)
            .toList();
    final MemberChangeRecord configuration =
        state
            .getMemberChangeState()
            .configuration(
                MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverMemberChangePlanner.RESOURCE_ID)
            .orElse(null);
    final String activeChangeId =
        state
            .getMemberChangeState()
            .active(
                MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverMemberChangePlanner.RESOURCE_ID)
            .map(MemberChangeRecord::getChangeId)
            .orElse("");
    return new MemberConfigurationRecord()
        .setEpoch(
            configuration == null
                ? topology.getConfigurationEpoch()
                : configuration.getTargetEpoch())
        .setTargetCount(
            configuration == null ? actualVoters.size() : configuration.getTargetCount())
        .setTargetVoters(configuration == null ? actualVoters : configuration.getTargetVoters())
        .setActualVoters(actualVoters)
        .setActualLearners(learners)
        .setLeaderId(topology.getLeaderId())
        .setTerm(topology.getTerm())
        .setActiveChangeId(activeChangeId);
  }

  private static boolean isVoter(final MemberTopologyRecord.MemberRecord member) {
    return member.getRole().isVoter();
  }
}
