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
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeResponseRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyQueryRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyQueryRecordValue.QueryType;
import io.taskite.topology.MemberRole;
import java.util.List;

public final class PlacementDriverMemberChangeQueryBehavior implements TopologyQueryBehavior {
  private final ProcessingState state;
  private static final String ACTIVE_CHANGE_NOT_FOUND =
      "ACTIVE_PD_MEMBER_CHANGE_NOT_FOUND: Expected to query the active PD member change, but no active change was found";
  private static final String CHANGE_NOT_FOUND =
      "PD_MEMBER_CHANGE_NOT_FOUND: Expected to query PD member change '%s', but no matching change was found";
  private static final String TOPOLOGY_UNAVAILABLE =
      "PLACEMENT_DRIVER_TOPOLOGY_NOT_AVAILABLE: Expected to query PD member change '%s', but no committed PD topology is available";

  public PlacementDriverMemberChangeQueryBehavior(final ProcessingState state) {
    this.state = state;
  }

  @Override
  public QueryType queryType() {
    return QueryType.PLACEMENT_DRIVER_CHANGE;
  }

  @Override
  public UnifiedRecordValue createResponseValue(final TopologyQueryRecord query) {
    final MemberChangeRecord change =
        query.getChangeId() == null || query.getChangeId().isBlank()
            ? state
                .getMemberChangeState()
                .active(
                    MemberChangeScope.PLACEMENT_DRIVER,
                    PlacementDriverMemberChangePlanner.RESOURCE_ID)
                .orElseThrow(() -> TopologyQueryRejection.notFound(ACTIVE_CHANGE_NOT_FOUND))
            : state
                .getMemberChangeState()
                .get(
                    MemberChangeScope.PLACEMENT_DRIVER,
                    PlacementDriverMemberChangePlanner.RESOURCE_ID,
                    query.getChangeId())
                .orElseThrow(
                    () ->
                        TopologyQueryRejection.notFound(
                            CHANGE_NOT_FOUND.formatted(query.getChangeId())));
    final MemberTopologyRecord topology =
        state
            .getPlacementDriverTopologyState()
            .topology()
            .orElseThrow(
                () ->
                    TopologyQueryRejection.unavailable(
                        TOPOLOGY_UNAVAILABLE.formatted(change.getChangeId())));
    final List<String> voters =
        topology.getMembers().stream()
            .filter(PlacementDriverMemberChangeQueryBehavior::isVoter)
            .map(MemberTopologyRecord.MemberRecord::getMemberId)
            .toList();
    final List<String> learners =
        topology.getMembers().stream()
            .filter(member -> member.getRole() == MemberRole.LEARNER)
            .map(MemberTopologyRecord.MemberRecord::getMemberId)
            .toList();
    return new MemberChangeResponseRecord()
        .setChange(change)
        .setActualVoters(voters)
        .setActualLearners(learners);
  }

  private static boolean isVoter(final MemberTopologyRecord.MemberRecord member) {
    return member.getRole().isVoter();
  }
}
