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
package io.taskite.placementdriver.runtime;

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.processing.placementdriver.PlacementDriverMemberChangePlanner;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Admits committed PD registrations as learners without changing the voter roster. */
public final class PlacementDriverLearnerAdmissionListener
    implements PlacementDriverCommittedRecordListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDriverLearnerAdmissionListener.class);
  private final PlacementDriverRaftRuntime raftRuntime;
  private final Runnable topologyChanged;

  public PlacementDriverLearnerAdmissionListener(
      final PlacementDriverRaftRuntime raftRuntime, final Runnable topologyChanged) {
    this.raftRuntime = raftRuntime;
    this.topologyChanged = topologyChanged;
  }

  @Override
  public void onCommittedRecord(final CommittedPlacementDriverRecord committed) {
    final var record = committed.record();
    if (record.metadata().getRecordType() != RecordType.EVENT
        || record.metadata().getIntent() != TopologyIntent.UPDATED
        || !(record.value() instanceof MemberTopologyRecord topology)
        || (topology.getUpdateType() != TopologyUpdateType.REGISTER
            && topology.getUpdateType() != TopologyUpdateType.REBIND
            && topology.getUpdateType() != TopologyUpdateType.RENEW)
        || raftRuntime.raftPartition().role() != RaftRole.LEADER) {
      return;
    }
    if (raftRuntime
        .processingState()
        .getMemberChangeState()
        .active(MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverMemberChangePlanner.RESOURCE_ID)
        .filter(change -> !change.isTerminal())
        .isPresent()) {
      return;
    }
    final Map<String, RaftMember> actual = new LinkedHashMap<>();
    raftRuntime.raftPartition().members().forEach(member -> actual.put(member.memberId(), member));
    final Set<String> removedMembers =
        raftRuntime
            .processingState()
            .getMemberChangeState()
            .configuration(
                MemberChangeScope.PLACEMENT_DRIVER, PlacementDriverMemberChangePlanner.RESOURCE_ID)
            .map(change -> Set.copyOf(change.getRemovedMembers()))
            .orElseGet(Set::of);
    final ArrayList<RaftPeer> target = new ArrayList<>();
    topology.getMembers().stream()
        .filter(member -> !actual.containsKey(member.getMemberId()))
        .filter(
            member ->
                !removedMembers.contains(member.getMemberId())
                    || ((topology.getUpdateType() == TopologyUpdateType.REGISTER
                            || topology.getUpdateType() == TopologyUpdateType.REBIND
                            || member.getRole() == MemberRole.REGISTERED)
                        && topology.getLocalMemberId().equals(member.getMemberId())))
        .filter(member -> member.getStatus() == RegistrationStatus.ONLINE)
        .filter(member -> member.getRaftPort() > 0)
        .forEach(
            member ->
                target.add(
                    new RaftPeer(
                        member.getMemberId(), member.getHost(), member.getRaftPort(), false)));
    raftRuntime
        .addRaftLearners(target)
        .onComplete(
            (ignored, error) -> {
              if (error != null) {
                LOG.warn("failed to admit registered placement-driver learner", error);
              } else {
                topologyChanged.run();
              }
            });
  }
}
