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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.placementdriver.processing.member.MemberChangeRejection;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PlacementDriverMemberChangePlannerTest {
  @TempDir Path tempDir;

  @Test
  void registeredMemberIsAddedAsLearnerBeforePromotion() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("registered"))) {
      final MutableProcessingState state = state(storage, "REGISTERED");

      final MemberChangeRecord plan =
          new PlacementDriverMemberChangePlanner()
              .calculate(request(List.of("meta-3"), List.of(), 4), state)
              .memberChange();

      assertEquals(
          List.of(
              MemberChangeOperationType.MEMBER_ADD_LEARNER,
              MemberChangeOperationType.MEMBER_PROMOTE_VOTER),
          plan.getOperations().stream()
              .map(MemberChangeRecord.MemberChangeOperation::getType)
              .toList());
      assertEquals(List.of("meta-0", "meta-1", "meta-2", "meta-3"), plan.getTargetVoters());
    }
  }

  @Test
  void committedLearnerOnlyNeedsExplicitPromotion() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("learner"))) {
      final MutableProcessingState state = state(storage, "LEARNER");

      final MemberChangeRecord plan =
          new PlacementDriverMemberChangePlanner()
              .calculate(request(List.of("meta-3"), List.of(), 4), state)
              .memberChange();

      assertEquals(1, plan.getOperations().size());
      assertEquals(
          MemberChangeOperationType.MEMBER_PROMOTE_VOTER,
          plan.getOperations().getFirst().getType());
    }
  }

  @Test
  void removingLeaderTransfersLeadershipFirst() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("leader-removal"))) {
      final MutableProcessingState state = state(storage, "LEARNER");

      final MemberChangeRecord plan =
          new PlacementDriverMemberChangePlanner()
              .calculate(request(List.of(), List.of("meta-0"), 2), state)
              .memberChange();

      assertEquals(
          List.of(
              MemberChangeOperationType.TRANSFER_LEADER, MemberChangeOperationType.MEMBER_REMOVE),
          plan.getOperations().stream()
              .map(MemberChangeRecord.MemberChangeOperation::getType)
              .toList());
      assertEquals("meta-1", plan.getOperations().getFirst().getMemberId());
    }
  }

  @Test
  void rejectsExplicitAdditionWhenCountDoesNotMatch() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("count-mismatch"))) {
      final MutableProcessingState state = state(storage, "LEARNER");

      final MemberChangeRejection rejection =
          assertThrows(
              MemberChangeRejection.class,
              () ->
                  new PlacementDriverMemberChangePlanner()
                      .calculate(request(List.of("meta-3"), List.of(), 3), state));

      assertEquals(
          "PD_MEMBER_COUNT_MISMATCH: Expected PD member change 'change-1' to produce requested voter count '3', but additions and removals produce '4' voters",
          rejection.getMessage());
    }
  }

  private static MutableProcessingState state(
      final PartitionStorage storage, final String meta3Role) {
    final MutableProcessingState state =
        new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 100, () -> 7);
    state
        .getPlacementDriverTopologyState()
        .updateTopology(
            new MemberTopologyRecord()
                .setLeaderId("meta-0")
                .setTerm(7)
                .setConfigurationEpoch(10)
                .setMembers(
                    List.of(
                        member("meta-0", "LEADER", 100),
                        member("meta-1", "FOLLOWER", 100),
                        member("meta-2", "FOLLOWER", 100),
                        member("meta-3", meta3Role, 100))));
    return state;
  }

  private static MemberTopologyRecord.MemberRecord member(
      final String memberId, final String role, final long commitIndex) {
    return new MemberTopologyRecord.MemberRecord()
        .setMemberId(memberId)
        .setRole(MemberRole.parse(role))
        .setStatus(RegistrationStatus.ONLINE)
        .setHost("127.0.0.1")
        .setGrpcPort(29000 + Integer.parseInt(memberId.substring(memberId.length() - 1)))
        .setRaftPort(29100 + Integer.parseInt(memberId.substring(memberId.length() - 1)))
        .setCommitIndex(commitIndex)
        .setLastApplied(commitIndex);
  }

  private static MemberChangeRecord request(
      final List<String> additions, final List<String> removals, final int count) {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.PLACEMENT_DRIVER)
        .setResourceId(PlacementDriverMemberChangePlanner.RESOURCE_ID)
        .setChangeId("change-1")
        .setRequestedAtEpochMillis(1)
        .setRequestedAdditions(additions)
        .setRequestedRemovals(removals)
        .setRequestedCount(count);
  }
}
