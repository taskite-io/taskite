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

import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.processing.member.MemberChangePlan;
import io.taskite.placementdriver.processing.member.MemberChangePlanner;
import io.taskite.placementdriver.processing.member.MemberChangeRejection;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOutcome;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PlacementDriverMemberChangePlanner implements MemberChangePlanner {
  public static final String RESOURCE_ID = PlacementDriverCluster.CLUSTER_ID;
  private static final String INVALID_RESOURCE =
      "INVALID_PD_RESOURCE_ID: Expected to plan PD member change '%s' for resource '%s', but received resource '%s'";
  private static final String TOPOLOGY_UNAVAILABLE =
      "PLACEMENT_DRIVER_TOPOLOGY_NOT_AVAILABLE: Expected to plan PD member change '%s', but no committed PD topology is available";
  private static final String INVALID_MEMBERS =
      "INVALID_PD_MEMBER_CHANGE: Expected to plan PD member change '%s' with distinct, nonblank additions and removals, but received duplicate, blank or overlapping member ids (additions '%s', removals '%s')";
  private static final String MEMBER_NOT_REGISTERED =
      "PD_MEMBER_NOT_REGISTERED: Expected to add PD member '%s' for change '%s', but no registration was found";
  private static final String MEMBER_NOT_ONLINE =
      "PD_MEMBER_NOT_ONLINE: Expected to add PD member '%s' for change '%s', but its status is '%s'";
  private static final String MEMBER_NOT_FOUND =
      "PD_MEMBER_NOT_FOUND: Expected to remove PD member '%s' for change '%s', but no member was found";
  private static final String COUNT_MISMATCH =
      "PD_MEMBER_COUNT_MISMATCH: Expected PD member change '%s' to produce requested voter count '%d', but additions and removals produce '%d' voters";
  private static final String COUNT_UNREACHABLE =
      "PD_MEMBER_COUNT_UNREACHABLE: Expected PD member change '%s' to produce positive voter count '%d', but only '%d' eligible voters are available";

  @Override
  public MemberChangeScope scope() {
    return MemberChangeScope.PLACEMENT_DRIVER;
  }

  @Override
  public MemberChangePlan calculate(final MemberChangeRecord request, final ProcessingState state) {
    if (!RESOURCE_ID.equals(request.getResourceId())) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          INVALID_RESOURCE.formatted(request.getChangeId(), RESOURCE_ID, request.getResourceId()));
    }
    final MemberTopologyRecord topology =
        state
            .getPlacementDriverTopologyState()
            .topology()
            .orElseThrow(
                () ->
                    rejection(
                        RejectionType.INVALID_STATE,
                        TOPOLOGY_UNAVAILABLE.formatted(request.getChangeId())));
    final Map<String, MemberTopologyRecord.MemberRecord> members = new LinkedHashMap<>();
    topology.getMembers().forEach(member -> members.put(member.getMemberId(), member));
    final LinkedHashSet<String> voters = new LinkedHashSet<>();
    final LinkedHashSet<String> learners = new LinkedHashSet<>();
    topology
        .getMembers()
        .forEach(
            member -> {
              if (member.getRole() == MemberRole.LEARNER) {
                learners.add(member.getMemberId());
              } else if (isVoter(member)) {
                voters.add(member.getMemberId());
              }
            });
    final LinkedHashSet<String> additions = normalized(request.getRequestedAdditions());
    final LinkedHashSet<String> removals = normalized(request.getRequestedRemovals());
    if (additions.size() != request.getRequestedAdditions().size()
        || removals.size() != request.getRequestedRemovals().size()
        || additions.stream().anyMatch(removals::contains)) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          INVALID_MEMBERS.formatted(
              request.getChangeId(),
              request.getRequestedAdditions(),
              request.getRequestedRemovals()));
    }
    additions.forEach(
        memberId -> {
          final MemberTopologyRecord.MemberRecord member = members.get(memberId);
          if (member == null) {
            throw rejection(
                RejectionType.NOT_FOUND,
                MEMBER_NOT_REGISTERED.formatted(memberId, request.getChangeId()));
          }
          if (member.getStatus() != RegistrationStatus.ONLINE) {
            throw rejection(
                RejectionType.INVALID_STATE,
                MEMBER_NOT_ONLINE.formatted(memberId, request.getChangeId(), member.getStatus()));
          }
        });
    removals.forEach(
        memberId -> {
          if (!members.containsKey(memberId)) {
            throw rejection(
                RejectionType.NOT_FOUND,
                MEMBER_NOT_FOUND.formatted(memberId, request.getChangeId()));
          }
        });

    final LinkedHashSet<String> target = new LinkedHashSet<>(voters);
    target.removeAll(removals);
    target.addAll(additions);
    final int targetCount =
        request.getRequestedCount() == null ? target.size() : request.getRequestedCount();
    final boolean countOnly = additions.isEmpty() && removals.isEmpty();
    if (countOnly) {
      resize(target, targetCount, members, removals, topology.getLeaderId());
    } else if (request.getRequestedCount() != null && target.size() != targetCount) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          COUNT_MISMATCH.formatted(request.getChangeId(), targetCount, target.size()));
    }
    if (targetCount < 1 || target.size() != targetCount) {
      throw rejection(
          RejectionType.INVALID_STATE,
          COUNT_UNREACHABLE.formatted(request.getChangeId(), targetCount, target.size()));
    }

    final LinkedHashSet<String> removedMembers = new LinkedHashSet<>(removals);
    voters.stream().filter(memberId -> !target.contains(memberId)).forEach(removedMembers::add);
    final long barrier =
        members.values().stream()
            .filter(member -> member.getMemberId().equals(topology.getLeaderId()))
            .mapToLong(MemberTopologyRecord.MemberRecord::getCommitIndex)
            .findFirst()
            .orElse(0L);
    final List<MemberChangeOperation> operations = new ArrayList<>();
    target.stream()
        .filter(memberId -> !voters.contains(memberId))
        .forEach(
            memberId -> {
              if (!learners.contains(memberId)) {
                operations.add(
                    operation(MemberChangeOperationType.MEMBER_ADD_LEARNER, memberId, 0));
              }
              operations.add(
                  operation(MemberChangeOperationType.MEMBER_PROMOTE_VOTER, memberId, barrier));
            });
    removedMembers.forEach(
        memberId -> {
          if (memberId.equals(topology.getLeaderId()) && !target.isEmpty()) {
            operations.add(
                operation(
                    MemberChangeOperationType.TRANSFER_LEADER,
                    target.stream()
                        .filter(candidate -> !candidate.equals(memberId))
                        .findFirst()
                        .orElse(target.getFirst()),
                    0));
          }
          operations.add(operation(MemberChangeOperationType.MEMBER_REMOVE, memberId, 0));
        });
    final boolean noChanges = operations.isEmpty();
    final MemberChangeRecord plan =
        request
            .copy()
            .setSourceEpoch(topology.getConfigurationEpoch())
            .setSourceVoters(List.copyOf(voters))
            .setTargetEpoch(topology.getConfigurationEpoch() + (noChanges ? 0 : 1))
            .setTargetCount(target.size())
            .setTargetVoters(List.copyOf(target))
            .setRemovedMembers(List.copyOf(removedMembers))
            .setOperations(operations)
            .setStatus(noChanges ? MemberChangeStatus.COMPLETED : MemberChangeStatus.PENDING)
            .setNextOperationIndex(noChanges ? operations.size() : 0)
            .setOutcome(noChanges ? MemberChangeOutcome.NO_CHANGES : MemberChangeOutcome.ACCEPTED);
    return new MemberChangePlan(plan, List.of());
  }

  private static void resize(
      final LinkedHashSet<String> target,
      final int targetSize,
      final Map<String, MemberTopologyRecord.MemberRecord> members,
      final Set<String> removals,
      final String leaderId) {
    if (target.size() < targetSize) {
      members.values().stream()
          .filter(member -> member.getStatus() == RegistrationStatus.ONLINE)
          .filter(member -> !target.contains(member.getMemberId()))
          .filter(member -> !removals.contains(member.getMemberId()))
          .sorted(Comparator.comparing(MemberTopologyRecord.MemberRecord::getMemberId))
          .limit(targetSize - target.size())
          .map(MemberTopologyRecord.MemberRecord::getMemberId)
          .forEach(target::add);
    }
    if (target.size() > targetSize) {
      target.stream()
          .filter(memberId -> !memberId.equals(leaderId))
          .sorted(Comparator.reverseOrder())
          .limit(target.size() - targetSize)
          .toList()
          .forEach(target::remove);
    }
  }

  private static MemberChangeOperation operation(
      final MemberChangeOperationType type, final String memberId, final long barrierIndex) {
    return new MemberChangeOperation()
        .setOperationId(type.name().toLowerCase() + "-" + memberId)
        .setType(type)
        .setMemberId(memberId)
        .setStatus(MemberChangeOperationStatus.PENDING)
        .setBarrierIndex(barrierIndex);
  }

  private static LinkedHashSet<String> normalized(final List<String> values) {
    final LinkedHashSet<String> normalized = new LinkedHashSet<>();
    values.stream()
        .filter(value -> value != null && !value.isBlank())
        .map(String::trim)
        .forEach(normalized::add);
    return normalized;
  }

  private static boolean isVoter(final MemberTopologyRecord.MemberRecord member) {
    return member.getRole().isVoter();
  }

  private static MemberChangeRejection rejection(final RejectionType type, final String reason) {
    return new MemberChangeRejection(type, reason);
  }
}
