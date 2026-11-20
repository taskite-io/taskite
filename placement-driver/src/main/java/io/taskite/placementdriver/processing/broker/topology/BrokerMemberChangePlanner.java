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
package io.taskite.placementdriver.processing.broker.topology;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeIds;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterConfigurationChange;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangePlanningService.BrokerClusterCreation;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerTopologyOperationRequests;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.BrokerTopologyPlanner;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.processing.member.MemberChangePlan;
import io.taskite.placementdriver.processing.member.MemberChangePlanner;
import io.taskite.placementdriver.processing.member.MemberChangeRejection;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOutcome;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.RejectionType;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Calculates Broker roster changes and the partition child plans needed to realize them. */
public final class BrokerMemberChangePlanner implements MemberChangePlanner {
  private static final long CHANGE_TIMEOUT_MILLIS = Duration.ofMinutes(5).toMillis();
  private static final String CHANGE_IN_PROGRESS =
      "TOPOLOGY_CHANGE_IN_PROGRESS:%s\nExpected to plan Broker member change '%s' in cluster '%s', but the listed topology changes are still pending";
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to plan Broker member change '%s', but cluster '%s' is not registered";
  private static final String INVALID_MEMBERS =
      "INVALID_BROKER_MEMBER_CHANGE: Expected to plan member change '%s' in cluster '%s' with distinct, nonblank additions and removals, but received duplicate, blank or overlapping Broker ids (additions '%s', removals '%s')";
  private static final String BROKER_NOT_ONLINE =
      "BROKER_NOT_ONLINE: Expected to add broker '%s' for member change '%s' in cluster '%s', but it is not registered or not ONLINE";
  private static final String BROKER_NOT_FOUND =
      "BROKER_NOT_FOUND: Expected to remove broker '%s' for member change '%s' in cluster '%s', but it is neither registered nor in the voter roster";
  private static final String COUNT_MISMATCH =
      "BROKER_MEMBER_COUNT_MISMATCH: Expected member change '%s' in cluster '%s' to produce requested broker count '%d', but additions and removals produce '%d' brokers";
  private static final String COUNT_UNREACHABLE =
      "BROKER_COUNT_UNREACHABLE: Expected member change '%s' in cluster '%s' to produce positive broker count '%d', but the target roster contains '%d' brokers";
  private static final String BELOW_REPLICATION_FACTOR =
      "Expected member change '%s' in cluster '%s' to retain at least replicationFactor '%d' brokers, but requested count is '%d'";
  private static final String INSUFFICIENT_ONLINE_BROKERS =
      "INSUFFICIENT_ONLINE_BROKERS: Expected member change '%s' in cluster '%s' to provide replicationFactor '%d' voters, but only '%d' eligible ONLINE voters are available";
  private static final String FREEZE_FAILED =
      "%s\nExpected to plan Broker member change '%s' in cluster '%s' for partition '%d', but freezing the child topology plan failed";
  private final BrokerClusterChangePlanningService planningService;

  public BrokerMemberChangePlanner(final BrokerClusterChangePlanningService planningService) {
    this.planningService = planningService;
  }

  @Override
  public MemberChangeScope scope() {
    return MemberChangeScope.BROKER;
  }

  @Override
  public MemberChangePlan calculate(final MemberChangeRecord request, final ProcessingState state) {
    final var activeClusterChange =
        state.getBrokerTopologyChangeState().activeClusterChange(request.getResourceId());
    final List<ChangePlan> pendingPlans =
        state.getBrokerTopologyChangeState().pendingChangePlans(request.getResourceId());
    if (activeClusterChange.isPresent() || !pendingPlans.isEmpty()) {
      final List<String> pendingChangeIds =
          activeClusterChange
              .map(change -> List.of(change.changeId()))
              .orElseGet(() -> pendingPlans.stream().map(ChangePlan::changeId).distinct().toList());
      throw rejection(
          RejectionType.INVALID_STATE,
          CHANGE_IN_PROGRESS.formatted(
              String.join(",", pendingChangeIds), request.getChangeId(), request.getResourceId()));
    }
    final BrokerClusterConfig current =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(request.getResourceId())
            .orElseThrow(
                () ->
                    rejection(
                        RejectionType.NOT_FOUND,
                        CLUSTER_NOT_FOUND.formatted(
                            request.getChangeId(), request.getResourceId())));
    final List<BrokerRegistration> registrations =
        state.getBrokerRegistrationState().registrations(request.getResourceId());
    final Map<String, BrokerRegistration> registrationsById = registrationsById(registrations);
    final LinkedHashSet<String> additions = normalized(request.getRequestedAdditions());
    final LinkedHashSet<String> removals = normalized(request.getRequestedRemovals());
    if (additions.size() != request.getRequestedAdditions().size()
        || removals.size() != request.getRequestedRemovals().size()
        || additions.stream().anyMatch(removals::contains)) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          INVALID_MEMBERS.formatted(
              request.getChangeId(),
              request.getResourceId(),
              request.getRequestedAdditions(),
              request.getRequestedRemovals()));
    }
    additions.forEach(
        brokerId -> {
          final BrokerRegistration registration = registrationsById.get(brokerId);
          if (registration == null || registration.status() != BrokerStatus.ONLINE) {
            throw rejection(
                RejectionType.INVALID_STATE,
                BROKER_NOT_ONLINE.formatted(
                    brokerId, request.getChangeId(), request.getResourceId()));
          }
        });
    removals.forEach(
        brokerId -> {
          if (!registrationsById.containsKey(brokerId)
              && !current.voterRoster().contains(brokerId)) {
            throw rejection(
                RejectionType.NOT_FOUND,
                BROKER_NOT_FOUND.formatted(
                    brokerId, request.getChangeId(), request.getResourceId()));
          }
        });

    final LinkedHashSet<String> target = new LinkedHashSet<>(current.voterRoster());
    target.removeAll(removals);
    target.addAll(additions);
    final int requestedCount =
        request.getRequestedCount() == null ? target.size() : request.getRequestedCount();
    final boolean countOnly = additions.isEmpty() && removals.isEmpty();
    if (countOnly) {
      // An explicit resize may readmit a returned startup; automatic roster repair may not.
      resize(
          target,
          requestedCount,
          BrokerTopologyPlanner.eligibleRegistrations(
              state.getBrokerRegistrationState(), request.getResourceId()),
          removals);
    } else if (request.getRequestedCount() != null && target.size() != requestedCount) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          COUNT_MISMATCH.formatted(
              request.getChangeId(), request.getResourceId(), requestedCount, target.size()));
    }
    if (requestedCount < 1 || target.size() != requestedCount) {
      throw rejection(
          RejectionType.INVALID_STATE,
          COUNT_UNREACHABLE.formatted(
              request.getChangeId(), request.getResourceId(), requestedCount, target.size()));
    }
    if (requestedCount < current.replicationFactor()) {
      throw rejection(
          RejectionType.INVALID_ARGUMENT,
          BELOW_REPLICATION_FACTOR.formatted(
              request.getChangeId(),
              request.getResourceId(),
              current.replicationFactor(),
              requestedCount));
    }

    final List<String> targetVoters = List.copyOf(target);
    final LinkedHashSet<String> removed = new LinkedHashSet<>(removals);
    current.voterRoster().stream()
        .filter(brokerId -> !target.contains(brokerId))
        .forEach(removed::add);
    final BrokerClusterConfig targetConfig =
        new BrokerClusterConfig(
            requestedCount,
            current.partitionSize(),
            current.replicationFactor(),
            targetVoters,
            BrokerClusterConfig.MANAGED_ROSTER_EPOCH);
    final List<BrokerRegistration> eligible =
        registrations.stream()
            .filter(registration -> registration.status() == BrokerStatus.ONLINE)
            .filter(registration -> !removed.contains(registration.brokerId()))
            .filter(
                registration ->
                    additions.contains(registration.brokerId())
                        || !state
                            .getBrokerRegistrationState()
                            .isLearnerExcluded(
                                request.getResourceId(),
                                registration.brokerId(),
                                registration.incarnationId()))
            .toList();
    final List<BrokerRegistration> voters =
        planningService.calculateVoterBrokers(targetConfig, eligible);
    if (voters.size() < current.replicationFactor()) {
      throw rejection(
          RejectionType.INVALID_STATE,
          INSUFFICIENT_ONLINE_BROKERS.formatted(
              request.getChangeId(),
              request.getResourceId(),
              current.replicationFactor(),
              voters.size()));
    }

    final List<ChangePlan> children = new ArrayList<>();
    for (int partitionId = 1; partitionId <= current.partitionSize(); partitionId++) {
      final var desired =
          planningService.calculateClusterCreation(
              new BrokerClusterCreation(
                  request.getResourceId(), targetConfig, partitionId, eligible, voters));
      final PartitionTopology existing =
          state
              .getBrokerPartitionTopologyState()
              .partitionTopology(request.getResourceId(), partitionId)
              .orElse(null);
      final List<TopologyOperation> operations =
          existing == null
              ? desired.operations()
              : planningService
                  .calculateConfigurationChange(
                      new BrokerClusterConfigurationChange(
                          existing,
                          desired.topology(),
                          registrations,
                          observedLeader(eligible, partitionId)))
                  .operations();
      if (!operations.isEmpty()) {
        try {
          children.add(
              BrokerTopologyOperationRequests.freeze(
                  state, changePlan(request, state, partitionId, operations)));
        } catch (final IllegalArgumentException | IllegalStateException error) {
          throw rejection(
              RejectionType.INVALID_STATE,
              FREEZE_FAILED.formatted(
                  error.getMessage(), request.getChangeId(), request.getResourceId(), partitionId));
        }
      }
    }

    final List<MemberChangeOperation> memberOperations = new ArrayList<>();
    targetVoters.stream()
        .filter(memberId -> !current.voterRoster().contains(memberId))
        .forEach(
            memberId ->
                memberOperations.add(operation(MemberChangeOperationType.MEMBER_JOIN, memberId)));
    removed.forEach(
        memberId ->
            memberOperations.add(operation(MemberChangeOperationType.MEMBER_LEAVE, memberId)));
    final boolean noChanges = current.voterRoster().equals(targetVoters) && children.isEmpty();
    if (children.isEmpty()) {
      memberOperations.forEach(
          operation ->
              operation.setStatus(MemberChangeRecord.MemberChangeOperationStatus.COMPLETED));
    }
    final MemberChangeRecord parent =
        request
            .copy()
            .setSourceEpoch(Math.max(0L, current.rosterEpoch()))
            .setSourceVoters(current.voterRoster())
            .setTargetEpoch(Math.max(0L, current.rosterEpoch()) + (noChanges ? 0 : 1))
            .setTargetCount(requestedCount)
            .setTargetVoters(targetVoters)
            .setRemovedMembers(List.copyOf(removed))
            .setOperations(memberOperations)
            .setChildPartitionChangeIds(children.stream().map(ChangePlan::changeId).toList())
            .setStatus(
                children.isEmpty() ? MemberChangeStatus.COMPLETED : MemberChangeStatus.PENDING)
            .setNextOperationIndex(children.isEmpty() ? memberOperations.size() : 0)
            .setOutcome(noChanges ? MemberChangeOutcome.NO_CHANGES : MemberChangeOutcome.ACCEPTED);
    return new MemberChangePlan(
        parent, children.stream().map(BrokerPartitionChangeRecordMapper::fromPlan).toList());
  }

  private static void resize(
      final LinkedHashSet<String> target,
      final int targetSize,
      final List<BrokerRegistration> registrations,
      final Set<String> removals) {
    if (target.size() < targetSize) {
      registrations.stream()
          .filter(registration -> registration.status() == BrokerStatus.ONLINE)
          .map(BrokerRegistration::brokerId)
          .filter(memberId -> !target.contains(memberId))
          .filter(memberId -> !removals.contains(memberId))
          .sorted()
          .limit(targetSize - target.size())
          .forEach(target::add);
    }
    if (target.size() > targetSize) {
      target.stream()
          .sorted(Comparator.reverseOrder())
          .limit(target.size() - targetSize)
          .toList()
          .forEach(target::remove);
    }
  }

  private static ChangePlan changePlan(
      final MemberChangeRecord request,
      final ProcessingState state,
      final int partitionId,
      final List<TopologyOperation> operations) {
    final String planId =
        BrokerClusterChangeIds.createAdminChangeId(
            "member-change", request.getChangeId(), partitionId);
    final long startedAt = request.getRequestedAtEpochMillis();
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(request.getResourceId(), planId, partitionId),
            new ChangePlan.Versions(state.getVersion(), state.getVersion() + 1),
            BrokerClusterChangeIds.scopeOperations(planId, operations),
            operations.stream().anyMatch(op -> op.type() == TopologyOperationType.ADD)
                ? ChangePlan.Purpose.BOOTSTRAP
                : ChangePlan.Purpose.MIGRATION),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        new ChangePlan.Deadline(startedAt, startedAt + CHANGE_TIMEOUT_MILLIS));
  }

  private static MemberChangeOperation operation(
      final MemberChangeOperationType type, final String memberId) {
    return new MemberChangeOperation()
        .setOperationId(type.name().toLowerCase() + "-" + memberId)
        .setType(type)
        .setMemberId(memberId);
  }

  private static Map<String, BrokerRegistration> registrationsById(
      final List<BrokerRegistration> registrations) {
    final Map<String, BrokerRegistration> byId = new LinkedHashMap<>();
    registrations.forEach(registration -> byId.put(registration.brokerId(), registration));
    return byId;
  }

  private static LinkedHashSet<String> normalized(final List<String> values) {
    final LinkedHashSet<String> normalized = new LinkedHashSet<>();
    values.stream()
        .filter(value -> value != null && !value.isBlank())
        .map(String::trim)
        .forEach(normalized::add);
    return normalized;
  }

  private static String observedLeader(
      final List<BrokerRegistration> registrations, final int partitionId) {
    long highestTerm = -1;
    String leader = "";
    for (final BrokerRegistration registration : registrations) {
      for (final BrokerPartitionState partition : registration.partitions()) {
        if (partition.partitionId() == partitionId
            && partition.role() == MemberRole.LEADER
            && partition.term() > highestTerm) {
          highestTerm = partition.term();
          leader = registration.brokerId();
        }
      }
    }
    return leader;
  }

  private static MemberChangeRejection rejection(final RejectionType type, final String reason) {
    return new MemberChangeRejection(type, reason);
  }
}
