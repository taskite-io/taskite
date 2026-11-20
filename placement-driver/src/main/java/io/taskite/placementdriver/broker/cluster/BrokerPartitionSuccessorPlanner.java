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
package io.taskite.placementdriver.broker.cluster;

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.BrokerPartitionConfigurationRecord;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RecoveryFence;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Builds each successor from one complete committed configuration, never from predicted ACKs. */
public final class BrokerPartitionSuccessorPlanner {
  private BrokerPartitionSuccessorPlanner() {}

  public static List<BrokerPartitionMember> verifiedMembers(
      final ChangePlan plan, final BrokerPartitionConfigurationRecord source) {
    if (!plan.clusterId().equals(source.clusterId())
        || plan.partitionId() != source.partitionId()
        || source.leaderId().isBlank()
        || source.leaderTerm() <= 0
        || source.configurationIndex() <= 0
        || source.recoveryEpoch() < 0) {
      throw new IllegalStateException(PlanBlockedReason.INCOMPLETE_SOURCE_CONFIGURATION.name());
    }
    final Set<String> ids = new HashSet<>();
    final List<BrokerPartitionMember> members = source.members();
    if (members.isEmpty()
        || members.stream()
            .anyMatch(
                member ->
                    member.brokerId().isBlank()
                        || !ids.add(member.brokerId())
                        || (member.voter() ? member.priority() <= 0 : member.priority() != 0))
        || members.stream()
            .noneMatch(member -> member.voter() && member.brokerId().equals(source.leaderId()))
        || !new BrokerOperationConfiguration(members, List.of(), 0, new RecoveryFence(0, 0, ""))
            .sourceDigest(plan.clusterId())
            .equals(source.configurationDigest())) {
      throw new IllegalStateException(PlanBlockedReason.INCOMPLETE_SOURCE_CONFIGURATION.name());
    }
    return members;
  }

  public static ChangePlan plan(
      final ProcessingState state,
      final ChangePlan previous,
      final BrokerPartitionConfigurationRecord source) {
    final List<BrokerPartitionMember> committed = verifiedMembers(previous, source);
    if (!previous.execution().successor().equals(source.ownerChangeId())
        || source.executionGeneration() <= previous.execution().generation()
        || source.authorityIndex() <= 0) {
      throw new IllegalStateException(PlanBlockedReason.INVALID_SUCCESSOR_AUTHORITY.name());
    }
    final var registrations =
        state.getBrokerRegistrationState().registrations(previous.clusterId());
    final Set<String> available = BrokerPlanDependencies.available(state, previous.clusterId());
    final int quorum =
        (int) committed.stream().filter(BrokerPartitionMember::voter).count() / 2 + 1;
    if (committed.stream()
            .filter(BrokerPartitionMember::voter)
            .filter(member -> available.contains(member.brokerId()))
            .count()
        < quorum) {
      throw new IllegalStateException(PlanBlockedReason.NO_QUORUM.name());
    }
    final Set<String> eligible = new LinkedHashSet<>();
    registrations.stream()
        .filter(registration -> registration.status() == BrokerStatus.ONLINE)
        .filter(
            registration ->
                !state
                    .getBrokerRegistrationState()
                    .isLearnerExcluded(
                        previous.clusterId(),
                        registration.brokerId(),
                        registration.incarnationId()))
        .map(registration -> registration.brokerId())
        .sorted()
        .forEach(eligible::add);
    final int rf =
        state
            .getBrokerClusterConfigState()
            .findClusterConfig(previous.clusterId())
            .orElseThrow()
            .replicationFactor();
    final Set<String> eligibleVoters =
        eligible.stream()
            .filter(
                id -> !state.getBrokerRegistrationState().isVoterExcluded(previous.clusterId(), id))
            .collect(Collectors.toCollection(LinkedHashSet::new));
    if (eligibleVoters.size() < rf) {
      throw new IllegalStateException(PlanBlockedReason.INSUFFICIENT_CAPACITY.name());
    }
    final var root =
        state
            .getBrokerTopologyChangeState()
            .changePlan(previous.clusterId(), previous.rootChangeId())
            .filter(plan -> plan.partitionId() == previous.partitionId())
            .orElse(previous);
    final var finalConfiguration =
        root.operations().isEmpty() ? null : root.operations().getLast().configuration();
    final boolean rootReachable =
        finalConfiguration != null
            && finalConfiguration.targetMembers().stream()
                    .filter(BrokerPartitionMember::voter)
                    .count()
                == rf
            && finalConfiguration.targetMembers().stream()
                .allMatch(
                    member ->
                        (member.voter() ? eligibleVoters : eligible).contains(member.brokerId()));
    final Set<String> desired = new LinkedHashSet<>();
    if (finalConfiguration != null) {
      finalConfiguration.targetMembers().stream()
          .filter(BrokerPartitionMember::voter)
          .map(BrokerPartitionMember::brokerId)
          .filter(eligibleVoters::contains)
          .forEach(desired::add);
    }
    committed.stream()
        .filter(BrokerPartitionMember::voter)
        .map(BrokerPartitionMember::brokerId)
        .filter(eligibleVoters::contains)
        .forEach(desired::add);
    desired.addAll(eligibleVoters);
    final List<String> voters = desired.stream().limit(rf).toList();
    final String primary =
        rootReachable
            ? finalConfiguration.targetMembers().stream()
                .filter(
                    member ->
                        member.voter() && member.priority() == PartitionTopology.PRIMARY_PRIORITY)
                .map(BrokerPartitionMember::brokerId)
                .findFirst()
                .orElse(voters.getFirst())
            : voters.contains(source.leaderId()) ? source.leaderId() : voters.getFirst();
    final List<String> learners =
        (rootReachable ? finalConfiguration.targetMembers() : committed)
            .stream()
                .filter(member -> !member.voter())
                .map(BrokerPartitionMember::brokerId)
                .filter(eligible::contains)
                .filter(id -> !voters.contains(id))
                .filter(
                    id ->
                        finalConfiguration == null
                            || finalConfiguration.targetMembers().stream()
                                .anyMatch(
                                    member -> !member.voter() && member.brokerId().equals(id)))
                .toList();
    final Map<String, Integer> currentPriorities = new LinkedHashMap<>();
    committed.forEach(member -> currentPriorities.put(member.brokerId(), member.priority()));
    final Map<String, Integer> desiredPriorities = new LinkedHashMap<>();
    voters.forEach(
        id ->
            desiredPriorities.put(
                id,
                id.equals(primary)
                    ? PartitionTopology.PRIMARY_PRIORITY
                    : PartitionTopology.VOTER_PRIORITY));
    learners.forEach(id -> desiredPriorities.put(id, PartitionTopology.LEARNER_PRIORITY));
    if (rootReachable) {
      finalConfiguration
          .targetMembers()
          .forEach(member -> desiredPriorities.put(member.brokerId(), member.priority()));
    }
    final var existing =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(previous.clusterId(), previous.partitionId())
            .orElseThrow();
    final var placement = new PartitionTopology.Placement(rf, voters, primary);
    PartitionTopology actual =
        new PartitionTopology(
            existing.identity(),
            placement,
            new PartitionTopology.Membership(
                committed.stream()
                    .filter(BrokerPartitionMember::voter)
                    .map(BrokerPartitionMember::brokerId)
                    .toList(),
                committed.stream()
                    .filter(member -> !member.voter())
                    .map(BrokerPartitionMember::brokerId)
                    .toList(),
                currentPriorities,
                source.recoveryEpoch()),
            PartitionStatus.ACTIVE,
            Map.of(),
            existing.initializing());
    final var target =
        new PartitionTopology(
            existing.identity(),
            placement,
            new PartitionTopology.Membership(
                voters, learners, desiredPriorities, source.recoveryEpoch()),
            PartitionStatus.ACTIVE,
            Map.of(),
            existing.initializing());
    final var operations =
        new ArrayList<>(
            new BrokerClusterChangePlanningService()
                .calculateTopologyRebalanceToTarget(
                    new BrokerClusterChangePlanningService.BrokerTopologyRebalance(
                        actual, target, source.leaderId()))
                .operations());
    if (operations.isEmpty()) {
      // An explicit no-op verifies the already-converged configuration through the same ACK path.
      operations.add(TopologyOperation.transferLeader(previous.partitionId(), primary));
    }
    final List<TopologyOperation> frozen = new ArrayList<>();
    for (final var operation : operations) {
      final var next = PartitionChangeTransitions.targetMembership(actual, operation);
      frozen.add(
          new TopologyOperation(
              operation.opId(),
              operation.type(),
              operation.targetBroker(),
              operation.priority(),
              new BrokerOperationConfiguration(
                  members(actual),
                  members(next),
                  frozen.isEmpty() ? source.configurationIndex() : 0,
                  new RecoveryFence(source.recoveryEpoch(), source.recoveryEpoch(), ""))));
      actual = next;
    }
    final long now = System.currentTimeMillis();
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(
                previous.clusterId(), previous.execution().successor(), previous.partitionId()),
            new ChangePlan.Versions(state.getVersion(), state.getVersion() + 1),
            frozen,
            previous.purpose()),
        new ChangePlan.Progress(0, BrokerPartitionChangeStatus.PENDING),
        new ChangePlan.Deadline(now, now + Duration.ofMinutes(5).toMillis()),
        new ChangePlan.Execution(
            source.executionGeneration(),
            source.authorityIndex(),
            previous.changeId(),
            "",
            previous.rootChangeId(),
            PlanBlockedReason.NONE,
            0));
  }

  private static List<BrokerPartitionMember> members(final PartitionTopology topology) {
    final List<BrokerPartitionMember> result = new ArrayList<>();
    topology
        .priorities()
        .forEach(
            (id, priority) -> {
              result.add(
                  new BrokerPartitionMember(id, topology.currentVoters().contains(id), priority));
            });
    return List.copyOf(result);
  }
}
