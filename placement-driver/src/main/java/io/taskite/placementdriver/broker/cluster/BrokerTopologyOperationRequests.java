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

import static io.taskite.protocol.record.TopologyOperationType.valueOf;

import io.taskite.placementdriver.broker.registry.BrokerPartitionReports;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.request.BrokerApplyTopologyOperationRequest;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Maps persisted source and target membership to a fenced Broker request. */
public final class BrokerTopologyOperationRequests {
  private BrokerTopologyOperationRequests() {}

  public static ChangePlan freeze(final ProcessingState state, final ChangePlan plan) {
    final var existing =
        state
            .getBrokerPartitionTopologyState()
            .partitionTopology(plan.clusterId(), plan.partitionId());
    PartitionTopology source =
        PartitionChangeTransitions.startPlan(
            existing, plan, existing.map(PartitionTopology::replicationFactor).orElse(1));
    if (!source.currentVoters().isEmpty()
        && plan.operations().stream()
            .anyMatch(
                operation -> operation.type() == TopologyOperationType.RECONFIGURE_PRIORITY)) {
      source = observedPrioritySource(source, state);
    }
    final BrokerOperationConfiguration recoverySource =
        plan.isQuorumRecovery() ? recoverySource(state, plan, source) : null;
    final List<TopologyOperation> operations = new ArrayList<>();
    for (final var operation : plan.operations()) {
      final var target = PartitionChangeTransitions.targetMembership(source, operation);
      final var sourceMembers =
          recoverySource != null ? recoverySource.sourceMembers() : members(source);
      final String sourceDigest =
          new BrokerOperationConfiguration(sourceMembers, List.of(), 0, new RecoveryFence(0, 0, ""))
              .sourceDigest(plan.clusterId());
      final long sourceEpoch = source.recoveryEpoch();
      // Later configuration indexes are allocated by Raft, not predictable by the planner.
      // The full recursively derived source configuration remains an exact fence at every step.
      final long sourceIndex =
          recoverySource != null
              ? recoverySource.expectedMembershipEpoch()
              : operations.isEmpty()
                  ? source.replicaStates().values().stream()
                      .filter(replica -> replica.recoveryEpoch() == sourceEpoch)
                      .filter(
                          replica -> sourceDigest.equals(replica.election().configurationDigest()))
                      .mapToLong(replica -> replica.election().configurationIndex())
                      .max()
                      .orElse(0)
                  : 0;
      operations.add(
          new TopologyOperation(
              operation.opId(),
              operation.type(),
              operation.targetBroker(),
              operation.priority(),
              new BrokerOperationConfiguration(
                  sourceMembers,
                  members(target),
                  sourceIndex,
                  new RecoveryFence(
                      recoverySource != null
                          ? recoverySource.expectedRecoveryEpoch()
                          : source.recoveryEpoch(),
                      target.recoveryEpoch(),
                      operation.type() == TopologyOperationType.RESET_PEERS
                          ? operation.opId()
                          : ""))));
      source = target;
    }
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity(plan.clusterId(), plan.changeId(), plan.partitionId()),
            new ChangePlan.Versions(plan.sourceTopologyVersion(), plan.targetTopologyVersion()),
            operations,
            plan.purpose()),
        new ChangePlan.Progress(plan.nextOperationIndex(), plan.status()),
        new ChangePlan.Deadline(plan.startedAtEpochMillis(), plan.deadlineEpochMillis()),
        plan.execution());
  }

  private static BrokerOperationConfiguration recoverySource(
      final ProcessingState state, final ChangePlan plan, final PartitionTopology source) {
    final String survivor = plan.nextOperation().targetBroker();
    final var registration =
        state
            .getBrokerRegistrationState()
            .registration(new BrokerRegistryKey(plan.clusterId(), survivor))
            .orElseThrow(() -> new IllegalStateException("NO_SURVIVOR"));
    final var report =
        registration.partitions().stream()
            .filter(partition -> partition.partitionId() == plan.partitionId())
            .findFirst()
            .orElse(null);
    // Legacy offline-majority recovery records may not contain election observations. Broker still
    // verifies the exact frozen source; modern reports must match a complete persisted member set.
    if (report == null) {
      return null;
    }
    if (!BrokerPartitionReports.current(registration, report)
        || report.election().configurationDigest().isBlank()
        || report.election().configurationIndex() > report.matchIndex()
        || report.recoveryEpoch() != source.recoveryEpoch()) {
      throw new IllegalStateException("INCOMPLETE_RECOVERY_SOURCE: " + survivor);
    }
    final List<List<BrokerPartitionMember>> candidates = new ArrayList<>();
    candidates.add(members(source));
    final Set<String> visited = new HashSet<>();
    ChangePlan previous =
        state.getBrokerTopologyChangeState().pendingChangePlans(plan.clusterId()).stream()
            .filter(candidate -> candidate.partitionId() == plan.partitionId())
            .findFirst()
            .orElse(null);
    while (previous != null && visited.add(previous.changeId())) {
      for (final var operation : previous.operations()) {
        final var configuration = operation.configuration();
        if (configuration == null) {
          continue;
        }
        if (configuration.expectedRecoveryEpoch() == report.recoveryEpoch()) {
          candidates.add(configuration.sourceMembers());
        }
        if (configuration.targetRecoveryEpoch() == report.recoveryEpoch()) {
          candidates.add(configuration.targetMembers());
        }
      }
      previous =
          state
              .getBrokerTopologyChangeState()
              .changePlan(plan.clusterId(), previous.execution().predecessor())
              .orElse(null);
    }
    for (final var candidate : candidates) {
      final var configuration =
          new BrokerOperationConfiguration(
              candidate,
              List.of(),
              report.election().configurationIndex(),
              new RecoveryFence(report.recoveryEpoch(), report.recoveryEpoch(), ""));
      if (configuration
              .sourceDigest(plan.clusterId())
              .equals(report.election().configurationDigest())
          && candidate.stream()
              .anyMatch(member -> member.voter() && member.brokerId().equals(survivor))) {
        return configuration;
      }
    }
    throw new IllegalStateException("UNVERIFIED_RECOVERY_SOURCE: " + survivor);
  }

  public static BrokerApplyTopologyOperationRequest build(
      final ChangePlan plan, final TopologyOperation operation) {
    final var configuration =
        Optional.ofNullable(operation.configuration())
            .orElseThrow(
                () -> new IllegalStateException("UNFROZEN_OPERATION: " + operation.opId()));
    return new BrokerApplyTopologyOperationRequest(
        new BrokerApplyTopologyOperationRequest.Partition(plan.clusterId(), plan.partitionId()),
        new BrokerApplyTopologyOperationRequest.Operation(
            operation.opId(),
            valueOf(operation.type().name()),
            operation.targetBroker(),
            operation.priority()),
        new BrokerApplyTopologyOperationRequest.Configuration(
            configuration.targetMembers(),
            new RaftConfigurationReference(
                configuration.expectedMembershipEpoch(),
                configuration.sourceDigest(plan.clusterId())),
            new RecoveryFence(
                configuration.expectedRecoveryEpoch(),
                operation.type() == TopologyOperationType.RESET_PEERS
                    ? configuration.targetRecoveryEpoch()
                    : 0,
                configuration.fencingProofId())),
        new BrokerApplyTopologyOperationRequest.Authority(
            plan.execution().generation(), plan.changeId()));
  }

  private static List<BrokerPartitionMember> members(final PartitionTopology topology) {
    final List<BrokerPartitionMember> result = new ArrayList<>();
    topology
        .currentVoters()
        .forEach(
            broker ->
                result.add(
                    new BrokerPartitionMember(
                        broker,
                        true,
                        topology
                            .priorities()
                            .getOrDefault(broker, PartitionTopology.VOTER_PRIORITY))));
    topology.learners().forEach(broker -> result.add(new BrokerPartitionMember(broker, false, 0)));
    return result;
  }

  private static PartitionTopology observedPrioritySource(
      final PartitionTopology source, final ProcessingState state) {
    final var ids = new ArrayList<>(source.currentVoters());
    ids.addAll(source.learners());
    final Map<String, Integer> priorities = new LinkedHashMap<>();
    String observedDigest = null;
    for (final String id : ids) {
      final var replica = source.replicaStates().get(id);
      final var registration =
          state
              .getBrokerRegistrationState()
              .registration(new BrokerRegistryKey(source.clusterId(), id))
              .orElse(null);
      final var report =
          registration == null
              ? null
              : registration.partitions().stream()
                  .filter(partition -> partition.partitionId() == source.partitionId())
                  .filter(partition -> BrokerPartitionReports.current(registration, partition))
                  .findFirst()
                  .orElse(null);
      if (replica == null
          || report == null
          || !replica.election().equals(report.election())
          || replica.priority() != report.priority()
          || replica.recoveryEpoch() != report.recoveryEpoch()
          || replica.lastReportTimeEpochMillis() <= 0
          || replica.recoveryEpoch() != source.recoveryEpoch()
          || replica.election().configurationDigest().isBlank()
          || replica.election().configurationIndex() > replica.matchIndex()) {
        throw new IllegalStateException("INCOMPLETE_SOURCE_CONFIGURATION: " + id);
      }
      if (observedDigest == null) {
        observedDigest = replica.election().configurationDigest();
      } else if (!observedDigest.equals(replica.election().configurationDigest())) {
        throw new IllegalStateException("INCONSISTENT_SOURCE_CONFIGURATION: " + id);
      }
      final boolean voter = source.currentVoters().contains(id);
      if ((voter && replica.priority() <= 0) || (!voter && replica.priority() != 0)) {
        throw new IllegalStateException("INVALID_SOURCE_PRIORITY: " + id);
      }
      priorities.put(id, replica.priority());
    }
    final var observed =
        new PartitionTopology(
            new PartitionTopology.Identity(source.clusterId(), source.partitionId()),
            new PartitionTopology.Placement(
                source.replicationFactor(), source.desiredVoters(), source.desiredPrimary()),
            new PartitionTopology.Membership(
                source.currentVoters(), source.learners(), priorities, source.recoveryEpoch()),
            source.status(),
            source.replicaStates(),
            source.initializing());
    final String reconstructed =
        new BrokerOperationConfiguration(
                members(observed), List.of(), 0, new RecoveryFence(0, 0, ""))
            .sourceDigest(source.clusterId());
    if (!reconstructed.equals(observedDigest)) {
      throw new IllegalStateException("UNVERIFIED_SOURCE_CONFIGURATION");
    }
    return observed;
  }
}
