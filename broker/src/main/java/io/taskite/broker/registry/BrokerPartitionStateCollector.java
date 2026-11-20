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
package io.taskite.broker.registry;

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.partition.BrokerPartitionOperationManifest;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.topology.BrokerPartitionElectionState;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RaftNodeState;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Builds the partition observation snapshot attached to Broker registration and renewal. */
final class BrokerPartitionStateCollector {
  private final BrokerConfig config;
  private final BrokerRuntime runtime;

  BrokerPartitionStateCollector(final BrokerConfig config, final BrokerRuntime runtime) {
    this.config = Objects.requireNonNull(config, "config");
    this.runtime = Objects.requireNonNull(runtime, "runtime");
  }

  List<BrokerPartitionState> collectPartitionStates() {
    return runtime.raftGroups().groups().stream()
        .sorted(Comparator.comparingInt(StreamPartition::partitionId))
        .map(this::partitionState)
        .toList();
  }

  private BrokerPartitionState partitionState(final StreamPartition group) {
    final var assignment =
        Optional.ofNullable(runtime.raftGroups().topology())
            .flatMap(topology -> topology.partition(group.partitionId()));
    final boolean awaitingAssignment =
        runtime.raftGroups().isAwaitingAssignment(group.partitionId());
    final var raft = group.raftPartition();
    final var observation = raft.electionState();
    final var localMember =
        raft.members().stream()
            .filter(member -> member.memberId().equals(config.brokerId()))
            .findFirst();
    final boolean learner =
        localMember.map(member -> member.role() == RaftRole.LEARNER).orElse(false);
    final long matchIndex = group.lastAppliedIndex();
    final long lag = Math.max(0, group.lastIndex() - matchIndex);
    final long targetRecoveryEpoch =
        assignment.map(partition -> partition.recoveryEpoch()).orElse(0L);
    final long localRecoveryEpoch = runtime.raftGroups().localRecoveryEpoch(group.partitionId());
    final String leaderId = group.raftPartition().leaderId();
    final boolean followingLeader = leaderId != null && !leaderId.isBlank() && group.term() > 0;
    final boolean serviceReady =
        !awaitingAssignment
            && localMember.isPresent()
            && followingLeader
            && group.serviceReady()
            && localRecoveryEpoch >= targetRecoveryEpoch
            && observation != null
            && (learner || observation.priority() > 0);
    final boolean ready = serviceReady && lag == 0 && group.businessReady();
    final boolean faulted = group.faulted();
    final BrokerPartitionOperationManifest manifest =
        runtime.partitionChangeExecutor().latestOperation(group.partitionId()).orElse(null);
    final int priority = observation == null ? -1 : observation.priority();
    final var election =
        new BrokerPartitionElectionState(
            new RaftConfigurationReference(
                observation == null ? 0 : observation.configurationIndex(),
                raft.appliedConfigurationDigest()),
            new BrokerPartitionElectionState.Priorities(
                assignment
                    .map(partition -> partition.priorities().getOrDefault(config.brokerId(), -1))
                    .orElse(-1),
                observation == null ? -1 : observation.targetPriority()),
            new BrokerPartitionElectionState.Observation(
                observation == null
                    ? RaftNodeState.UNKNOWN
                    : RaftNodeState.valueOf(observation.state().name()),
                leaderId,
                serviceReady && !faulted,
                System.currentTimeMillis()));
    final BrokerPartitionLifecycle lifecycleState;
    if (faulted) {
      lifecycleState = BrokerPartitionLifecycle.FAULTED;
    } else if (localMember.isEmpty() || observation == null) {
      lifecycleState = BrokerPartitionLifecycle.ABSENT;
    } else if (learner ? !ready : !serviceReady) {
      lifecycleState = BrokerPartitionLifecycle.CATCHING_UP;
    } else {
      lifecycleState = BrokerPartitionLifecycle.ACTIVE;
    }
    return new BrokerPartitionState(
        group.partitionId(),
        new BrokerPartitionState.Status(
            learner ? MemberRole.LEARNER : MemberRole.valueOf(group.role().name()),
            group.term(),
            lifecycleState,
            priority,
            localRecoveryEpoch),
        new BrokerPartitionState.Replication(matchIndex, lag, ready),
        new BrokerPartitionState.Operation(
            manifest == null ? "" : manifest.opId(),
            manifest == null
                ? BrokerTopologyOperationStatus.NONE
                : BrokerTopologyOperationStatus.valueOf(manifest.status().name())),
        election);
  }
}
