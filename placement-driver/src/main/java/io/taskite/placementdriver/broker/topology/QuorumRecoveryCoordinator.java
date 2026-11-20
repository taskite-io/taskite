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
package io.taskite.placementdriver.broker.topology;

import io.taskite.placementdriver.broker.cluster.TopologyOperation;
import io.taskite.placementdriver.broker.registry.BrokerPartitionReports;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RaftNodeState;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Owns disaster-recovery for partitions that have lost quorum (RF hard constraints M5/Q3/Q4/Q6).
 *
 * <p>Losing quorum is NOT repaired by the ordinary reconciliation chain: per constraint Q3, an RF=2
 * partition that loses one voter defaults to NO_QUORUM fast-fail rather than an automatic {@code
 * resetPeers}, and per M5 a forced membership reset must go through an explicit, audited,
 * epoch-fenced entry — never by reusing {@code ADD}/rebalance.
 *
 * <p>This class is the seam for that fenced flow. In the current Pass it is a <b>null object</b>:
 * automatic quorum recovery is disabled ({@link #autoQuorumRecoveryEnabled} defaults to {@code
 * false}), so {@link #planRecovery} always returns empty and a lost-quorum partition simply stays
 * unavailable until the original voter returns or an operator triggers the audited recovery
 * endpoint (wired in a later Pass). Keeping the seam lets the reconciler drop its old, unsafe
 * auto-{@code ADD} branch without changing observable behavior.
 */
public final class QuorumRecoveryCoordinator {

  private final boolean autoQuorumRecoveryEnabled;

  public QuorumRecoveryCoordinator() {
    this(false);
  }

  public QuorumRecoveryCoordinator(final boolean autoQuorumRecoveryEnabled) {
    this.autoQuorumRecoveryEnabled = autoQuorumRecoveryEnabled;
  }

  /** Whether a partition has fewer available voters than its quorum requires. */
  public static boolean hasLostQuorum(
      final PartitionTopology topology, final Map<String, BrokerRegistration> byBrokerId) {
    if (topology.currentVoters().isEmpty()) {
      return false;
    }
    final long availableVoters =
        topology.currentVoters().stream()
            .map(byBrokerId::get)
            .filter(Objects::nonNull)
            .filter(registration -> registration.status() != BrokerStatus.OFFLINE)
            .count();
    final int quorum = topology.currentVoters().size() / 2 + 1;
    if (availableVoters < quorum) {
      return true;
    }
    // Registration proves process availability, not that its persisted Raft group can vote with
    // the other replicas. Restarted empty groups may all be ONLINE with incompatible membership.
    // Missing/stale evidence and a transient election within one configuration are insufficient.
    final Map<String, Integer> compatibleVoters = new HashMap<>();
    int persistedVoters = 0;
    for (final String voter : topology.currentVoters()) {
      final BrokerRegistration registration = byBrokerId.get(voter);
      if (registration == null || registration.status() == BrokerStatus.OFFLINE) {
        continue;
      }
      final BrokerPartitionState report =
          registration.partitions().stream()
              .filter(partition -> partition.partitionId() == topology.partitionId())
              .filter(partition -> BrokerPartitionReports.current(registration, partition))
              .findFirst()
              .orElse(null);
      if (report == null
          || report.election().configurationDigest().isBlank()
          || report.election().configurationIndex() > report.matchIndex()
          || report.election().nodeState() == RaftNodeState.UNKNOWN
          || report.role() == MemberRole.LEADER
          || report.election().nodeState() == RaftNodeState.STATE_LEADER
          || report.election().serviceReady()) {
        return false;
      }
      final String configuration =
          report.recoveryEpoch() + ":" + report.election().configurationDigest();
      if (compatibleVoters.merge(configuration, 1, Integer::sum) >= quorum) {
        return false;
      }
      if (report.matchIndex() > 0
          || report.election().configurationIndex() > 0
          || report.term() > 0) {
        persistedVoters++;
      }
    }
    // Different priority/configuration digests alone do not establish data loss. Only admit this
    // online case when a majority no longer has any persisted term, log or configuration index.
    return persistedVoters < quorum;
  }

  /**
   * Returns the operations to recover a partition that has lost quorum, or empty when automatic
   * recovery is disabled (the safe default). Even when disabled the audited operator endpoint calls
   * {@link #planReset} directly, so a lost-quorum partition fast-fails (Q3) under normal
   * reconciliation and only resets through the explicit, fenced path (M5/Q4).
   */
  public Optional<List<TopologyOperation>> planRecovery(
      final PartitionTopology topology, final Map<String, BrokerRegistration> byBrokerId) {
    if (!autoQuorumRecoveryEnabled) {
      return Optional.empty();
    }
    return chooseSurvivor(topology, byBrokerId)
        .map(survivor -> planReset(topology.partitionId(), survivor, topology, byBrokerId));
  }

  /**
   * Builds a fenced single-operation {@code RESET_PEERS} plan that collapses the partition onto
   * {@code survivor}. The recovery epoch is carried in the operation priority (Q6); the caller
   * supplies a globally-monotonic value so a later reset always supersedes an earlier one.
   */
  public static List<TopologyOperation> planReset(
      final int partitionId, final String survivor, final int recoveryEpoch) {
    return List.of(TopologyOperation.resetPeers(partitionId, survivor, recoveryEpoch));
  }

  private List<TopologyOperation> planReset(
      final int partitionId,
      final String survivor,
      final PartitionTopology topology,
      final Map<String, BrokerRegistration> byBrokerId) {
    // The auto path (disabled by default) has no external epoch source, so it reuses the current
    // voter count as a coarse, monotonic-per-recovery placeholder; the audited endpoint always
    // supplies the PD's global version instead.
    return planReset(partitionId, survivor, Math.max(1, topology.currentVoters().size()));
  }

  /**
   * Picks the survivor a fenced reset should collapse onto: an online current voter, preferring the
   * one with the highest observed match index (most complete log) to minimize data loss.
   */
  public static Optional<String> chooseSurvivor(
      final PartitionTopology topology, final Map<String, BrokerRegistration> byBrokerId) {
    return topology.currentVoters().stream()
        .filter(
            brokerId -> {
              final BrokerRegistration registration = byBrokerId.get(brokerId);
              return registration != null && registration.status() == BrokerStatus.ONLINE;
            })
        .max(
            Comparator.comparingLong(
                brokerId -> {
                  final var state = topology.replicaStates().get(brokerId);
                  return state == null ? -1L : state.matchIndex();
                }));
  }
}
