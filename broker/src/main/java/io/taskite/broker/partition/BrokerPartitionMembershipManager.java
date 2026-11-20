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
package io.taskite.broker.partition;

import io.taskite.broker.state.BrokerPartitionOperationManifestStore;
import io.taskite.broker.state.BrokerPartitionRecoveryEpochStore;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.group.PartitionGroupDescriptor;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.runtime.StreamPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Executes local Raft membership changes selected by committed placement-driver plans. */
public final class BrokerPartitionMembershipManager {
  private final BrokerPartitionManager partitions;
  private final Map<Integer, Object> reconfiguringPartitions = new ConcurrentHashMap<>();

  public BrokerPartitionMembershipManager(final BrokerPartitionManager partitions) {
    this.partitions = Objects.requireNonNull(partitions, "partitions");
  }

  /** A committed authority proves that the old JRaft configuration procedure has settled. */
  public void authorityInstalled(final int partitionId) {
    reconfiguringPartitions.remove(partitionId);
  }

  public synchronized ActorFuture<Void> addPartition(final BrokerPartitionOperation request) {
    partitions.confirmAssignment(request.partitionId());
    final List<RaftPeer> peers = peers(request);
    if (partitions.group(request.partitionId()).isPresent()) {
      // ADD cannot force-reset an existing group. Re-delivery is an idempotent no-op.
      return CompletableActorFuture.completed(null);
    }
    final RaftRole role =
        peers.stream()
            .filter(peer -> peer.brokerId().equals(partitions.localBrokerId()))
            .findFirst()
            .map(peer -> peer.voter() ? RaftRole.FOLLOWER : RaftRole.LEARNER)
            .orElse(null);
    if (role == null) {
      return failed(
          new IllegalStateException(
              "Expected to add partition "
                  + request.partitionId()
                  + ", but broker "
                  + partitions.localBrokerId()
                  + " is not a target member"));
    }
    if (role == RaftRole.LEARNER && peers.stream().noneMatch(RaftPeer::voter)) {
      return failed(
          new IllegalStateException(
              "Expected to add partition "
                  + request.partitionId()
                  + ", but the target membership has no voter"));
    }
    partitions.createPartition(
        new PartitionGroupDescriptor(
            request.partitionId(),
            request.clusterId(),
            0,
            role,
            peers,
            request.expectedRecoveryEpoch()));
    return CompletableActorFuture.completed(null);
  }

  public synchronized ActorFuture<Void> reconfigurePartition(
      final BrokerPartitionOperation request) {
    final Optional<StreamPartition> group = partitions.group(request.partitionId());
    if (group.isEmpty()) {
      return failed(
          new IllegalStateException(
              "Expected to reconfigure partition "
                  + request.partitionId()
                  + ", but no local raft group exists"));
    }
    if (group.get().role() != RaftRole.LEADER) {
      return failed(
          new IllegalStateException(
              "Expected to reconfigure partition "
                  + request.partitionId()
                  + ", but broker "
                  + partitions.localBrokerId()
                  + " is not the leader"));
    }
    final Object slot = new Object();
    if (reconfiguringPartitions.putIfAbsent(request.partitionId(), slot) != null) {
      return failed(
          new IllegalStateException(
              "Expected to reconfigure partition "
                  + request.partitionId()
                  + ", but a membership change is already in flight"));
    }
    final ActorFuture<Void> result;
    try {
      result =
          group
              .get()
              .raftPartition()
              .updateRaftMembership(
                  peers(request), request.authority().generation(), request.authority().owner());
    } catch (final RuntimeException error) {
      reconfiguringPartitions.remove(request.partitionId(), slot);
      throw error;
    }
    result.onComplete(
        (ignored, error) -> reconfiguringPartitions.remove(request.partitionId(), slot));
    return result;
  }

  public synchronized ActorFuture<Void> transferLeadership(final BrokerPartitionOperation request) {
    return partitions
        .group(request.partitionId())
        .map(
            group ->
                group.transferLeadership(
                    request.targetBroker(),
                    request.authority().generation(),
                    request.authority().owner()))
        .orElseGet(
            () ->
                failed(
                    new IllegalStateException(
                        "Expected to transfer leadership for partition "
                            + request.partitionId()
                            + ", but no local raft group exists")));
  }

  /**
   * Performs the fenced recovery reset. This is the only membership path allowed to invoke jraft's
   * reset-peers primitive without an existing leader.
   */
  public synchronized ActorFuture<Void> resetPeers(final BrokerPartitionOperation request) {
    final Optional<StreamPartition> group = partitions.group(request.partitionId());
    if (group.isEmpty()) {
      return failed(
          new IllegalStateException(
              "Expected to reset partition "
                  + request.partitionId()
                  + ", but no local raft group exists"));
    }
    if (request.fencingProofId().isBlank()) {
      return failed(
          new IllegalStateException(
              "Refusing to reset partition " + request.partitionId() + " without a fencing proof"));
    }
    if (!partitions.localBrokerId().equals(request.targetBroker())) {
      return failed(
          new IllegalStateException(
              "Refusing to reset partition "
                  + request.partitionId()
                  + " because broker "
                  + partitions.localBrokerId()
                  + " is not the survivor "
                  + request.targetBroker()));
    }
    final List<RaftPeer> peers = peers(request);
    if (peers.stream()
        .noneMatch(peer -> peer.brokerId().equals(partitions.localBrokerId()) && peer.voter())) {
      return failed(
          new IllegalStateException(
              "Refusing to reset partition "
                  + request.partitionId()
                  + " because the survivor "
                  + partitions.localBrokerId()
                  + " is not in the target voter set"));
    }
    final BrokerPartitionRecoveryEpochStore epochStore =
        new BrokerPartitionRecoveryEpochStore(group.get().raftPartition().storage());
    final String sourceDigest = group.get().raftPartition().appliedConfigurationDigest();
    if (request.targetRecoveryEpoch() == epochStore.current()
        && request.targetConfigurationDigest().equals(sourceDigest)) {
      return CompletableActorFuture.completed(null);
    }
    if (request.expectedRecoveryEpoch() != epochStore.current()
        || request.targetRecoveryEpoch() <= epochStore.current()) {
      return failed(new IllegalStateException("STALE_OPERATION: recovery epoch"));
    }
    final var store =
        new BrokerPartitionOperationManifestStore(group.get().raftPartition().storage());
    if (request.expectedConfigurationDigest().isBlank()
        || !request.expectedConfigurationDigest().equals(sourceDigest)
            && !store.canResume(request, sourceDigest)) {
      return failed(new IllegalStateException("STALE_CONFIGURATION: " + request.opId()));
    }
    final ActorFuture<Void> reset = group.get().resetRaftMembership(peers, sourceDigest);
    final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
    reset.onComplete(
        (ignored, error) -> {
          if (error != null) {
            result.completeExceptionally(error);
            return;
          }
          try {
            epochStore.put(request.targetRecoveryEpoch());
            BrokerPartitionRecoveryGuard.recordEpoch(
                partitions.groupDataDirectory(request.clusterId(), request.partitionId()),
                request.targetRecoveryEpoch());
          } catch (final RuntimeException persistenceError) {
            result.completeExceptionally(persistenceError);
            return;
          }
          result.complete(null);
        });
    return result;
  }

  private List<RaftPeer> peers(final BrokerPartitionOperation request) {
    final List<RaftPeer> peers = new ArrayList<>();
    for (final BrokerPartitionMember member : request.members()) {
      final var endpoint = partitions.topology().brokers().get(member.brokerId());
      if (endpoint == null) {
        throw new IllegalStateException("broker endpoint does not exist: " + member.brokerId());
      }
      peers.add(
          new RaftPeer(
              member.brokerId(),
              endpoint.host(),
              endpoint.raftPort(),
              member.voter(),
              member.priority()));
    }
    return peers;
  }

  private static <T> ActorFuture<T> failed(final Throwable error) {
    return CompletableActorFuture.completedExceptionally(error);
  }
}
