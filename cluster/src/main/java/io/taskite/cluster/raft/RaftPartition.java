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
package io.taskite.cluster.raft;

import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import io.taskite.cluster.raft.log.RaftLogListener;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The engine-agnostic partition surface the business layer (stream-platform / broker /
 * placement-driver) depends on, so it can bind to either the in-house {@code RaftPartition} or the
 * SOFAJRaft-backed {@code io.taskite.cluster.raft.jraft.JRaftPartition} interchangeably (migration
 * "门面 不变、内部换心"). It captures only the read/append/subscribe/snapshot/storage seams the callers
 * actually use; engine-specific lifecycle and RPC surfaces stay off this interface.
 */
public interface RaftPartition {

  // --- identity / role -------------------------------------------------------------------------

  int partitionId();

  RaftRole role();

  long term();

  String leaderId();

  /**
   * The raft group's members as the committed configuration defines them — each member's id and
   * role. Used by the placement-driver's {@code /members} view to report the raft-consistent
   * membership every node agrees on. Per-node liveness is intentionally not exposed (see {@link
   * RaftMember}).
   */
  List<RaftMember> members();

  /** Stable applied membership, including endpoint and priority, for operation recovery fencing. */
  default List<RaftPeer> appliedPeers() {
    return List.of();
  }

  record ElectionState(
      long configurationIndex, int priority, int targetPriority, RaftNodeState state, long term) {}

  default ElectionState electionState() {
    return null;
  }

  default String appliedConfigurationDigest() {
    return "";
  }

  record ExecutionAuthority(long generation, String owner, long index) {}

  record CommittedConfiguration(
      String leaderId,
      long term,
      long configurationIndex,
      String digest,
      List<RaftPeer> peers,
      ExecutionAuthority authority) {}

  default ActorFuture<CommittedConfiguration> readCommittedConfiguration() {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("linearizable configuration read is unsupported"));
  }

  default ExecutionAuthority executionAuthority() {
    return new ExecutionAuthority(0, "", 0);
  }

  default boolean executionAuthorized(final long generation, final String owner) {
    return generation == 0;
  }

  default ActorFuture<ExecutionAuthority> installExecutionAuthority(
      final long expectedGeneration, final long generation, final String owner) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("partition execution authority is unsupported"));
  }

  default ActorFuture<Void> updateRaftMembership(
      final List<RaftPeer> peers, final long generation, final String owner) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("authorized membership update is unsupported"));
  }

  default ActorFuture<Void> transferLeadership(
      final String targetMemberId, final long generation, final String owner) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("authorized leader transfer is unsupported"));
  }

  // --- commit progress -------------------------------------------------------------------------

  long committedIndex();

  long committedPosition();

  /** Highest application position covered by the current business-state checkpoint. */
  default long snapshotPosition() {
    return 0L;
  }

  long lastAppliedIndex();

  long lastIndex();

  // --- append & read ---------------------------------------------------------------------------

  /** Appends one batch entry spanning {@code [lowestPosition, highestPosition]} (leader only). */
  ActorFuture<PersistedRaftRecord> append(long lowestPosition, long highestPosition, byte[] data);

  /** A forward, sequential cursor over the committed log, expanded per-position by the reader. */
  RaftJournalReader openLogReader();

  // --- subscriptions ---------------------------------------------------------------------------

  /** Subscribes a listener notified as committed entries become available to apply. */
  void subscribe(RaftLogListener listener);

  /** Subscribes a listener notified with the new {@link RaftRole} after each role transition. */
  void addRoleChangeListener(Consumer<RaftRole> listener);

  /** Subscribes a listener notified with the observed leader's id on each leader change. */
  void addLeaderChangeListener(Consumer<String> listener);

  // --- state / storage -------------------------------------------------------------------------

  /** The business RocksDB store this partition writes to. */
  PartitionStorage storage();

  /** The partition's on-disk runtime directory. */
  Path runtimeDirectory();

  /**
   * Registers the business layer's durable applied-position high-water source so log compaction
   * never deletes entries carrying positions the layer has not yet applied and checkpointed.
   */
  void setAppliedPositionSupplier(LongSupplier appliedPositionSupplier);
}
