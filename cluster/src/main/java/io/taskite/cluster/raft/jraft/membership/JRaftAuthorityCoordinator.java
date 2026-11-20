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
package io.taskite.cluster.raft.jraft.membership;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import io.taskite.cluster.raft.RaftPartition.CommittedConfiguration;
import io.taskite.cluster.raft.RaftPartition.ExecutionAuthority;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.jraft.log.JRaftLogEntryCodec;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.RaftConfigurationDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Coordinates linear configuration certificates and replicated execution admission. */
public final class JRaftAuthorityCoordinator {
  private final Supplier<Node> nodeSupplier;
  private final LongSupplier termSupplier;
  private final JRaftMembershipManager membership;
  private final JRaftMemberView members;
  private final JRaftExecutionAuthority authority;
  private final AtomicBoolean installing = new AtomicBoolean();

  public JRaftAuthorityCoordinator(
      final Supplier<Node> nodeSupplier,
      final LongSupplier termSupplier,
      final JRaftMembershipManager membership,
      final JRaftMemberView members,
      final JRaftExecutionAuthority authority) {
    this.nodeSupplier = nodeSupplier;
    this.termSupplier = termSupplier;
    this.membership = membership;
    this.members = members;
    this.authority = authority;
  }

  public ExecutionAuthority current() {
    final var value = authority.current();
    return new ExecutionAuthority(value.generation(), value.owner(), value.index());
  }

  public boolean authorized(final long generation, final String owner) {
    final Node source = nodeSupplier.get();
    return source != null
        && source.isLeader()
        && source.getLastAppliedLogIndex() >= source.getLastCommittedIndex()
        && authority.permits(generation, owner);
  }

  public ActorFuture<CommittedConfiguration> readConfiguration() {
    final var result = new CompletableActorFuture<CommittedConfiguration>();
    final Node source = nodeSupplier.get();
    if (source == null || !source.isLeader()) {
      return CompletableActorFuture.completedExceptionally(new IllegalStateException("NOT_LEADER"));
    }
    final long term = termSupplier.getAsLong();
    source.readIndex(
        new byte[0],
        new ReadIndexClosure() {
          @Override
          public void run(final Status status, final long index, final byte[] context) {
            if (!status.isOk()) {
              final String reason =
                  status.getRaftError() == RaftError.EAGAIN
                      ? "CONFIGURATION_UNSETTLED"
                      : status.getRaftError() == RaftError.EPERM
                          ? "NOT_LEADER"
                          : status.getRaftError() == RaftError.ETIMEDOUT
                              ? "NO_QUORUM"
                              : "CONFIGURATION_READ_UNAVAILABLE";
              result.completeExceptionally(new IllegalStateException(reason + ": " + status));
              return;
            }
            // A single-voter closure runs under NodeImpl's read lock. Leave its call stack
            // before taking the write lock needed to capture the complete applied certificate.
            CompletableFuture.runAsync(() -> capture(source, term, index, result));
          }
        });
    CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS)
        .execute(
            () -> {
              if (!result.isDone()) {
                result.completeExceptionally(
                    new IllegalStateException("CONFIGURATION_READ_TIMEOUT"));
              }
            });
    return result;
  }

  private void capture(
      final Node source,
      final long term,
      final long index,
      final CompletableActorFuture<CommittedConfiguration> result) {
    try {
      synchronized (membership) {
        TaskiteElectionLifecycle.withNodeLock(
            (NodeImpl) source,
            () -> {
              if (source != nodeSupplier.get()
                  || !source.isLeader()
                  || termSupplier.getAsLong() != term
                  || source.getLastAppliedLogIndex() < index
                  || !TaskiteElectionLifecycle.configurationSettled((NodeImpl) source)) {
                result.completeExceptionally(new IllegalStateException("CONFIGURATION_UNSETTLED"));
                return;
              }
              final var observation =
                  TaskiteElectionLifecycle.committedObservation((NodeImpl) source);
              final var certificate = current();
              final var peers = appliedPeers(observation);
              final String digest =
                  RaftConfigurationDigest.of(
                      peers.stream()
                          .map(
                              peer ->
                                  new RaftConfigurationDigest.Member(
                                      peer.brokerId(),
                                      peer.host(),
                                      peer.port(),
                                      peer.voter(),
                                      peer.priority()))
                          .toList());
              if (observation == null
                  || observation.configurationIndex() > source.getLastAppliedLogIndex()
                  || certificate.index() > source.getLastAppliedLogIndex()
                  || !certificate.equals(current())
                  || termSupplier.getAsLong() != term
                  || !source.isLeader()) {
                result.completeExceptionally(new IllegalStateException("CONFIGURATION_UNSETTLED"));
                return;
              }
              result.complete(
                  new CommittedConfiguration(
                      members.leaderId(),
                      term,
                      observation.configurationIndex(),
                      digest,
                      peers,
                      certificate));
            });
      }
    } catch (final RuntimeException error) {
      if (!result.isDone()) {
        result.completeExceptionally(error);
      }
    }
  }

  public ActorFuture<ExecutionAuthority> install(
      final long expectedGeneration, final long generation, final String owner) {
    if (generation != expectedGeneration + 1 || owner == null || owner.isBlank()) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException("INVALID_EXECUTION_AUTHORITY"));
    }
    if (!installing.compareAndSet(false, true)) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("EXECUTION_AUTHORITY_IN_PROGRESS"));
    }
    final var result = new CompletableActorFuture<ExecutionAuthority>();
    result.onComplete((ignored, error) -> installing.set(false));
    CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS)
        .execute(
            () -> {
              synchronized (result) {
                if (!result.isDone()) {
                  result.completeExceptionally(
                      new IllegalStateException("EXECUTION_AUTHORITY_TIMEOUT"));
                }
              }
            });
    membership.quiesce(authority::closeAdmission);
    final Node source = nodeSupplier.get();
    final long term = termSupplier.getAsLong();
    readConfiguration()
        .onComplete(
            (ignored, error) -> {
              if (error != null) {
                result.completeExceptionally(error);
              } else {
                settleAndInstall(
                    source,
                    term,
                    expectedGeneration,
                    generation,
                    owner,
                    System.nanoTime() + 10_000_000_000L,
                    result);
              }
            });
    return result;
  }

  private void settleAndInstall(
      final Node source,
      final long term,
      final long expectedGeneration,
      final long generation,
      final String owner,
      final long deadline,
      final CompletableActorFuture<ExecutionAuthority> result) {
    synchronized (result) {
      if (result.isDone()) {
        return;
      }
      if (source == null
          || source != nodeSupplier.get()
          || !source.isLeader()
          || termSupplier.getAsLong() != term) {
        result.completeExceptionally(new IllegalStateException("NOT_LEADER"));
        return;
      }
      if (System.nanoTime() >= deadline) {
        result.completeExceptionally(new IllegalStateException("CONFIGURATION_UNSETTLED"));
        return;
      }
      if (source.getLastAppliedLogIndex() < source.getLastCommittedIndex()
          || !TaskiteElectionLifecycle.configurationSettled((NodeImpl) source)) {
        CompletableFuture.delayedExecutor(25, TimeUnit.MILLISECONDS)
            .execute(
                () ->
                    settleAndInstall(
                        source, term, expectedGeneration, generation, owner, deadline, result));
        return;
      }
      final var value = authority.current();
      if (value.generation() == generation && value.owner().equals(owner)) {
        authority.openAdmission();
        result.complete(current());
        return;
      }
      if (value.generation() != expectedGeneration) {
        result.completeExceptionally(new IllegalStateException("STALE_EXECUTION_AUTHORITY"));
        return;
      }
      final Task task = new Task();
      task.setData(JRaftLogEntryCodec.executionAuthority(generation, owner));
      task.setExpectedTerm(term);
      task.setDone(
          status -> {
            synchronized (result) {
              if (result.isDone()) {
                return;
              }
              if (!status.isOk()) {
                result.completeExceptionally(
                    new IllegalStateException("authority commit failed: " + status));
              } else {
                // The DATA closure runs before JRaft publishes lastAppliedLogIndex.
                CompletableFuture.delayedExecutor(1, TimeUnit.MILLISECONDS)
                    .execute(
                        () ->
                            settleAndInstall(
                                source,
                                term,
                                expectedGeneration,
                                generation,
                                owner,
                                deadline,
                                result));
              }
            }
          });
      source.apply(task);
    }
  }

  public List<RaftPeer> appliedPeers(final TaskiteElectionLifecycle.Observation observation) {
    if (observation == null) {
      return List.of();
    }
    final var peers = new ArrayList<RaftPeer>();
    observation
        .configuration()
        .listPeers()
        .forEach(
            peer ->
                peers.add(
                    new RaftPeer(
                        members.memberIdOf(peer),
                        peer.getIp(),
                        peer.getPort(),
                        true,
                        peer.getPriority())));
    observation
        .configuration()
        .listLearners()
        .forEach(
            peer ->
                peers.add(
                    new RaftPeer(
                        members.memberIdOf(peer), peer.getIp(), peer.getPort(), false, 0)));
    return List.copyOf(peers);
  }
}
