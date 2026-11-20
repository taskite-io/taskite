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

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns leader-only membership changes, leadership transfer, and fenced reset retries. */
public final class JRaftMembershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(JRaftMembershipManager.class);
  private static final int RESET_PEERS_RETRIES = 20;
  private static final long RESET_PEERS_RETRY_DELAY_MILLIS = 100;

  private final String groupId;
  private final PeerId serverId;
  private final Supplier<Node> nodeSupplier;
  private final JRaftMemberView memberView;
  private final JRaftPeerMapper peerMapper;
  private final JRaftConfigurationExecutor configurationExecutor;

  public JRaftMembershipManager(
      final String groupId,
      final PeerId serverId,
      final Supplier<Node> nodeSupplier,
      final JRaftMemberView memberView) {
    this.groupId = groupId;
    this.serverId = serverId;
    this.nodeSupplier = nodeSupplier;
    this.memberView = memberView;
    peerMapper = memberView.peerMapper();
    configurationExecutor = new JRaftConfigurationExecutor(groupId, nodeSupplier, this);
  }

  public ActorFuture<Void> updateMembership(final List<RaftPeer> peers) {
    return updateMembership(peers, () -> true);
  }

  /** Each internal stage rechecks the authority while sharing the admission lock. */
  public synchronized ActorFuture<Void> updateMembership(
      final List<RaftPeer> peers, final BooleanSupplier authorized) {
    memberView.observePeerIdentities(peers);
    final Node node = nodeSupplier.get();
    if (node == null) {
      return notStarted();
    }
    if (!node.isLeader()) {
      return notLeader();
    }
    try {
      if (!authorized.getAsBoolean()) {
        return superseded();
      }
      return reconfigureAsLeader(
          node, peerMapper.configuration(peers, RaftPeer::priority), authorized);
    } catch (final IllegalStateException lostLeadership) {
      return CompletableActorFuture.completedExceptionally(lostLeadership);
    } catch (final RuntimeException error) {
      LOG.warn("Membership update on {} was rejected by jraft", groupId, error);
      return CompletableActorFuture.completedExceptionally(error);
    }
  }

  public synchronized void quiesce(final Runnable closeAdmission) {
    closeAdmission.run();
  }

  /** Admission may add learners, but must never replace a concurrently changed voter roster. */
  public ActorFuture<Void> admitLearners(final List<RaftPeer> peers) {
    return admitLearners(peers, () -> true);
  }

  public synchronized ActorFuture<Void> admitLearners(
      final List<RaftPeer> peers, final BooleanSupplier authorized) {
    if (peers.stream().anyMatch(RaftPeer::voter)) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException("learner admission must not contain voters"));
    }
    memberView.observePeerIdentities(peers);
    final Node node = nodeSupplier.get();
    if (node == null) {
      return notStarted();
    }
    return configure(
        node,
        "admitLearners",
        done -> {
          if (!authorized.getAsBoolean()) {
            done.run(new Status(RaftError.EPERM, "PARTITION_OPERATION_SUPERSEDED"));
            return;
          }
          final Set<PeerId> voters = new HashSet<>(node.listPeers());
          final Set<PeerId> learners = new HashSet<>(node.listLearners());
          final List<PeerId> additions =
              peers.stream()
                  .map(peerMapper::peerId)
                  .filter(peer -> !voters.contains(peer) && !learners.contains(peer))
                  .toList();
          if (additions.isEmpty()) {
            done.run(Status.OK());
          } else {
            node.addLearners(additions, done);
          }
        });
  }

  public ActorFuture<Void> resetMembership(final List<RaftPeer> peers) {
    return resetMembership(peers, null);
  }

  public ActorFuture<Void> resetMembership(
      final List<RaftPeer> peers, final TaskiteElectionLifecycle.Observation source) {
    memberView.observePeerIdentities(peers);
    final Node node = nodeSupplier.get();
    if (node == null) {
      return notStarted();
    }
    final Configuration target = peerMapper.configuration(peers, RaftPeer::priority);
    if (target.getPeers().stream().noneMatch(serverId::equals)) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException(
              "refusing to reset partition "
                  + groupId
                  + " because the local node is not a target voter"));
    }
    final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
    resetMembership(node, source, target, RESET_PEERS_RETRIES, result);
    return result;
  }

  public ActorFuture<Void> transferLeadership(final String targetMemberId) {
    return transferLeadership(targetMemberId, () -> true);
  }

  public synchronized ActorFuture<Void> transferLeadership(
      final String targetMemberId, final BooleanSupplier authorized) {
    if (!authorized.getAsBoolean()) {
      return superseded();
    }
    final Node node = nodeSupplier.get();
    if (node == null) {
      return notStarted();
    }
    if (!node.isLeader()) {
      return notLeader();
    }
    final PeerId target =
        memberView.currentConfiguration(node).listPeers().stream()
            .filter(peer -> memberView.memberIdOf(peer).equals(targetMemberId))
            .findFirst()
            .orElse(null);
    if (target == null) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException(
              "Expected to transfer leadership to voter `"
                  + targetMemberId
                  + "`, but it is not configured for "
                  + groupId));
    }
    try {
      final Status status = node.transferLeadershipTo(target);
      return status.isOk()
          ? CompletableActorFuture.completed(null)
          : CompletableActorFuture.completedExceptionally(
              new IllegalStateException("transferLeadership failed: " + status));
    } catch (final RuntimeException error) {
      return CompletableActorFuture.completedExceptionally(error);
    }
  }

  private void resetMembership(
      final Node node,
      final TaskiteElectionLifecycle.Observation source,
      final Configuration target,
      final int retriesRemaining,
      final CompletableActorFuture<Void> result) {
    final Status status;
    try {
      status =
          source == null
              ? node.resetPeers(target)
              : TaskiteElectionLifecycle.resetConfiguration((NodeImpl) node, source, target);
    } catch (final RuntimeException error) {
      result.completeExceptionally(error);
      return;
    }
    if (status.isOk()) {
      result.complete(null);
      return;
    }
    if (status.getRaftError() != RaftError.EBUSY || retriesRemaining == 0) {
      result.completeExceptionally(new IllegalStateException("resetPeers failed: " + status));
      return;
    }
    CompletableFuture.delayedExecutor(RESET_PEERS_RETRY_DELAY_MILLIS, TimeUnit.MILLISECONDS)
        .execute(() -> resetMembership(node, source, target, retriesRemaining - 1, result));
  }

  private ActorFuture<Void> reconfigureAsLeader(
      final Node node, final Configuration target, final BooleanSupplier authorized) {
    final List<PeerId> currentVoterList = node.listPeers();
    final Set<PeerId> currentVoters = new HashSet<>(currentVoterList);
    final Set<PeerId> currentLearners = new HashSet<>(node.listLearners());
    final List<PeerId> targetVoterList = new ArrayList<>(target.getPeers());
    final LinkedHashSet<PeerId> targetVoters = new LinkedHashSet<>(targetVoterList);
    final LinkedHashSet<PeerId> targetLearners = new LinkedHashSet<>(target.getLearners());
    final boolean voterMembershipChanged = !currentVoters.equals(targetVoters);
    final boolean voterPrioritiesChanged = !prioritiesMatch(currentVoterList, targetVoterList);
    if (!voterMembershipChanged && voterPrioritiesChanged && targetVoterList.size() == 1) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException(
              "Single-voter priority-only changes are not supported by JRaft 1.4.1"));
    }
    if (!voterMembershipChanged
        && !voterPrioritiesChanged
        && currentLearners.equals(targetLearners)) {
      return CompletableActorFuture.completed(null);
    }

    final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
    final List<PeerId> learnersToRemove =
        currentLearners.stream().filter(peer -> !targetLearners.contains(peer)).toList();
    // Keep standby replication in the voter-change entry. Newly demoted voters can only join as
    // learners after that entry settles, since the old joint configuration still lists them as
    // voters.
    final List<PeerId> retainedLearners =
        targetLearners.stream().filter(currentLearners::contains).toList();
    final List<PeerId> learnersToAdd =
        targetLearners.stream().filter(peer -> !currentLearners.contains(peer)).toList();
    final boolean votersChanged = voterMembershipChanged || voterPrioritiesChanged;
    final List<PeerId> votersToInstall =
        forceConfigurationChangeWhenOnlyPriorityChanged(
            currentVoterList, targetVoterList, voterMembershipChanged, voterPrioritiesChanged);
    afterRemoveLearners(
        node,
        learnersToRemove,
        result,
        authorized,
        () -> {
          if (votersChanged) {
            afterChangePeers(
                node,
                new Configuration(votersToInstall, retainedLearners),
                learnersToRemove,
                result,
                authorized,
                () -> addLearnersThenComplete(node, learnersToAdd, result, authorized));
          } else {
            addLearnersThenComplete(node, learnersToAdd, result, authorized);
          }
        });
    return result;
  }

  private void afterRemoveLearners(
      final Node node,
      final List<PeerId> learners,
      final CompletableActorFuture<Void> result,
      final BooleanSupplier authorized,
      final Runnable onSuccess) {
    if (learners.isEmpty()) {
      onSuccess.run();
      return;
    }
    configure(node, "removeLearners", done -> node.removeLearners(learners, done), authorized)
        .onComplete(
            (ignored, error) -> {
              if (error == null) {
                onSuccess.run();
              } else {
                result.completeExceptionally(error);
              }
            });
  }

  private void afterChangePeers(
      final Node node,
      final Configuration configuration,
      final List<PeerId> removedLearners,
      final CompletableActorFuture<Void> result,
      final BooleanSupplier authorized,
      final Runnable onSuccess) {
    if (configuration.isEmpty()) {
      result.completeExceptionally(
          new IllegalStateException(
              "refusing to reconfigure partition " + groupId + " to an empty voter set"));
      return;
    }
    configure(node, "changePeers", done -> node.changePeers(configuration, done), authorized)
        .onComplete(
            (ignored, error) -> {
              if (error == null) {
                onSuccess.run();
              } else {
                restoreLearnersAfterFailedChange(node, removedLearners, error, result, authorized);
              }
            });
  }

  private void restoreLearnersAfterFailedChange(
      final Node node,
      final List<PeerId> removedLearners,
      final Throwable changeError,
      final CompletableActorFuture<Void> result,
      final BooleanSupplier authorized) {
    final List<PeerId> learnersToRestore;
    try {
      final Set<PeerId> currentVoters = new HashSet<>(node.listPeers());
      final Set<PeerId> currentLearners = new HashSet<>(node.listLearners());
      learnersToRestore =
          removedLearners.stream()
              .filter(peer -> !currentVoters.contains(peer) && !currentLearners.contains(peer))
              .toList();
    } catch (final RuntimeException inspectionError) {
      changeError.addSuppressed(inspectionError);
      result.completeExceptionally(changeError);
      return;
    }
    if (learnersToRestore.isEmpty()) {
      result.completeExceptionally(changeError);
      return;
    }
    try {
      configure(
              node,
              "restoreLearners",
              done -> node.addLearners(learnersToRestore, done),
              authorized)
          .onComplete(
              (ignored, rollbackError) -> {
                if (rollbackError != null) {
                  changeError.addSuppressed(rollbackError);
                }
                result.completeExceptionally(changeError);
              });
    } catch (final RuntimeException rollbackError) {
      changeError.addSuppressed(rollbackError);
      result.completeExceptionally(changeError);
    }
  }

  private void addLearnersThenComplete(
      final Node node,
      final List<PeerId> learners,
      final CompletableActorFuture<Void> result,
      final BooleanSupplier authorized) {
    configure(
            node,
            "addLearners",
            done -> {
              if (learners.isEmpty()) {
                done.run(Status.OK());
              } else {
                node.addLearners(learners, done);
              }
            },
            authorized)
        .onComplete(
            (ignored, error) -> {
              if (error == null) {
                result.complete(null);
              } else {
                result.completeExceptionally(error);
              }
            });
  }

  private ActorFuture<Void> configure(
      final Node node, final String operation, final Consumer<Closure> command) {
    return configure(node, operation, command, () -> true);
  }

  private ActorFuture<Void> configure(
      final Node node,
      final String operation,
      final Consumer<Closure> command,
      final BooleanSupplier authorized) {
    return configurationExecutor.configure(node, operation, command, authorized);
  }

  private static boolean prioritiesMatch(final List<PeerId> current, final List<PeerId> target) {
    if (current.size() != target.size()) {
      return false;
    }
    for (final PeerId expected : target) {
      final PeerId actual = current.stream().filter(expected::equals).findFirst().orElse(null);
      if (actual == null || actual.getPriority() != expected.getPriority()) {
        return false;
      }
    }
    return true;
  }

  private static List<PeerId> forceConfigurationChangeWhenOnlyPriorityChanged(
      final List<PeerId> current,
      final List<PeerId> target,
      final boolean membershipChanged,
      final boolean prioritiesChanged) {
    final List<PeerId> result = new ArrayList<>(target);
    if (!membershipChanged && prioritiesChanged && result.size() > 1 && current.equals(result)) {
      Collections.rotate(result, 1);
    }
    return result;
  }

  private <T> ActorFuture<T> notStarted() {
    return CompletableActorFuture.completedExceptionally(
        new IllegalStateException("partition not started: " + groupId));
  }

  private <T> ActorFuture<T> superseded() {
    return CompletableActorFuture.completedExceptionally(
        new IllegalStateException("EXECUTION_SUPERSEDED"));
  }

  private <T> ActorFuture<T> notLeader() {
    return CompletableActorFuture.completedExceptionally(
        new IllegalStateException("NOT_LEADER: " + groupId));
  }
}
