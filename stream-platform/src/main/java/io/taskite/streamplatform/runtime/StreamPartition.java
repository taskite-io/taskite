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
package io.taskite.streamplatform.runtime;

import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.command.StreamPartitionCommandDispatcher;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.processing.StreamProcessor;
import io.taskite.streamplatform.runtime.transition.PartitionRole;
import io.taskite.streamplatform.runtime.transition.PartitionTransition;
import io.taskite.streamplatform.runtime.transition.PartitionTransitionContext;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StreamPartition implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamPartition.class);

  private final StreamPartitionConfig config;
  private final JRaftPartition raftPartition;
  private final StreamPartitionRuntime runtime;
  private final Actor lifecycleActor;
  private final PartitionTransitionContext transitionContext;
  private final StreamPartitionCommandDispatcher commandDispatcher;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final CompletableActorFuture<Void> startupFuture = new CompletableActorFuture<>();
  private volatile boolean faulted;
  private volatile boolean started;

  StreamPartition(
      final StreamPartitionConfig config,
      final JRaftPartition raftPartition,
      final StreamPartitionRuntime runtime) {
    this.config = config;
    this.raftPartition = raftPartition;
    this.runtime = runtime;
    lifecycleActor = runtime.actors().lifecycle();
    transitionContext = runtime.processing().transitionContext();
    commandDispatcher = runtime.processing().commandDispatcher();
  }

  void start(final ActorFuture<Void> partitionActorUp, final ActorFuture<Void> lifecycleActorUp) {
    partitionActorUp
        .thenCompose(ignored -> lifecycleActorUp)
        .onComplete(
            (ignored, error) -> {
              if (error != null) {
                failStartup("partition actors", error);
              } else {
                startRaftPartition();
              }
            });
  }

  public ActorFuture<Void> startupFuture() {
    return startupFuture;
  }

  private void startRaftPartition() {
    if (closed.get()) {
      failStartup(
          "raft partition",
          new IllegalStateException("partition " + partitionId() + " closed during startup"));
      return;
    }
    try {
      raftPartition.start();
      RaftRecoveryBarrier.await(raftPartition, lifecycleActor, closed::get)
          .onComplete(
              (ignored, error) -> {
                if (error != null) {
                  failStartup("raft recovery", error);
                } else {
                  started = true;
                  lifecycleActor.run(
                      () -> requestTransition(PartitionRole.fromRaftRole(role()), term()));
                  startupFuture.complete(null);
                }
              });
    } catch (final Throwable error) {
      failStartup("raft partition", error);
    }
  }

  private void failStartup(final String component, final Throwable error) {
    LOG.error("{} failed to start for partition {}", component, partitionId(), error);
    startupFuture.completeExceptionally(error);
  }

  void onCommittedLogstreamRecord() {
    runtime
        .actors()
        .partition()
        .run(
            () -> {
              final StreamProcessor processor = transitionContext.getStreamProcessor();
              if (processor != null) {
                processor.onRecordAvailable();
              }
            });
  }

  void onRoleChange(final RaftRole newRole) {
    lifecycleActor.run(
        () -> {
          if (closed.get() || !started) {
            return;
          }
          requestTransition(PartitionRole.fromRaftRole(newRole), term());
          if (newRole != RaftRole.LEADER) {
            commandDispatcher.onLeadershipLost();
          }
        });
  }

  void onFault(final String reason) {
    faulted = true;
    lifecycleActor.run(
        () -> {
          LOG.error("partition {} marked FAULTED by raft state machine: {}", partitionId(), reason);
          commandDispatcher.onLeadershipLost();
          requestTransition(PartitionRole.INACTIVE, term());
        });
  }

  ActorFuture<Void> transitionToInactive() {
    return requestTransitionAsync(PartitionRole.INACTIVE, term());
  }

  ActorFuture<Void> restoreCurrentRole() {
    if (!started) {
      return CompletableActorFuture.completed(null);
    }
    return requestTransitionAsync(PartitionRole.fromRaftRole(role()), term());
  }

  public int partitionId() {
    return config.partitionId();
  }

  public RaftRole role() {
    return raftPartition.role();
  }

  public long term() {
    return raftPartition.term();
  }

  public long committedIndex() {
    return raftPartition.committedIndex();
  }

  public long lastAppliedIndex() {
    return raftPartition.lastAppliedIndex();
  }

  public long lastIndex() {
    return raftPartition.lastIndex();
  }

  public Path runtimeDirectory() {
    return raftPartition.runtimeDirectory();
  }

  public RaftPartition raftPartition() {
    return raftPartition;
  }

  public boolean faulted() {
    return faulted;
  }

  /** A recovered processor in its current role can run with a transient processing backlog. */
  public boolean serviceReady() {
    return started && !closed.get() && !faulted && runtime.processing().serviceReady(role());
  }

  /** Uses the business event high-water shared with snapshot gating, plus processor recovery. */
  public boolean businessReady() {
    return serviceReady()
        && runtime.appliedPosition().getAsLong() >= raftPartition.committedPosition();
  }

  public ActorFuture<LoggedEvent> submit(final LoggedEvent commandRecord) {
    return commandDispatcher.submit(commandRecord);
  }

  public ActorFuture<Void> updateRaftMembership(final List<RaftPeer> peers) {
    return raftPartition.updateRaftMembership(peers);
  }

  public ActorFuture<Void> addRaftLearners(final List<RaftPeer> peers) {
    return raftPartition.addRaftLearners(peers);
  }

  public ActorFuture<Void> resetRaftMembership(final List<RaftPeer> peers) {
    return raftPartition.resetRaftMembership(peers);
  }

  public ActorFuture<Void> resetRaftMembership(
      final List<RaftPeer> peers, final String authorizedSourceDigest) {
    return raftPartition.resetRaftMembership(peers, authorizedSourceDigest);
  }

  public void observeTopology(final List<RaftPeer> peers) {
    raftPartition.observeTopology(peers);
  }

  public void observePeerIdentities(final List<RaftPeer> peers) {
    raftPartition.observePeerIdentities(peers);
  }

  public ActorFuture<Void> transferLeadership(final String targetBroker) {
    return transferLeadership(targetBroker, 0, "");
  }

  public ActorFuture<Void> transferLeadership(
      final String targetBroker, final long generation, final String owner) {
    return runtime.transferLeadership(config, raftPartition, targetBroker, generation, owner);
  }

  public ActorFuture<Void> takeSnapshotIfLeader() {
    return runtime.takeSnapshotIfLeader(role());
  }

  private void requestTransition(final PartitionRole role, final long term) {
    lifecycleActor.runOnCompletion(
        transitionTo(role, term),
        (ignored, error) -> {
          if (error != null
              && !(error instanceof PartitionTransition.CancelledPartitionTransitionException)) {
            LOG.warn("partition {} transition to {} failed", partitionId(), role, error);
          }
        });
  }

  private ActorFuture<Void> requestTransitionAsync(final PartitionRole role, final long term) {
    return runtime.transitionAsync(() -> transitionTo(role, term));
  }

  private ActorFuture<Void> transitionTo(final PartitionRole role, final long term) {
    final var transition = runtime.processing().transition();
    if (faulted && role != PartitionRole.INACTIVE) {
      return transition.toInactive(term);
    }
    return switch (role) {
      case LEADER -> transition.toLeader(term);
      case FOLLOWER -> transition.toFollower(term);
      case INACTIVE -> transition.toInactive(term);
    };
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    commandDispatcher.close();
    if (started) {
      transitionToInactive().join();
    }
    runtime.processing().logStream().close();
    raftPartition.close();
    runtime.processing().storage().close();
    lifecycleActor.closeAsync().join();
    runtime.actors().partition().closeAsync().join();
    if (config.ownsScheduler()) {
      runtime.actors().scheduler().stop().join();
    }
  }
}
