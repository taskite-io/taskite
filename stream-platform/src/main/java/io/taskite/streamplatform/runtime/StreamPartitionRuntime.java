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

import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.PartitionStorage;
import io.taskite.streamplatform.command.StreamPartitionCommandDispatcher;
import io.taskite.streamplatform.logstream.LogStream;
import io.taskite.streamplatform.processing.StreamProcessorMode;
import io.taskite.streamplatform.runtime.transition.PartitionTransition;
import io.taskite.streamplatform.runtime.transition.PartitionTransitionContext;
import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Resources owned by one partition, with actor and processing responsibilities kept separate. */
record StreamPartitionRuntime(Actors actors, Processing processing, LongSupplier appliedPosition) {
  ActorFuture<Void> transitionAsync(final Supplier<ActorFuture<Void>> transition) {
    return actors.lifecycle().call(transition::get).thenCompose(result -> result);
  }

  ActorFuture<Void> takeSnapshotIfLeader(final RaftRole role) {
    final var director = processing.transitionContext().getSnapshotDirector();
    return role == RaftRole.LEADER && director != null
        ? director.forceSnapshot().thenApply(ignored -> null)
        : CompletableActorFuture.completed(null);
  }

  ActorFuture<Void> transferLeadership(
      final StreamPartitionConfig config,
      final JRaftPartition raft,
      final String target,
      final long generation,
      final String owner) {
    if (config.localMemberId().equals(target)) {
      return raft.transferLeadership(target, generation, owner);
    }
    return actors
        .lifecycle()
        .call(
            () ->
                processing
                    .commandDispatcher()
                    .transferLeadership(
                        () -> raft.transferLeadership(target, generation, owner),
                        Duration.ofSeconds(5)))
        .thenCompose(result -> result);
  }

  record Actors(Actor partition, Actor lifecycle, ActorScheduler scheduler) {}

  record Processing(
      PartitionStorage storage,
      LogStream logStream,
      PartitionTransition transition,
      PartitionTransitionContext transitionContext,
      StreamPartitionCommandDispatcher commandDispatcher) {

    boolean serviceReady(final RaftRole role) {
      final var processor = transitionContext.getStreamProcessor();
      return processor != null
          && processor.isRecovered()
          && (role == RaftRole.LEADER
              ? processor.mode() == StreamProcessorMode.PROCESSING
              : processor.mode() == StreamProcessorMode.REPLAY);
    }
  }
}
