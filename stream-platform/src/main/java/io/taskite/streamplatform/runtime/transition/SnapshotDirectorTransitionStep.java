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
package io.taskite.streamplatform.runtime.transition;

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.snapshot.SnapshotDirector;
import java.util.Objects;
import java.util.function.Supplier;

/** Installs a snapshot director for leaders and removes it before every role transition. */
public final class SnapshotDirectorTransitionStep implements PartitionTransitionStep {
  private final Supplier<SnapshotDirector> directorFactory;

  public SnapshotDirectorTransitionStep(final Supplier<SnapshotDirector> directorFactory) {
    this.directorFactory = Objects.requireNonNull(directorFactory, "directorFactory");
  }

  @Override
  public void onNewRaftRole(final PartitionTransitionContext context, final PartitionRole newRole) {
    final SnapshotDirector director = context.getSnapshotDirector();
    if (director != null) {
      director.stopAccepting();
    }
  }

  @Override
  public ActorFuture<Void> prepareTransition(
      final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
    final SnapshotDirector director = context.getSnapshotDirector();
    if (director == null) {
      return CompletableActorFuture.completed(null);
    }
    context.setSnapshotDirector(null);
    director.close();
    return CompletableActorFuture.completed(null);
  }

  @Override
  public ActorFuture<Void> transitionTo(
      final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
    if (targetRole != PartitionRole.LEADER) {
      return CompletableActorFuture.completed(null);
    }
    try {
      final SnapshotDirector director = directorFactory.get();
      director.start();
      context.setSnapshotDirector(director);
      return CompletableActorFuture.completed(null);
    } catch (final Throwable error) {
      return CompletableActorFuture.completedExceptionally(error);
    }
  }

  @Override
  public String getName() {
    return "SnapshotDirector";
  }
}
