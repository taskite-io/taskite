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

import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.SchedulingHints;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.processing.StreamProcessor;
import java.util.Objects;
import java.util.function.Function;

/** Installs/removes the {@link StreamProcessor} apply loop for a partition role. */
public final class StreamProcessorTransitionStep implements PartitionTransitionStep {

  private final Function<PartitionRole, StreamProcessor> processorFactory;
  private final ActorScheduler scheduler;

  public StreamProcessorTransitionStep(
      final Function<PartitionRole, StreamProcessor> processorFactory,
      final ActorScheduler scheduler) {
    this.processorFactory = Objects.requireNonNull(processorFactory, "processorFactory");
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
  }

  @Override
  public ActorFuture<Void> prepareTransition(
      final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
    final StreamProcessor current = context.getStreamProcessor();
    if (current == null) {
      return CompletableActorFuture.completed(null);
    }
    context.setStreamProcessor(null);
    final ActorFuture<Void> closed = new CompletableActorFuture<>();
    current.closeAsync().onComplete((ok, error) -> closed.complete(null));
    return closed;
  }

  @Override
  public ActorFuture<Void> transitionTo(
      final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
    if (targetRole == PartitionRole.INACTIVE) {
      return CompletableActorFuture.completed(null);
    }
    final StreamProcessor processor = processorFactory.apply(targetRole);
    final ActorFuture<Void> installed = new CompletableActorFuture<>();
    scheduler
        .submitActor(processor, SchedulingHints.ioBound())
        .onComplete(
            (started, error) -> {
              if (error != null) {
                installed.completeExceptionally(error);
                return;
              }
              context.setStreamProcessor(processor);
              installed.complete(null);
            });
    return installed;
  }

  @Override
  public String getName() {
    return "StreamProcessor";
  }
}
