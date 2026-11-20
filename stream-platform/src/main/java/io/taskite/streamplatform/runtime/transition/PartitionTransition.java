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

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Serializes role transitions and runs prepare in reverse before installing steps in order. */
public final class PartitionTransition {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionTransition.class);

  private final List<PartitionTransitionStep> steps;
  private ConcurrencyControl concurrencyControl;
  private PartitionTransitionContext context;

  private TransitionProcess currentTransition;
  private ActorFuture<Void> currentTransitionFuture;

  public PartitionTransition(final List<PartitionTransitionStep> steps) {
    this.steps = List.copyOf(Objects.requireNonNull(steps, "steps"));
  }

  public ActorFuture<Void> toFollower(final long currentTerm) {
    return transitionTo(currentTerm, PartitionRole.FOLLOWER);
  }

  public ActorFuture<Void> toLeader(final long currentTerm) {
    return transitionTo(currentTerm, PartitionRole.LEADER);
  }

  public ActorFuture<Void> toInactive(final long term) {
    return transitionTo(term, PartitionRole.INACTIVE);
  }

  public void setConcurrencyControl(final ConcurrencyControl concurrencyControl) {
    this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
  }

  public void updateTransitionContext(final PartitionTransitionContext transitionContext) {
    this.context = Objects.requireNonNull(transitionContext, "transitionContext");
  }

  private ActorFuture<Void> transitionTo(final long term, final PartitionRole role) {
    LOG.info("Transition to {} on term {} requested", role, term);
    steps.forEach(step -> step.onNewRaftRole(context, role));

    final ActorFuture<Void> nextTransitionFuture = concurrencyControl.createFuture();
    concurrencyControl.run(
        () -> {
          final TransitionProcess nextTransition =
              new TransitionProcess(steps, concurrencyControl, context, term, role);
          nextTransitionFuture.onComplete(
              (ok, error) -> {
                if (!(error instanceof FailedPartitionTransitionPreparationException)) {
                  context.setCurrentTerm(term);
                  context.setCurrentRole(role);
                }
              });
          enqueueNextTransition(term, role, nextTransitionFuture, nextTransition);
        });
    return nextTransitionFuture;
  }

  private void enqueueNextTransition(
      final long term,
      final PartitionRole role,
      final ActorFuture<Void> nextTransitionFuture,
      final TransitionProcess nextTransition) {
    final ActorFuture<Void> ongoingTransitionFuture;
    if (currentTransition == null) {
      ongoingTransitionFuture = concurrencyControl.createCompletedFuture();
    } else {
      ongoingTransitionFuture = currentTransitionFuture;
      final TransitionProcess ongoingTransition = currentTransition;
      if (!ongoingTransition.isCompleted()) {
        LOG.info(
            "Cancelling transition {} in favour of next transition {}",
            ongoingTransition,
            nextTransition);
      }
      ongoingTransition.cancel();
    }
    currentTransitionFuture = nextTransitionFuture;
    currentTransition = nextTransition;
    ongoingTransitionFuture.onComplete(
        (ignored, error) ->
            performNextTransition(term, role, nextTransitionFuture, nextTransition));
  }

  private void performNextTransition(
      final long term,
      final PartitionRole role,
      final ActorFuture<Void> nextTransitionFuture,
      final TransitionProcess nextTransition) {
    nextTransition
        .prepare(term, role)
        .onComplete(
            (ok, error) -> {
              if (error != null) {
                LOG.error("Transition preparation to {} on term {} failed", role, term, error);
                nextTransitionFuture.completeExceptionally(
                    new FailedPartitionTransitionPreparationException(error));
              } else {
                nextTransition.start(nextTransitionFuture);
              }
            });
  }

  public static final class CancelledPartitionTransitionException extends RuntimeException {
    public CancelledPartitionTransitionException() {
      super("Transition was cancelled in favour of a newer transition");
    }
  }

  public static final class FailedPartitionTransitionPreparationException extends RuntimeException {
    public FailedPartitionTransitionPreparationException(final Throwable cause) {
      super("Transition preparation failed", cause);
    }
  }

  private static final class TransitionProcess {
    private final List<PartitionTransitionStep> pendingSteps;
    private final Deque<PartitionTransitionStep> stepsToPrepare = new ArrayDeque<>();
    private final ConcurrencyControl concurrencyControl;
    private final PartitionTransitionContext context;
    private final long term;
    private final PartitionRole role;
    private boolean cancelRequested;
    private boolean completed;

    private TransitionProcess(
        final List<PartitionTransitionStep> steps,
        final ConcurrencyControl concurrencyControl,
        final PartitionTransitionContext context,
        final long term,
        final PartitionRole role) {
      pendingSteps = new ArrayList<>(Objects.requireNonNull(steps, "steps"));
      steps.forEach(stepsToPrepare::push);
      this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
      this.context = Objects.requireNonNull(context, "context");
      this.term = term;
      this.role = Objects.requireNonNull(role, "role");
    }

    private ActorFuture<Void> prepare(final long newTerm, final PartitionRole newRole) {
      final ActorFuture<Void> future = concurrencyControl.createFuture();
      if (stepsToPrepare.isEmpty()) {
        future.complete(null);
      } else {
        proceedWithPrepare(future, newTerm, newRole);
      }
      return future;
    }

    private void proceedWithPrepare(
        final ActorFuture<Void> future, final long newTerm, final PartitionRole newRole) {
      concurrencyControl.run(
          () ->
              stepsToPrepare
                  .pop()
                  .prepareTransition(context, newTerm, newRole)
                  .onComplete(
                      (ok, error) -> {
                        if (error != null) {
                          future.completeExceptionally(error);
                        } else if (stepsToPrepare.isEmpty()) {
                          future.complete(null);
                        } else {
                          proceedWithPrepare(future, newTerm, newRole);
                        }
                      }));
    }

    private void start(final ActorFuture<Void> future) {
      if (pendingSteps.isEmpty()) {
        completed = true;
        future.complete(null);
      } else {
        proceedWithTransition(future);
      }
    }

    private void proceedWithTransition(final ActorFuture<Void> future) {
      if (cancelRequested) {
        completed = true;
        future.completeExceptionally(new CancelledPartitionTransitionException());
        return;
      }
      concurrencyControl.run(
          () ->
              pendingSteps
                  .remove(0)
                  .transitionTo(context, term, role)
                  .onComplete(
                      (ok, error) -> {
                        if (error != null) {
                          future.completeExceptionally(error);
                        } else if (pendingSteps.isEmpty()) {
                          completed = true;
                          future.complete(null);
                        } else {
                          proceedWithTransition(future);
                        }
                      }));
    }

    private void cancel() {
      if (!completed) {
        cancelRequested = true;
      }
    }

    private boolean isCompleted() {
      return completed;
    }

    @Override
    public String toString() {
      return "TransitionProcess{term=" + term + ", role=" + role + "}";
    }
  }
}
