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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the {@link PartitionTransition} orchestration in isolation with recording fake steps:
 * verifies transition runs steps forward, prepare runs the previous transition's steps in reverse,
 * the first transition also prepares safely, and a newer transition cancels an in-flight one.
 */
class PartitionTransitionTest {

  private ActorScheduler scheduler;
  private Actor actor;

  @BeforeEach
  void setUp() {
    scheduler =
        ActorScheduler.newActorScheduler()
            .setCpuBoundActorThreadCount(1)
            .setIoBoundActorThreadCount(1)
            .build();
    scheduler.start();
    actor = Actor.newActor().name("transition-test-actor").build();
    scheduler.submitActor(actor).join();
  }

  @AfterEach
  void tearDown() {
    scheduler.stop().join();
  }

  @Test
  void firstTransitionPreparesSafelyThenRunsStepsForward() {
    final List<String> events = new CopyOnWriteArrayList<>();
    final RecordingStep a = new RecordingStep("A", events);
    final RecordingStep b = new RecordingStep("B", events);
    final PartitionTransition transition = newTransition(List.of(a, b));

    transition.toLeader(1).join();

    // prepare = reverse (B then A), transition = forward (A then B). On first startup the prepare
    // steps are no-ops in production, but running them avoids missing cleanup if startup is
    // cancelled midway.
    assertEquals(
        List.of(
            "B.prepare=LEADER",
            "A.prepare=LEADER",
            "A.transitionTo=LEADER",
            "B.transitionTo=LEADER"),
        events);
  }

  @Test
  void secondTransitionPreparesPreviousStepsInReverseThenTransitionsForward() {
    final List<String> events = new CopyOnWriteArrayList<>();
    final RecordingStep a = new RecordingStep("A", events);
    final RecordingStep b = new RecordingStep("B", events);
    final PartitionTransition transition = newTransition(List.of(a, b));

    transition.toLeader(1).join();
    events.clear();
    transition.toFollower(2).join();

    // prepare = reverse (B then A), transition = forward (A then B).
    assertEquals(
        List.of(
            "B.prepare=FOLLOWER",
            "A.prepare=FOLLOWER",
            "A.transitionTo=FOLLOWER",
            "B.transitionTo=FOLLOWER"),
        events);
  }

  @Test
  void currentTermAndRoleAreRecordedOnContextAfterTransition() {
    final List<String> events = new CopyOnWriteArrayList<>();
    final RecordingStep a = new RecordingStep("A", events);
    final PartitionTransitionContext context = newContext();
    final PartitionTransition transition = new PartitionTransition(List.of(a));
    transition.setConcurrencyControl(actor);
    transition.updateTransitionContext(context);

    transition.toLeader(7).join();

    assertEquals(PartitionRole.LEADER, context.getCurrentRole());
    assertEquals(7, context.getCurrentTerm());
  }

  @Test
  void newerTransitionCancelsInFlightOne() {
    final List<String> events = new CopyOnWriteArrayList<>();
    // A blocks on its leader transition until released; B follows. Cancellation is honoured only
    // *between* steps, so a second step (B) must remain pending for the in-flight leader transition
    // to observe the cancel when A finally completes.
    final BlockingStep a = new BlockingStep("A", events);
    final RecordingStep b = new RecordingStep("B", events);
    final PartitionTransition transition = newTransition(List.of(a, b));

    final ActorFuture<Void> first = transition.toLeader(1);
    // Wait until the first transition is parked inside step A.
    a.awaitEntered();
    final ActorFuture<Void> second = transition.toFollower(2);
    // Release A on the actor thread: FIFO after toFollower's cancel-enqueue guarantees the
    // in-flight
    // leader transition is already cancelled, and the whole step-completion chain runs on the actor
    // thread (as in production), so the process observes the cancel between steps A and B.
    actor.run(a::release);

    boolean firstFailed = false;
    try {
      first.join();
    } catch (final Exception cancelled) {
      firstFailed = true;
    }
    second.join();

    assertTrue(firstFailed, "in-flight leader transition should be cancelled by the follower one");
    assertFalse(second.isCompletedExceptionally(), "queued follower transition should complete");
    assertTrue(
        events.contains("B.transitionTo=FOLLOWER"), "follower transition should install step B");
    assertFalse(
        events.contains("B.transitionTo=LEADER"),
        "cancelled leader transition must not install step B");
  }

  private PartitionTransition newTransition(final List<PartitionTransitionStep> steps) {
    final PartitionTransition transition = new PartitionTransition(steps);
    transition.setConcurrencyControl(actor);
    transition.updateTransitionContext(newContext());
    return transition;
  }

  private PartitionTransitionContext newContext() {
    // The engine and steps here never touch the raft partition / scheduler, so nulls are fine for
    // this isolated orchestration test.
    return new PartitionTransitionContext(0, null);
  }

  /** A step that records its prepare/transition calls and completes immediately. */
  private static class RecordingStep implements PartitionTransitionStep {
    private final String name;
    private final List<String> events;

    RecordingStep(final String name, final List<String> events) {
      this.name = name;
      this.events = events;
    }

    @Override
    public ActorFuture<Void> prepareTransition(
        final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
      events.add(name + ".prepare=" + targetRole);
      return CompletableActorFuture.completed(null);
    }

    @Override
    public ActorFuture<Void> transitionTo(
        final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
      events.add(name + ".transitionTo=" + targetRole);
      return CompletableActorFuture.completed(null);
    }

    @Override
    public String getName() {
      return name;
    }
  }

  /**
   * A step whose transitionTo parks until released, to exercise cancellation of an in-flight one.
   */
  private static final class BlockingStep extends RecordingStep {
    private final CompletableActorFuture<Void> gate = new CompletableActorFuture<>();
    private final CompletableActorFuture<Void> entered = new CompletableActorFuture<>();

    BlockingStep(final String name, final List<String> events) {
      super(name, events);
    }

    @Override
    public ActorFuture<Void> transitionTo(
        final PartitionTransitionContext context, final long term, final PartitionRole targetRole) {
      super.transitionTo(context, term, targetRole);
      if (targetRole == PartitionRole.LEADER) {
        if (!entered.isDone()) {
          entered.complete(null);
        }
        final CompletableActorFuture<Void> done = new CompletableActorFuture<>();
        gate.onComplete((ok, error) -> done.complete(null));
        return done;
      }
      return CompletableActorFuture.completed(null);
    }

    void awaitEntered() {
      entered.join();
    }

    void release() {
      gate.complete(null);
    }
  }
}
