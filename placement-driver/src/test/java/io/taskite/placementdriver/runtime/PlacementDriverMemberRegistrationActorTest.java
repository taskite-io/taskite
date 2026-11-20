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
package io.taskite.placementdriver.runtime;

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.MemberRole;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

final class PlacementDriverMemberRegistrationActorTest {
  @Test
  void learnerReportsProgressFrequentlyUntilItBecomesAVoter() {
    final var control = new Control();
    final var registrations = new AtomicInteger();
    try (var actor =
        actor(
            control,
            new AtomicInteger(),
            command -> {
              command
                  .getMembers()
                  .getFirst()
                  .setRole(
                      switch (registrations.incrementAndGet()) {
                        case 1 -> MemberRole.REGISTERED;
                        case 2 -> MemberRole.LEARNER;
                        default -> MemberRole.FOLLOWER;
                      });
              return CompletableActorFuture.completed(command);
            })) {
      actor.start();
      assertEquals(Duration.ofMillis(200), control.delay);
      control.tick();
      assertEquals(Duration.ofMillis(200), control.delay);
      control.tick();
      assertEquals(Duration.ofSeconds(5), control.delay);
    }
  }

  @Test
  void conflictingIdFailsStartupWithoutRemovalCleanupOrRetry() {
    final var control = new Control();
    final var removed = new AtomicInteger();
    final var failure = new IllegalStateException("PD_MEMBER_ID_CONFLICT");
    try (var actor =
        actor(
            control, removed, command -> CompletableActorFuture.completedExceptionally(failure))) {
      final ActorFuture<Void> started = actor.start();
      assertThrows(RuntimeException.class, started::join);
      assertTrue(started.isCompletedExceptionally());
      assertEquals(0, removed.get());
      assertNull(control.timer);
    }
  }

  @Test
  void waitsForAdmissionBeforeTreatingMissingLocalMembershipAsRemoval() {
    final var control = new Control();
    final var removed = new AtomicInteger();
    final var localMember = new AtomicBoolean(false);
    final List<TopologyUpdateType> commands = new ArrayList<>();
    try (var actor =
        new PlacementDriverMemberRegistrationActor(
            new PlacementDriverMemberRegistrationActor.Endpoint(
                "pd", "127.0.0.1", 8080, 26500, 26600),
            new PlacementDriverMemberRegistrationActor.Context(
                control,
                localMember::get,
                removed::incrementAndGet,
                () -> new PlacementDriverNodeSnapshot("pd", "127.0.0.1", 8080, 26500, 10, 9, 10),
                command -> {
                  commands.add(command.getUpdateType());
                  command
                      .getMembers()
                      .getFirst()
                      .setRole(commands.size() < 3 ? MemberRole.REGISTERED : MemberRole.LEARNER);
                  return CompletableActorFuture.completed(command);
                }))) {
      actor.start();
      control.tick();
      assertEquals(0, removed.get());
      localMember.set(true);
      control.tick();
      localMember.set(false);
      control.tick();
      assertEquals(1, removed.get());
      assertEquals(
          List.of(TopologyUpdateType.REGISTER, TopologyUpdateType.RENEW, TopologyUpdateType.RENEW),
          commands);
      assertNull(control.timer);
    }
  }

  @Test
  void renewalUsesEpochReturnedByCommittedRegistration() {
    final var control = new Control();
    final List<MemberTopologyRecord> commands = new ArrayList<>();
    try (var actor =
        actor(
            control,
            new AtomicInteger(),
            command -> {
              commands.add(command);
              command.getMembers().getFirst().setEndpointEpoch(7);
              return CompletableActorFuture.completed(command);
            })) {
      actor.start();
      assertEquals(TopologyUpdateType.REGISTER, commands.getFirst().getUpdateType());
      control.tick();
      assertEquals(TopologyUpdateType.RENEW, commands.getLast().getUpdateType());
      assertEquals(7, commands.getLast().getMembers().getFirst().getEndpointEpoch());
    }
    assertNull(control.timer);
  }

  @Test
  void initialRejectionRetriesRegistrationAndRevokedRenewalStopsNode() {
    final var control = new Control();
    final var removed = new AtomicInteger();
    final List<TopologyUpdateType> commands = new ArrayList<>();
    try (var actor =
        actor(
            control,
            removed,
            command -> {
              commands.add(command.getUpdateType());
              if (commands.size() == 1) {
                return CompletableActorFuture.completedExceptionally(
                    new IllegalStateException("NOT_LEADER"));
              }
              if (commands.size() == 3) {
                return CompletableActorFuture.completedExceptionally(
                    new IllegalStateException("STALE_PD_MEMBER_ENDPOINT_EPOCH"));
              }
              return CompletableActorFuture.completed(command);
            })) {
      actor.start();
      control.tick();
      control.tick();
      assertEquals(
          List.of(
              TopologyUpdateType.REGISTER, TopologyUpdateType.REGISTER, TopologyUpdateType.RENEW),
          commands);
      assertEquals(1, removed.get());
      assertNull(control.timer);
    }
  }

  @Test
  void completionAfterCloseCannotScheduleAnotherRenewal() {
    final var control = new Control();
    final var pending = new CompletableActorFuture<MemberTopologyRecord>();
    final var actor = actor(control, new AtomicInteger(), command -> pending);
    actor.start();
    actor.close();
    pending.complete(new MemberTopologyRecord());
    assertNull(control.timer);
  }

  private static PlacementDriverMemberRegistrationActor actor(
      final Control control,
      final AtomicInteger removed,
      final Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> updater) {
    return new PlacementDriverMemberRegistrationActor(
        new PlacementDriverMemberRegistrationActor.Endpoint("pd", "127.0.0.1", 8080, 26500, 26600),
        new PlacementDriverMemberRegistrationActor.Context(
            control,
            () -> true,
            removed::incrementAndGet,
            () -> new PlacementDriverNodeSnapshot("pd", "127.0.0.1", 8080, 26500, 10, 9, 10),
            updater));
  }

  private static final class Control implements ConcurrencyControl {
    private Runnable timer;
    private Duration delay;

    void tick() {
      final var action = timer;
      timer = null;
      assertNotNull(action);
      action.run();
    }

    public void run(final Runnable action) {
      action.run();
    }

    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.onComplete(callback);
    }

    public <T> ActorFuture<T> call(final Callable<T> action) {
      throw new UnsupportedOperationException();
    }

    public ScheduledTimer schedule(final Duration delay, final Runnable action) {
      assertNull(timer);
      this.delay = delay;
      timer = action;
      return () -> timer = null;
    }
  }
}
