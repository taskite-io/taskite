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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.RecordingCommandWriter;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;

final class PlacementDriverClusterLeadershipListenerTest {

  @Test
  void leaderSubmitsOneTopologyVersion() {
    final List<LoggedEvent> submitted = new ArrayList<>();
    final var committed = new AtomicReference<Optional<MemberTopologyRecord>>(Optional.empty());
    final PlacementDriverClusterLeadershipListener listener =
        new PlacementDriverClusterLeadershipListener(
            "meta-0",
            new ImmediateConcurrencyControl(),
            () -> true,
            PlacementDriverClusterLeadershipListenerTest::topology,
            committed::get,
            new RecordingCommandWriter(submitted::add));

    final var ready = listener.initializeClusterLeadership();
    assertFalse(ready.isDone());
    listener.onClusterLeadershipChanged();
    listener.onClusterLeadershipChanged();

    assertEquals(1, submitted.size());
    assertEquals(TopologyIntent.UPDATE, submitted.getFirst().metadata().getIntent());
    committed.set(Optional.of(topology()));
    listener.onClusterLeadershipChanged();
    assertTrue(ready.isDone());
    assertFalse(ready.isCompletedExceptionally());
    listener.close();
  }

  @Test
  void followerDoesNotSubmitUpdate() {
    final List<LoggedEvent> submitted = new ArrayList<>();
    final PlacementDriverClusterLeadershipListener listener =
        new PlacementDriverClusterLeadershipListener(
            "meta-0",
            new ImmediateConcurrencyControl(),
            () -> false,
            PlacementDriverClusterLeadershipListenerTest::topology,
            Optional::empty,
            new RecordingCommandWriter(submitted::add));

    listener.onClusterLeadershipChanged();

    assertEquals(List.of(), submitted);
    listener.close();
  }

  private static MemberTopologyRecord topology() {
    return new MemberTopologyRecord()
        .setLocalMemberId("meta-0")
        .setLeaderId("meta-0")
        .setLeaderEndpoint("127.0.0.1:26500")
        .setTerm(1);
  }

  private static final class ImmediateConcurrencyControl implements ConcurrencyControl {
    @Override
    public <T> void runOnCompletion(
        final ActorFuture<T> future, final BiConsumer<T, Throwable> callback) {
      future.onComplete(callback);
    }

    @Override
    public void run(final Runnable action) {
      action.run();
    }

    @Override
    public <T> ActorFuture<T> call(final Callable<T> callable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ScheduledTimer schedule(final Duration delay, final Runnable runnable) {
      return () -> {};
    }
  }
}
