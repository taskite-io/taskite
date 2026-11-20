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
import io.taskite.cluster.raft.RaftRole;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.function.BooleanSupplier;

/** Waits for the committed startup prefix to publish its application positions through onApply. */
final class RaftRecoveryBarrier {
  private final RaftPartition partition;
  private final ConcurrencyControl lifecycle;
  private final BooleanSupplier closed;
  private final long committedIndex;
  private final CompletableActorFuture<Void> recovered = new CompletableActorFuture<>();

  private RaftRecoveryBarrier(
      final RaftPartition partition,
      final ConcurrencyControl lifecycle,
      final BooleanSupplier closed) {
    this.partition = partition;
    this.lifecycle = lifecycle;
    this.closed = closed;
    committedIndex = partition.committedIndex();
  }

  static ActorFuture<Void> await(
      final RaftPartition partition,
      final ConcurrencyControl lifecycle,
      final BooleanSupplier closed) {
    final var barrier = new RaftRecoveryBarrier(partition, lifecycle, closed);
    barrier.check();
    return barrier.recovered;
  }

  private void check() {
    if (closed.getAsBoolean()) {
      recovered.completeExceptionally(
          new IllegalStateException(
              "partition " + partition.partitionId() + " closed during startup"));
    } else if (partition.lastAppliedIndex() < committedIndex
        || partition.role() == RaftRole.LEADER && partition.term() == 0) {
      // Node.isLeader becomes true before the FSM replays the prefix and calls onLeaderStart.
      lifecycle.schedule(Duration.ofMillis(1), this::check);
    } else {
      recovered.complete(null);
    }
  }
}
