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
import com.alipay.sofa.jraft.error.RaftError;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Bounds configuration callbacks and retries under the membership admission lock. */
final class JRaftConfigurationExecutor {
  private final String groupId;
  private final Supplier<Node> nodeSupplier;
  private final Object admissionLock;

  JRaftConfigurationExecutor(
      final String groupId, final Supplier<Node> nodeSupplier, final Object admissionLock) {
    this.groupId = groupId;
    this.nodeSupplier = nodeSupplier;
    this.admissionLock = admissionLock;
  }

  private record Change(
      Node node, String operation, Consumer<Closure> command, BooleanSupplier authorized) {}

  ActorFuture<Void> configure(
      final Node node,
      final String operation,
      final Consumer<Closure> command,
      final BooleanSupplier authorized) {
    final var result = new CompletableActorFuture<Void>();
    CompletableFuture.delayedExecutor(10, TimeUnit.SECONDS)
        .execute(
            () -> {
              synchronized (admissionLock) {
                if (!result.isDone()) {
                  result.completeExceptionally(
                      new IllegalStateException("CONFIGURATION_TIMEOUT: " + operation));
                }
              }
            });
    configure(new Change(node, operation, command, authorized), 20, result);
    return result;
  }

  private void configure(
      final Change change, final int retriesRemaining, final CompletableActorFuture<Void> result) {
    synchronized (admissionLock) {
      if (result.isDone()) {
        return;
      }
      try {
        if (!change.authorized().getAsBoolean()) {
          // A configuration closure runs before JRaft publishes its applied index. Recheck
          // authority once application catches up, without admitting an unfenced mutation.
          if (nodeSupplier.get() == change.node()
              && change.node().isLeader()
              && change.node().getLastAppliedLogIndex() < change.node().getLastCommittedIndex()
              && retriesRemaining > 0) {
            CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
                .execute(() -> configure(change, retriesRemaining - 1, result));
            return;
          }
          result.completeExceptionally(new IllegalStateException("EXECUTION_SUPERSEDED"));
          return;
        }
        if (nodeSupplier.get() != change.node() || !change.node().isLeader()) {
          result.completeExceptionally(new IllegalStateException("NOT_LEADER: " + groupId));
          return;
        }
        change
            .command()
            .accept(
                status -> {
                  synchronized (admissionLock) {
                    if (result.isDone()) {
                      return;
                    }
                    if (status.isOk()) {
                      result.complete(null);
                    } else if (status.getRaftError() == RaftError.EBUSY && retriesRemaining > 0) {
                      CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
                          .execute(() -> configure(change, retriesRemaining - 1, result));
                    } else {
                      result.completeExceptionally(
                          new IllegalStateException(change.operation() + " failed: " + status));
                    }
                  }
                });
      } catch (final RuntimeException error) {
        if (!result.isDone()) {
          result.completeExceptionally(error);
        }
      }
    }
  }
}
