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
package io.taskite.snapshot;

import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executes size, interval, and manual snapshot triggers for one leader term. */
public final class SnapshotDirector implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDirector.class);

  private final int partitionId;
  private final SnapshotPolicy policy;
  private final SnapshotProgressSource progressSource;
  private final SnapshotOperation operation;
  private final SnapshotMetrics metrics;
  private final ConcurrencyControl control;
  private final AtomicBoolean accepting = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean();

  private SnapshotProgressSubscription progressSubscription;
  private SnapshotProgress progress = SnapshotProgress.EMPTY;
  private ActorFuture<SnapshotResult> inFlight;
  private ScheduledTimer intervalTimer;
  private ScheduledTimer retryTimer;
  private Duration retryDelay;
  private boolean started;

  public SnapshotDirector(
      final int partitionId,
      final SnapshotPolicy policy,
      final SnapshotProgressSource progressSource,
      final SnapshotOperation operation,
      final SnapshotMetrics metrics,
      final ConcurrencyControl control) {
    this.partitionId = partitionId;
    this.policy = Objects.requireNonNull(policy, "policy");
    this.progressSource = Objects.requireNonNull(progressSource, "progressSource");
    this.operation = Objects.requireNonNull(operation, "operation");
    this.metrics = Objects.requireNonNullElse(metrics, SnapshotMetrics.NOOP);
    this.control = Objects.requireNonNull(control, "control");
    retryDelay = policy.initialRetryDelay();
  }

  /** Installs the progress subscription and leader-only timers. Must run on the lifecycle actor. */
  public void start() {
    if (started) {
      throw new IllegalStateException("snapshot director already started");
    }
    if (closed.get()) {
      throw new IllegalStateException("snapshot director already closed");
    }
    started = true;
    progress = progressSource.current();
    metrics.onProgress(progress);
    progressSubscription =
        progressSource.subscribe(
            updated ->
                control.run(
                    () -> {
                      if (!accepting.get()) {
                        return;
                      }
                      progress = updated;
                      metrics.onProgress(updated);
                      triggerForSizeIfDue();
                    }));
    scheduleInterval();
    triggerForSizeIfDue();
  }

  /**
   * Prevents new work immediately while transition preparation tears down actor-owned resources.
   */
  public void stopAccepting() {
    accepting.set(false);
  }

  public ActorFuture<SnapshotResult> forceSnapshot() {
    final CompletableActorFuture<SnapshotResult> result = new CompletableActorFuture<>();
    control.run(
        () -> {
          if (!accepting.get() || closed.get()) {
            result.completeExceptionally(
                new IllegalStateException(
                    "snapshot director is not active for partition " + partitionId));
            return;
          }
          final ActorFuture<SnapshotResult> operationFuture =
              inFlight == null ? startSnapshot(SnapshotTriggerReason.MANUAL) : inFlight;
          operationFuture.onComplete(result);
        });
    return result;
  }

  private void triggerForSizeIfDue() {
    if (!accepting.get()
        || !policy.sizeEnabled()
        || progress.uncoveredLogBytes() < policy.logSizeThresholdBytes()) {
      return;
    }
    startSnapshot(SnapshotTriggerReason.SIZE);
  }

  private ActorFuture<SnapshotResult> startSnapshot(final SnapshotTriggerReason reason) {
    if (inFlight != null) {
      return inFlight;
    }
    if (!accepting.get() || closed.get()) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException(
              "snapshot director is not active for partition " + partitionId));
    }
    if (retryTimer != null) {
      retryTimer.cancel();
      retryTimer = null;
    }
    metrics.onTriggered(reason);
    final long startedAt = System.nanoTime();
    final ActorFuture<SnapshotResult> snapshot;
    try {
      snapshot = Objects.requireNonNull(operation.takeSnapshot(), "snapshot operation future");
    } catch (final Throwable error) {
      onSnapshotCompleted(reason, startedAt, null, error);
      return CompletableActorFuture.completedExceptionally(error);
    }
    inFlight = snapshot;
    control.runOnCompletion(
        snapshot,
        (result, error) -> {
          if (inFlight == snapshot) {
            inFlight = null;
          }
          onSnapshotCompleted(reason, startedAt, result, error);
        });
    return snapshot;
  }

  private void onSnapshotCompleted(
      final SnapshotTriggerReason reason,
      final long startedAt,
      final SnapshotResult result,
      final Throwable error) {
    metrics.onCompleted(reason, Duration.ofNanos(System.nanoTime() - startedAt), result, error);
    if (error != null) {
      if (accepting.get() && !closed.get()) {
        LOG.warn("partition {} snapshot failed", partitionId, error);
        scheduleRetry();
      }
      return;
    }
    if (result != null && result.skipped()) {
      progress = progressSource.current();
      metrics.onProgress(progress);
      if (policy.sizeEnabled() && progress.uncoveredLogBytes() >= policy.logSizeThresholdBytes()) {
        scheduleRetry();
      }
      return;
    }
    retryDelay = policy.initialRetryDelay();
    progress = progressSource.current();
    metrics.onProgress(progress);
    triggerForSizeIfDue();
  }

  private void scheduleInterval() {
    if (!policy.intervalEnabled() || !accepting.get() || closed.get()) {
      return;
    }
    intervalTimer =
        control.schedule(
            policy.interval(),
            () -> {
              intervalTimer = null;
              if (accepting.get() && progress.hasUncoveredEntries()) {
                startSnapshot(SnapshotTriggerReason.INTERVAL);
              }
              scheduleInterval();
            });
  }

  private void scheduleRetry() {
    if (retryTimer != null) {
      return;
    }
    final Duration delay = retryDelay;
    retryDelay = nextRetryDelay(delay);
    metrics.onRetryScheduled(delay);
    retryTimer =
        control.schedule(
            delay,
            () -> {
              retryTimer = null;
              if (accepting.get() && !closed.get()) {
                startSnapshot(SnapshotTriggerReason.RETRY);
              }
            });
  }

  private Duration nextRetryDelay(final Duration current) {
    if (current.compareTo(policy.maxRetryDelay()) >= 0) {
      return policy.maxRetryDelay();
    }
    final Duration doubled = current.multipliedBy(2);
    return doubled.compareTo(policy.maxRetryDelay()) > 0 ? policy.maxRetryDelay() : doubled;
  }

  @Override
  public void close() {
    stopAccepting();
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (progressSubscription != null) {
      progressSubscription.close();
      progressSubscription = null;
    }
    if (intervalTimer != null) {
      intervalTimer.cancel();
      intervalTimer = null;
    }
    if (retryTimer != null) {
      retryTimer.cancel();
      retryTimer = null;
    }
    metrics.close();
  }
}
