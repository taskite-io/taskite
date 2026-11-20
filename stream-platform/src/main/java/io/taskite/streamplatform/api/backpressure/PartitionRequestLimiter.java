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
package io.taskite.streamplatform.api.backpressure;

import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.Limiter.Listener;
import com.netflix.concurrency.limits.limit.AIMDLimit;
import com.netflix.concurrency.limits.limit.FixedLimit;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * One Netflix concurrency limiter per partition. Business exemptions are supplied by the owner.
 * Permits live until actual processing completes, including after caller cancellation.
 */
public final class PartitionRequestLimiter<C> {
  private final BackpressureConfiguration config;
  private final Predicate<C> exempt;
  private final LongSupplier nanoTime;
  private SimpleLimiter<C> limiter;

  public PartitionRequestLimiter(
      final BackpressureConfiguration config, final Predicate<C> exempt) {
    this(config, exempt, System::nanoTime);
  }

  PartitionRequestLimiter(
      final BackpressureConfiguration config,
      final Predicate<C> exempt,
      final LongSupplier nanoTime) {
    this.config = Objects.requireNonNull(config, "config");
    config.validate();
    this.exempt = Objects.requireNonNull(exempt, "exempt");
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    reset();
  }

  public synchronized Optional<Permit> tryAcquire(final C command) {
    if (!config.enabled() || exempt.test(command)) {
      return Optional.of(new Permit(limiter, null));
    }
    return limiter.acquire(command).map(listener -> new Permit(limiter, listener));
  }

  public synchronized int limit() {
    return limiter.getLimit();
  }

  public synchronized int inFlight() {
    return limiter.getInflight();
  }

  /** Starts a new leadership epoch; callbacks holding old permits cannot update the new window. */
  public synchronized void reset() {
    final Limit algorithm =
        switch (config.algorithm()) {
          case FIXED -> FixedLimit.of(config.fixed().limit());
          case AIMD ->
              AIMDLimit.newBuilder()
                  .initialLimit(config.aimd().initialLimit())
                  .minLimit(config.aimd().minLimit())
                  .maxLimit(config.aimd().maxLimit())
                  .backoffRatio(config.aimd().backoffRatio())
                  .timeout(config.aimd().requestTimeout().toNanos(), TimeUnit.NANOSECONDS)
                  .build();
          case VEGAS ->
              VegasLimit.newBuilder()
                  .initialLimit(config.vegas().initialLimit())
                  .alpha(config.vegas().alpha())
                  .beta(config.vegas().beta())
                  .maxConcurrency(config.vegas().maxLimit())
                  .build();
        };
    limiter =
        SimpleLimiter.newBuilder()
            .limit(new ValidRttLimit(algorithm))
            .nanoClock(nanoTime)
            .named("taskite-partition-commands")
            .build();
  }

  /**
   * Invalid clock samples must not interrupt SimpleLimiter's semaphore release. All valid samples
   * and window changes are handled exclusively by the library algorithm.
   */
  private record ValidRttLimit(Limit delegate) implements Limit {
    @Override
    public int getLimit() {
      return delegate.getLimit();
    }

    @Override
    public void notifyOnChange(final Consumer<Integer> listener) {
      delegate.notifyOnChange(listener);
    }

    @Override
    public void onSample(
        final long startTime, final long rtt, final int inFlight, final boolean didDrop) {
      if (rtt > 0) {
        delegate.onSample(startTime, rtt, inFlight, didDrop);
      }
    }
  }

  private enum Completion {
    SUCCESS,
    DROPPED,
    IGNORED
  }

  public final class Permit {
    private final SimpleLimiter<C> owner;
    private final Listener listener;
    private boolean completed;

    private Permit(final SimpleLimiter<C> owner, final Listener listener) {
      this.owner = owner;
      this.listener = listener;
    }

    /** Includes business rejections: the Processor successfully handled the command. */
    public void success() {
      finish(Completion.SUCCESS);
    }

    /** Append or processing failed for a technical reason. */
    public void drop() {
      finish(Completion.DROPPED);
    }

    /** Leadership loss, shutdown or a command that never entered processing. */
    public void ignore() {
      finish(Completion.IGNORED);
    }

    private void finish(final Completion completion) {
      synchronized (PartitionRequestLimiter.this) {
        if (completed) {
          return;
        }
        completed = true;
        if (listener == null) {
          return;
        }
        if (owner != limiter) {
          listener.onIgnore();
          return;
        }
        switch (completion) {
          case SUCCESS -> listener.onSuccess();
          case DROPPED -> listener.onDropped();
          case IGNORED -> listener.onIgnore();
        }
      }
    }
  }
}
