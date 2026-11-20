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
package io.taskite.broker.common;

import java.util.Objects;
import java.util.Random;

/** Exponential retry delays in milliseconds with symmetric jitter and a strict upper bound. */
public final class ExponentialBackoff {
  public static final long DEFAULT_MAX_DELAY = 5_000L;
  public static final long DEFAULT_MIN_DELAY = 50L;
  public static final double DEFAULT_BACKOFF_FACTOR = 1.6;
  public static final double DEFAULT_JITTER_FACTOR = 0.1;

  private final long maxDelay;
  private final long minDelay;
  private final double backoffFactor;
  private final double jitterFactor;
  private final Random random;

  private ExponentialBackoff(final Builder builder) {
    maxDelay = builder.maxDelay;
    minDelay = builder.minDelay;
    backoffFactor = builder.backoffFactor;
    jitterFactor = builder.jitterFactor;
    random = builder.random;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A zero previous delay starts a new retry sequence; the minimum applies before jitter. */
  public long nextDelay(final long previousDelay) {
    final double delay = Math.max(minDelay, Math.min(maxDelay, previousDelay * backoffFactor));
    final double jitter = delay * jitterFactor * (2 * random.nextDouble() - 1);
    return Math.max(1, Math.min(maxDelay, Math.round(delay + jitter)));
  }

  public static final class Builder {
    private long maxDelay = DEFAULT_MAX_DELAY;
    private long minDelay = DEFAULT_MIN_DELAY;
    private double backoffFactor = DEFAULT_BACKOFF_FACTOR;
    private double jitterFactor = DEFAULT_JITTER_FACTOR;
    private Random random = new Random();

    private Builder() {}

    /** Sets the strict maximum delay in milliseconds. */
    public Builder maxDelay(final long maxDelay) {
      this.maxDelay = maxDelay;
      return this;
    }

    /** Sets the minimum delay before jitter, in milliseconds. */
    public Builder minDelay(final long minDelay) {
      this.minDelay = minDelay;
      return this;
    }

    public Builder backoffFactor(final double backoffFactor) {
      this.backoffFactor = backoffFactor;
      return this;
    }

    /** Sets the symmetric jitter range as a fraction of the baseline delay. */
    public Builder jitterFactor(final double jitterFactor) {
      this.jitterFactor = jitterFactor;
      return this;
    }

    /** Supplies a random generator, including seeded generators for deterministic tests. */
    public Builder random(final Random random) {
      this.random = Objects.requireNonNull(random, "random");
      return this;
    }

    public ExponentialBackoff build() {
      if (minDelay < 1 || maxDelay < minDelay) {
        throw new IllegalArgumentException("delays must satisfy 1ms <= minDelay <= maxDelay");
      }
      if (!Double.isFinite(backoffFactor) || backoffFactor < 1) {
        throw new IllegalArgumentException("backoffFactor must be finite and at least 1");
      }
      if (!Double.isFinite(jitterFactor) || jitterFactor < 0 || jitterFactor > 1) {
        throw new IllegalArgumentException("jitterFactor must be finite and between 0 and 1");
      }
      return new ExponentialBackoff(this);
    }
  }
}
