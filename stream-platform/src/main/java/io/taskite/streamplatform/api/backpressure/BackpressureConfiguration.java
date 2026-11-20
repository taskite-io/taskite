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

import java.time.Duration;

/**
 * Shared partition admission configuration; independent of transport and business request types.
 */
public record BackpressureConfiguration(
    Boolean enabled, Algorithm algorithm, Aimd aimd, Vegas vegas, Fixed fixed) {
  public enum Algorithm {
    VEGAS,
    AIMD,
    FIXED
  }

  public BackpressureConfiguration {
    enabled = enabled == null ? true : enabled;
    algorithm = algorithm == null ? Algorithm.AIMD : algorithm;
    aimd = aimd == null ? new Aimd(null, null, null, null, null) : aimd;
    vegas = vegas == null ? new Vegas(null, null, null, null) : vegas;
    fixed = fixed == null ? new Fixed(null) : fixed;
  }

  public static BackpressureConfiguration defaults() {
    return new BackpressureConfiguration(null, null, null, null, null);
  }

  public static BackpressureConfiguration fixed(final int limit) {
    return new BackpressureConfiguration(true, Algorithm.FIXED, null, null, new Fixed(limit));
  }

  public void validate() {
    if (aimd.requestTimeout().isNegative() || aimd.requestTimeout().toMillis() < 1) {
      throw new IllegalArgumentException("aimd.requestTimeout must be at least 1ms");
    }
    aimd.requestTimeout().toNanos();
    positive(aimd.minLimit(), "aimd.minLimit");
    if (aimd.initialLimit() < aimd.minLimit() || aimd.initialLimit() > aimd.maxLimit()) {
      throw new IllegalArgumentException("aimd requires minLimit <= initialLimit <= maxLimit");
    }
    if (!Double.isFinite(aimd.backoffRatio())
        || aimd.backoffRatio() < 0.5
        || aimd.backoffRatio() >= 1) {
      throw new IllegalArgumentException("aimd.backoffRatio must be in [0.5, 1)");
    }
    positive(vegas.initialLimit(), "vegas.initialLimit");
    positive(vegas.alpha(), "vegas.alpha");
    if (vegas.beta() <= vegas.alpha() || vegas.maxLimit() < vegas.initialLimit()) {
      throw new IllegalArgumentException(
          "vegas requires alpha < beta and initialLimit <= maxLimit");
    }
    positive(fixed.limit(), "fixed.limit");
  }

  private static void positive(final int value, final String name) {
    if (value <= 0) {
      throw new IllegalArgumentException("backpressure " + name + " must be positive");
    }
  }

  public record Aimd(
      Duration requestTimeout,
      Integer initialLimit,
      Integer minLimit,
      Integer maxLimit,
      Double backoffRatio) {
    public Aimd {
      requestTimeout = requestTimeout == null ? Duration.ofMillis(200) : requestTimeout;
      initialLimit = initialLimit == null ? 100 : initialLimit;
      minLimit = minLimit == null ? 1 : minLimit;
      maxLimit = maxLimit == null ? 1000 : maxLimit;
      backoffRatio = backoffRatio == null ? 0.9 : backoffRatio;
    }
  }

  public record Vegas(Integer initialLimit, Integer alpha, Integer beta, Integer maxLimit) {
    public Vegas {
      initialLimit = initialLimit == null ? 20 : initialLimit;
      alpha = alpha == null ? 3 : alpha;
      beta = beta == null ? 6 : beta;
      maxLimit = maxLimit == null ? 1000 : maxLimit;
    }
  }

  public record Fixed(Integer limit) {
    public Fixed {
      limit = limit == null ? 20 : limit;
    }
  }
}
