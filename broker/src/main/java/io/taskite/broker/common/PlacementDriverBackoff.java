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

import io.taskite.broker.BrokerConfig.Registration;
import java.time.Duration;

/** Owned by one registration loop; only explicit admission failures advance the backoff. */
public final class PlacementDriverBackoff {
  private final Duration heartbeatInterval;
  private final ExponentialBackoff backoff;
  private long previousDelay;

  public PlacementDriverBackoff(final Registration registration) {
    heartbeatInterval = registration.renewInterval();
    final var config = registration.backpressure();
    final long maxDelay = Math.min(config.maxDelay().toMillis(), heartbeatInterval.toMillis());
    backoff =
        ExponentialBackoff.builder()
            .minDelay(Math.min(config.minDelay().toMillis(), maxDelay))
            .maxDelay(maxDelay)
            .backoffFactor(config.backoffFactor())
            .jitterFactor(config.jitterFactor())
            .build();
  }

  public Duration nextDelay(final Throwable error) {
    if (error == null) {
      previousDelay = 0;
      return heartbeatInterval;
    }
    if (!PlacementDriverErrorClassifier.isBackpressure(error)) {
      return heartbeatInterval;
    }
    previousDelay = backoff.nextDelay(previousDelay);
    return Duration.ofMillis(previousDelay);
  }
}
