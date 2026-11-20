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
package io.taskite.broker.client;

import java.time.Duration;

public record BrokerClientConfig(Duration requestTimeout, int maxRetries) {
  public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(5);
  public static final int DEFAULT_MAX_RETRIES = 2;

  public BrokerClientConfig {
    requestTimeout =
        requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()
            ? DEFAULT_REQUEST_TIMEOUT
            : requestTimeout;
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must be >= 0");
    }
  }

  public BrokerClientConfig() {
    this(DEFAULT_REQUEST_TIMEOUT, DEFAULT_MAX_RETRIES);
  }
}
