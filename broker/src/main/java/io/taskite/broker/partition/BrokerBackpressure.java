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
package io.taskite.broker.partition;

import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;

/** Completion and cleanup commands must remain available when ordinary commands saturate. */
public final class BrokerBackpressure {
  private BrokerBackpressure() {}

  public static boolean isExempt(final Intent intent) {
    return switch (intent) {
      case JobIntent.COMPLETE,
              JobIntent.FAIL,
              JobIntent.TIME_OUT,
              JobIntent.YIELD,
              JobYieldIntent.FENCE_SESSION,
              JobYieldIntent.COMPLETE_YIELD,
              JobIntent.CANCEL ->
          true;
      case null, default -> false;
    };
  }
}
