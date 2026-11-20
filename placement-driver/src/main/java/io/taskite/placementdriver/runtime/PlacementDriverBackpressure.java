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

import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;

/** PD owns its command exemptions; management and internal maintenance always bypass admission. */
public final class PlacementDriverBackpressure {
  private PlacementDriverBackpressure() {}

  public static boolean isExempt(final Intent intent) {
    return switch (intent) {
      case BrokerIntent.REGISTER,
              BrokerIntent.RENEW,
              BrokerIntent.DEREGISTER,
              WorkerIntent.REGISTER,
              WorkerIntent.RENEW,
              WorkerIntent.DEREGISTER,
              CommandDistributionIntent.START ->
          false;
      case null, default -> true;
    };
  }
}
