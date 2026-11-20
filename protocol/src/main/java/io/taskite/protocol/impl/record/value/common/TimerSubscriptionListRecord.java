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
package io.taskite.protocol.impl.record.value.common;

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.Map;

/** Snapshot of the independent timers that are still scheduled in PD. */
public final class TimerSubscriptionListRecord extends UnifiedRecordValue {
  private Map<Long, TimerSubscriptionRecord> subscriptions = Map.of();

  public Map<Long, TimerSubscriptionRecord> getSubscriptions() {
    return subscriptions;
  }

  public TimerSubscriptionListRecord setSubscriptions(
      final Map<Long, TimerSubscriptionRecord> subscriptions) {
    this.subscriptions = Map.copyOf(subscriptions);
    return this;
  }
}
