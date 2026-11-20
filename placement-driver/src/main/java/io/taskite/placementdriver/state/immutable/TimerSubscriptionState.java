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
package io.taskite.placementdriver.state.immutable;

import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

public interface TimerSubscriptionState {
  Map<Long, TimerSubscriptionRecord> subscriptions();

  Optional<TimerSubscriptionRecord> get(long timerKey);

  /** Visits due timers in index order; returns the first unconsumed deadline, or -1 if empty. */
  long processTimersWithDueDateBefore(
      long timestamp, BiPredicate<Long, TimerSubscriptionRecord> visitor);
}
