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
package io.taskite.placementdriver.processing.broker.timer;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.scheduler.DueDateChecker;
import java.time.Duration;
import java.util.function.LongSupplier;

/** Reads the ordered deadline index and produces commands; never sends RPCs or changes state. */
public final class DueDateTimerChecker implements StreamProcessorLifecycleAware {
  private final ProcessingState state;
  private final LongSupplier clock;
  private final DueDateChecker checker;

  public DueDateTimerChecker(final ProcessingState state) {
    this(state, System::currentTimeMillis);
  }

  DueDateTimerChecker(final ProcessingState state, final LongSupplier clock) {
    this.state = state;
    this.clock = clock;
    checker = new DueDateChecker(Duration.ofMillis(100), this::check, clock);
  }

  public void scheduleTimer(final long dueDate) {
    checker.schedule(dueDate);
  }

  long check(final CommandWriter writer) {
    return state
        .getTimerSubscriptionState()
        .processTimersWithDueDateBefore(
            clock.getAsLong(),
            (key, subscription) -> {
              return writer.tryAppend(key, ValueType.TIMER, TimerIntent.TRIGGER, subscription);
            });
  }

  @Override
  public void onRecovered(final ProcessingScheduleService scheduling) {
    checker.onRecovered(scheduling);
  }

  @Override
  public void onProcessed(final TypedRecord<?> command) {
    if (command.getValueType() == ValueType.TIMER
        && command.getValue() instanceof final TimerSubscriptionRecord timer) {
      // A pending or failed RPC leaves this timer in state. Retry at the minimum resolution.
      scheduleTimer(timer.getDueDate());
    }
  }

  @Override
  public void onClose() {
    checker.onClose();
  }
}
