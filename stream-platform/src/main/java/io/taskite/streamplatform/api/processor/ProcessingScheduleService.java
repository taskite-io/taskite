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
package io.taskite.streamplatform.api.processor;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.ScheduledTimer;
import java.time.Duration;
import java.util.function.Consumer;

/** Leader tasks executed on the processor actor, between committed command transactions. */
@FunctionalInterface
public interface ProcessingScheduleService {
  /**
   * Schedules a read-only state task. Collected commands are copied and appended after its read
   * transaction closes. Tasks wait for earlier scheduled commands to be processed before reading
   * state again. May be called from any thread; task registration and execution belong to the
   * processor actor. Cancellation prevents tasks which have not started from running.
   */
  ScheduledTimer schedule(Duration delay, Consumer<CommandWriter> task);

  @FunctionalInterface
  interface CommandWriter {
    void append(long key, ValueType type, Intent intent, UnifiedRecordValue value);

    /** Returns false when this round's shared processing budget is exhausted. */
    default boolean tryAppend(long key, ValueType type, Intent intent, UnifiedRecordValue value) {
      append(key, type, intent, value);
      return true;
    }

    /** Upper bound for a state query's result count; recheck before each append. */
    default int remainingCapacity() {
      return Integer.MAX_VALUE;
    }

    default boolean hasRemainingCapacity() {
      return remainingCapacity() > 0;
    }
  }
}
