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
package io.taskite.placementdriver.processing;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.processor.StreamProcessorLifecycleAware;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import java.time.Duration;
import java.util.ArrayDeque;
import org.agrona.concurrent.UnsafeBuffer;

/** Copies runtime commands onto the processor's scheduled, budgeted append path. */
public final class PlacementDriverCommandWriter
    implements CommandWriter, StreamProcessorLifecycleAware {
  private static final int CAPACITY = 256;
  private static final String COMMAND_NOT_ADMITTED =
      "Expected to append PD command with key '%d', valueType '%s' and intent '%s', but the command writer is unavailable or full (remaining capacity '%d')";
  private static final String EVENT_NOT_ALLOWED =
      "Expected to append PD command with key '%d' and valueType '%s', but intent '%s' is an event";
  private static final Duration INTERVAL = Duration.ofMillis(100);
  private final ArrayDeque<Command> commands = new ArrayDeque<>();
  private final TypedEventRegistry registry = PlacementDriverProcessors.createEventRegistry();
  private boolean active;
  private ProcessingScheduleService scheduling;
  private ScheduledTimer timer;

  @Override
  public void append(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    if (!tryAppend(key, type, intent, value)) {
      throw new IllegalStateException(
          COMMAND_NOT_ADMITTED.formatted(key, type, intent, remainingCapacity()));
    }
  }

  /** Admission only; callers observe durable state and retry if it has not advanced. */
  @Override
  public synchronized boolean tryAppend(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    if (!active || commands.size() >= CAPACITY) {
      return false;
    }
    if (intent.isEvent()) {
      throw new IllegalArgumentException(EVENT_NOT_ALLOWED.formatted(key, type, intent));
    }
    final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(buffer, 0);
    final var copy = registry.create(type, intent);
    copy.wrap(buffer);
    commands.addLast(new Command(key, type, intent, copy));
    return true;
  }

  @Override
  public synchronized int remainingCapacity() {
    return active ? CAPACITY - commands.size() : 0;
  }

  @Override
  public synchronized void onRecovered(final ProcessingScheduleService scheduling) {
    this.scheduling = scheduling;
    active = true;
    timer = scheduling.schedule(INTERVAL, this::drain);
  }

  private synchronized void drain(final CommandWriter writer) {
    if (!active) {
      return;
    }
    while (!commands.isEmpty() && writer.hasRemainingCapacity()) {
      final Command command = commands.getFirst();
      if (!writer.tryAppend(command.key(), command.type(), command.intent(), command.value())) {
        break;
      }
      commands.removeFirst();
    }
    timer = scheduling.schedule(INTERVAL, this::drain);
  }

  @Override
  public synchronized void onClose() {
    active = false;
    commands.clear();
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    scheduling = null;
  }

  private record Command(long key, ValueType type, Intent intent, UnifiedRecordValue value) {}
}
