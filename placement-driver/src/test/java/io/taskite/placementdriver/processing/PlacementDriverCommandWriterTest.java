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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

final class PlacementDriverCommandWriterTest {
  @Test
  void copiesValuesAndRetainsCommandsWhenTheSharedBudgetIsExhausted() {
    final var tasks = new ArrayDeque<Consumer<CommandWriter>>();
    final var commands = new PlacementDriverCommandWriter();
    commands.onRecovered(scheduling(tasks));
    final var value = new JobYieldRecord().setYieldId(1).setAttempts(1);
    assertTrue(commands.tryAppend(1, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    value.setAttempts(9);
    assertTrue(commands.tryAppend(2, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    final var appended = new ArrayList<Integer>();
    final CommandWriter budget =
        new CommandWriter() {
          @Override
          public int remainingCapacity() {
            return 1 - appended.size();
          }

          @Override
          public void append(
              final long key,
              final ValueType type,
              final Intent intent,
              final UnifiedRecordValue value) {
            appended.add(((JobYieldRecord) value).getAttempts());
          }
        };
    tasks.removeFirst().accept(budget);
    assertEquals(1, appended.getFirst());
    assertEquals(255, commands.remainingCapacity());
    appended.clear();
    tasks.removeFirst().accept(budget);
    assertEquals(9, appended.getFirst());
    assertEquals(256, commands.remainingCapacity());
    commands.onClose();
  }

  @Test
  void rejectsAdmissionOutsideLeaderLifetimeAndDiscardsOldGeneration() {
    final var tasks = new ArrayDeque<Consumer<CommandWriter>>();
    final var commands = new PlacementDriverCommandWriter();
    final var value = new JobYieldRecord().setYieldId(1);
    assertFalse(commands.tryAppend(1, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    commands.onRecovered(scheduling(tasks));
    for (int index = 0; index < 256; index++) {
      assertTrue(
          commands.tryAppend(index, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    }
    assertFalse(
        commands.tryAppend(257, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    commands.onClose();
    assertFalse(commands.tryAppend(1, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, value));
    tasks.clear();
    commands.onRecovered(scheduling(tasks));
    tasks
        .removeFirst()
        .accept(
            (key, type, intent, record) -> {
              throw new AssertionError("old leader commands must not be drained");
            });
    assertEquals(256, commands.remainingCapacity());
    commands.onClose();
  }

  @Test
  void retriesWhenScheduledWriterDeclinesAdmission() {
    final var tasks = new ArrayDeque<Consumer<CommandWriter>>();
    final var commands = new PlacementDriverCommandWriter();
    commands.onRecovered(scheduling(tasks));
    commands.append(1, ValueType.JOB_YIELD, JobYieldIntent.UPDATE_PROGRESS, new JobYieldRecord());
    tasks
        .removeFirst()
        .accept(
            new CommandWriter() {
              @Override
              public boolean tryAppend(
                  final long key,
                  final ValueType type,
                  final Intent intent,
                  final UnifiedRecordValue value) {
                return false;
              }

              @Override
              public void append(
                  final long key,
                  final ValueType type,
                  final Intent intent,
                  final UnifiedRecordValue value) {
                throw new AssertionError("budget refusal must not drop the command");
              }
            });
    assertEquals(255, commands.remainingCapacity());
    final var appended = new ArrayList<Long>();
    tasks.removeFirst().accept((key, type, intent, value) -> appended.add(key));
    assertEquals(1L, appended.getFirst());
    commands.onClose();
  }

  private static ProcessingScheduleService scheduling(
      final ArrayDeque<Consumer<CommandWriter>> tasks) {
    return (delay, task) -> {
      tasks.addLast(task);
      return () -> tasks.remove(task);
    };
  }
}
