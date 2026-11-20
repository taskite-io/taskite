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
package io.taskite.protocol.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.util.BufferUtil;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JobPriorityProtocolTest {
  @ParameterizedTest
  @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE})
  void prioritySurvivesSerializationAndCopy(int priority) {
    final var original = new JobRecord().setJobType("work").setPriority(priority);
    final var restored = new JobRecord();
    restored.wrap(BufferUtil.wrapString(original.toJson()));
    assertEquals(priority, restored.getPriority());
    assertEquals(priority, original.copy().getPriority());
  }
}
