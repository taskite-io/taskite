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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import org.junit.jupiter.api.Test;

/**
 * Verifies the version dimension added for ZEP004 "Record Versioning": several versions of the same
 * {@code (ValueType, Intent)} command logic can coexist, exact-version lookups win, and {@code
 * getLatestVersion} exposes the fallback the {@code ProcessingStateMachine} uses for unknown
 * versions.
 */
final class TypedRecordProcessorsTest {

  private final TypedRecordProcessor<?> v1 = new NamedProcessor("v1");
  private final TypedRecordProcessor<?> v2 = new NamedProcessor("v2");

  @Test
  void coexistingVersionsAreLookedUpExactly() {
    final TypedRecordProcessors map =
        TypedRecordProcessors.processors()
            .onCommand(ValueType.JOB, JobIntent.COMPLETE, 1, v1)
            .onCommand(ValueType.JOB, JobIntent.COMPLETE, 2, v2);

    assertSame(v1, map.get(ValueType.JOB, JobIntent.COMPLETE, 1));
    assertSame(v2, map.get(ValueType.JOB, JobIntent.COMPLETE, 2));
  }

  @Test
  void getLatestVersionReturnsHighestRegistered() {
    final TypedRecordProcessors map =
        TypedRecordProcessors.processors()
            .onCommand(ValueType.JOB, JobIntent.COMPLETE, 1, v1)
            .onCommand(ValueType.JOB, JobIntent.COMPLETE, 3, v2);

    assertEquals(3, map.getLatestVersion(ValueType.JOB, JobIntent.COMPLETE));
  }

  @Test
  void getLatestVersionIsMinusOneWhenPairUnregistered() {
    final TypedRecordProcessors map = TypedRecordProcessors.processors();
    assertEquals(-1, map.getLatestVersion(ValueType.JOB, JobIntent.COMPLETE));
    assertNull(map.get(ValueType.JOB, JobIntent.COMPLETE, 1));
  }

  @Test
  void duplicateSameVersionRegistrationIsRejected() {
    final TypedRecordProcessors map =
        TypedRecordProcessors.processors().onCommand(ValueType.JOB, JobIntent.COMPLETE, 1, v1);

    assertThrows(
        IllegalArgumentException.class,
        () -> map.onCommand(ValueType.JOB, JobIntent.COMPLETE, 1, v2));
  }

  @Test
  void onCommandDefaultsToDefaultRecordVersion() {
    final TypedRecordProcessors processors =
        TypedRecordProcessors.processors().onCommand(ValueType.JOB, JobIntent.COMPLETE, v1);

    assertSame(
        v1,
        processors.get(
            ValueType.JOB, JobIntent.COMPLETE, TypedRecordProcessors.DEFAULT_RECORD_VERSION));
    assertEquals(
        TypedRecordProcessors.DEFAULT_RECORD_VERSION,
        processors.getLatestVersion(ValueType.JOB, JobIntent.COMPLETE));
  }

  @Test
  void onCommandRejectsEventIntents() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TypedRecordProcessors.processors().onCommand(ValueType.JOB, JobIntent.CREATED, v1));
  }

  private record NamedProcessor(String name) implements TypedRecordProcessor<UnifiedRecordValue> {}
}
