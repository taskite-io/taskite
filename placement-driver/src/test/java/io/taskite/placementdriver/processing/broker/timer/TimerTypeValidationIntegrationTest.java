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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.record.RejectionType;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Nonliteral schedules and expressions of another type must not create durable scheduled state. */
class TimerTypeValidationIntegrationTest {
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "DURATION,R/PT30S",
    "DURATION,= \"PT30S\"",
    "DURATION,= \"R/PT30S\"",
    "CYCLE,PT30S",
    "CYCLE,= \"R/PT30S\"",
    "CYCLE,= \"PT30S\"",
    "CRON,R/PT30S",
    "CRON,PT30S",
    "CRON,= \"0 */5 * * * *\""
  })
  void processorRejectsNonliteralOrMismatchedExpressions(
      final TimerType type, final String expression) throws Exception {
    try (var fixture = new TimerFixture(directory)) {
      fixture.complete(
          42, TimerFixture.once().setType(type).setExpression(expression), TimerFixture.NOW);
      verify(fixture.writers.rejection())
          .appendRejection(eq(RejectionType.INVALID_ARGUMENT), anyString());
      assertTrue(fixture.state.getTimerSubscriptionState().subscriptions().isEmpty());
    }
  }
}
