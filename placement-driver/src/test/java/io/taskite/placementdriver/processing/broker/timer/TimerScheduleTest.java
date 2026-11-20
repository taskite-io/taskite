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

import static org.junit.jupiter.api.Assertions.*;

import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

final class TimerScheduleTest {
  @ParameterizedTest
  @CsvSource({"DURATION,' PT1S ',1000", "CYCLE,' R/PT1S ',1000", "CRON,' 0 * * * * * ',60000"})
  void acceptsWhitespaceAroundLiteralSchedules(
      final TimerType type, final String expression, final long delay) {
    final long now = epoch("2026-11-11T00:00:00Z");
    assertEquals(
        now + delay,
        TimerSchedule.parse(type, expression).first(now, ZoneId.of("UTC")).orElseThrow());
  }

  @ParameterizedTest
  @CsvSource({
    "P1M,2024-01-31T12:00:00Z,2024-02-29T12:00:00Z",
    "P1Y,2024-02-29T12:00:00Z,2025-02-28T12:00:00Z",
    "P1W,2026-11-11T00:00:00Z,2026-11-18T00:00:00Z",
    "P1DT2H,2026-11-11T00:00:00Z,2026-11-12T02:00:00Z",
    "PT0.001S,2026-11-11T00:00:00Z,2026-11-11T00:00:00.001Z"
  })
  void computesCalendarAndClockUnits(
      final String expression, final String from, final String expected) {
    assertEquals(epoch(expected), TimeInterval.parse(expression).addTo(ZonedDateTime.parse(from)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"P", "PT", "PT0S", "P0D", "PT-1S", "-P1D", "PT0.0001S", "P1DT", "1h", "PT1Sx"})
  void rejectsInvalidOrNonAdvancingIntervals(final String expression) {
    assertThrows(RuntimeException.class, () -> TimeInterval.parse(expression));
  }

  @Test
  void calendarDayAndTwentyFourHoursDifferAcrossDst() {
    var from = ZonedDateTime.parse("2026-03-07T12:00:00-05:00[America/New_York]");
    assertEquals(epoch("2026-03-08T16:00:00Z"), TimeInterval.parse("P1D").addTo(from));
    assertEquals(epoch("2026-03-08T17:00:00Z"), TimeInterval.parse("PT24H").addTo(from));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "R0/PT1S",
        "R-1/PT1S",
        "R2",
        "R2//PT1S",
        "R2/broken/PT1S",
        "R999999999999/PT1S",
        "R/PT0S",
        "R2/PT1S/"
      })
  void rejectsMalformedCycles(final String expression) {
    assertThrows(RuntimeException.class, () -> TimerSchedule.parse(TimerType.CYCLE, expression));
  }

  @Test
  void supportsFutureStartPastStartAndFiniteExhaustion() {
    long now = epoch("2026-11-11T00:00:00Z");
    var future = TimerSchedule.parse(TimerType.CYCLE, "R2/2026-11-12T00:00:00Z/PT1H");
    assertEquals(now + 86400000, future.first(now, ZoneId.of("UTC")).orElseThrow());
    assertEquals(2, future.repetitions());
    var past = TimerSchedule.parse(TimerType.CYCLE, "R/2026-11-10T00:00:00Z/PT1H");
    assertEquals(now + 3600000, past.first(now, ZoneId.of("UTC")).orElseThrow());
    assertEquals(-1, past.repetitions());
    var timer = new TimerSubscriptionRecord().setDueDate(now).setTimeZone("UTC").setRepetitions(1);
    assertTrue(future.next(timer, now).isEmpty());
    assertEquals(1, TimerSchedule.parse(TimerType.DURATION, "PT1S").repetitions());
  }

  @Test
  void cronUsesSixFieldsAndSkipsMissedOccurrences() {
    var cron = TimerSchedule.parse(TimerType.CRON, "0 * * * * *");
    long now = epoch("2026-11-11T00:00:00Z");
    assertEquals(now + 60000, cron.first(now, ZoneId.of("UTC")).orElseThrow());
    var timer =
        new TimerSubscriptionRecord().setDueDate(now + 60000).setTimeZone("UTC").setRepetitions(-1);
    assertEquals(now + 240000, cron.next(timer, now + 181000).orElseThrow());
    assertEquals(-1, cron.repetitions());
    assertEquals(
        epoch("2026-11-11T01:00:00Z"),
        new CronTimer("0 0 9 * * *").next(now, ZoneId.of("Asia/Shanghai")).orElseThrow());
  }

  @ParameterizedTest
  @ValueSource(strings = {"* * * * *", "61 * * * * *", "0 0 0 32 * *", "broken", ""})
  void rejectsInvalidCron(final String expression) {
    assertThrows(RuntimeException.class, () -> TimerSchedule.parse(TimerType.CRON, expression));
  }

  @Test
  void impossibleCronHasNoExecutionAndMissingTypeIsRejected() {
    assertTrue(new CronTimer("0 0 0 31 2 *").next(TimerFixture.NOW, ZoneId.of("UTC")).isEmpty());
    assertThrows(IllegalArgumentException.class, () -> TimerSchedule.parse(null, "PT1S"));
    assertThrows(
        IllegalArgumentException.class, () -> TimerSchedule.parse(TimerType.DURATION, null));
  }

  private static long epoch(final String time) {
    return Instant.parse(time).toEpochMilli();
  }
}
