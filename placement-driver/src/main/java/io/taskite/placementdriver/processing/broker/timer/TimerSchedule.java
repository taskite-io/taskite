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

import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.OptionalLong;

/** Pure expression validation and calculation, independent of processors and persistence. */
public final class TimerSchedule {
  private final TimeInterval interval;
  private final CronTimer cron;
  private final ZonedDateTime start;
  private final int repetitions;

  private TimerSchedule(
      final TimeInterval interval,
      final CronTimer cron,
      final ZonedDateTime start,
      final int repetitions) {
    this.interval = interval;
    this.cron = cron;
    this.start = start;
    this.repetitions = repetitions;
  }

  public static TimerSchedule parse(final TimerType type, final String expression) {
    if (expression == null || expression.isBlank()) {
      throw new IllegalArgumentException("expression is required");
    }
    final String value = expression.trim();
    if (type == TimerType.DURATION) {
      return new TimerSchedule(TimeInterval.parse(value), null, null, 1);
    }
    if (type == TimerType.CRON) {
      return new TimerSchedule(null, new CronTimer(value), null, -1);
    }
    if (type != TimerType.CYCLE) {
      throw new IllegalArgumentException("type must be DURATION, CYCLE or CRON");
    }
    final String[] parts = value.split("/", -1);
    if ((parts.length != 2 && parts.length != 3) || !parts[0].matches("R\\d*")) {
      throw new IllegalArgumentException("Expected R[n]/[ISO8601 start/]interval");
    }
    final int count = parts[0].equals("R") ? -1 : Integer.parseInt(parts[0].substring(1));
    if (count != -1 && count < 1) {
      throw new IllegalArgumentException("cycle repetitions must be positive");
    }
    final ZonedDateTime start = parts.length == 3 ? ZonedDateTime.parse(parts[1]) : null;
    return new TimerSchedule(TimeInterval.parse(parts[parts.length - 1]), null, start, count);
  }

  public int repetitions() {
    return repetitions;
  }

  public OptionalLong first(final long from, final ZoneId zone) {
    if (cron != null) {
      return cron.next(from, zone);
    }
    final var time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), zone);
    // Validate progression even when a future start is used for the first occurrence.
    interval.addTo(time);
    return OptionalLong.of(
        start != null && start.toInstant().toEpochMilli() > from
            ? start.toInstant().toEpochMilli()
            : interval.addTo(time));
  }

  public OptionalLong next(final TimerSubscriptionRecord timer, final long processedAt) {
    if (timer.getRepetitions() == 1) {
      return OptionalLong.empty();
    }
    final ZoneId zone = ZoneId.of(timer.getTimeZone());
    if (cron != null) {
      return cron.next(Math.max(timer.getDueDate(), processedAt), zone);
    }
    return OptionalLong.of(
        interval.addTo(ZonedDateTime.ofInstant(Instant.ofEpochMilli(timer.getDueDate()), zone)));
  }
}
