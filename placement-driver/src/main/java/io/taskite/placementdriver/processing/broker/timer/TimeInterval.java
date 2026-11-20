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

import java.time.Duration;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

/** ISO8601 interval: calendar units remain a Period; clock units remain a Duration. */
final class TimeInterval {
  private static final Pattern FORMAT =
      Pattern.compile(
          "P(?:(?:\\d+Y)?(?:\\d+M)?(?:\\d+W)?(?:\\d+D)?)(?:T(?:\\d+H)?(?:\\d+M)?(?:\\d+(?:\\.\\d+)?S)?)?");
  private final Period period;
  private final Duration duration;

  private TimeInterval(final Period period, final Duration duration) {
    this.period = period;
    this.duration = duration;
  }

  static TimeInterval parse(final String expression) {
    if (!FORMAT.matcher(expression).matches() || expression.endsWith("T")) {
      throw new IllegalArgumentException("Expected a positive ISO8601 interval");
    }
    final int time = expression.indexOf('T');
    final String datePart = time < 0 ? expression : expression.substring(0, time);
    final Period period = datePart.equals("P") ? Period.ZERO : Period.parse(datePart);
    final Duration duration =
        time < 0 ? Duration.ZERO : Duration.parse("P" + expression.substring(time));
    if ((period.isZero() && duration.isZero()) || duration.getNano() % 1_000_000 != 0) {
      throw new IllegalArgumentException("Interval must be positive with millisecond precision");
    }
    return new TimeInterval(period, duration);
  }

  long addTo(final ZonedDateTime from) {
    final long result = from.plus(period).plus(duration).toInstant().toEpochMilli();
    if (result <= from.toInstant().toEpochMilli()) {
      throw new IllegalArgumentException("Interval must advance time");
    }
    return result;
  }
}
