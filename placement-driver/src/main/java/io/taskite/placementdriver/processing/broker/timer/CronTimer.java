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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.OptionalLong;

/** Uses the same SPRING53 six-field cron semantics as the referenced CronTimer. */
final class CronTimer {
  private final ExecutionTime execution;

  CronTimer(final String expression) {
    final var cron =
        new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING53))
            .parse(expression);
    cron.validate();
    execution = ExecutionTime.forCron(cron);
  }

  OptionalLong next(final long from, final ZoneId zone) {
    final var result =
        execution.nextExecution(ZonedDateTime.ofInstant(Instant.ofEpochMilli(from), zone));
    return result.isPresent()
        ? OptionalLong.of(result.get().toInstant().toEpochMilli())
        : OptionalLong.empty();
  }
}
