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
package io.taskite.engine.util.record;

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.Record;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public final class JobRecordStream extends RecordStream<JobRecord, JobRecordStream> {
  JobRecordStream(
      final RecordingExporter exporter,
      final Function<RecordingExporter.ReadContext, Stream<Record<JobRecord>>> source,
      final Duration timeout,
      final boolean bounded) {
    super(exporter, source, timeout, bounded);
  }

  @Override
  protected JobRecordStream create(
      final Function<RecordingExporter.ReadContext, Stream<Record<JobRecord>>> next,
      final Duration wait,
      final boolean bounded) {
    return new JobRecordStream(exporter, next, wait, bounded);
  }

  public JobRecordStream withType(final String type) {
    return valueFilter(value -> Objects.equals(value.getType(), type));
  }

  public JobRecordStream withTenantId(final String tenant) {
    return valueFilter(value -> Objects.equals(value.getTenantId(), tenant));
  }

  public JobRecordStream withWorkerId(final String worker) {
    return valueFilter(value -> Objects.equals(value.getWorkerId(), worker));
  }

  public JobRecordStream withRetries(final int retries) {
    return valueFilter(value -> value.getRetries() == retries);
  }
}
