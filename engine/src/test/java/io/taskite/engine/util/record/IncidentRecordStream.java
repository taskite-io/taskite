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

import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.value.broker.ErrorType;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public final class IncidentRecordStream extends RecordStream<IncidentRecord, IncidentRecordStream> {
  IncidentRecordStream(
      final RecordingExporter exporter,
      final Function<RecordingExporter.ReadContext, Stream<Record<IncidentRecord>>> source,
      final Duration timeout,
      final boolean bounded) {
    super(exporter, source, timeout, bounded);
  }

  @Override
  protected IncidentRecordStream create(
      final Function<RecordingExporter.ReadContext, Stream<Record<IncidentRecord>>> next,
      final Duration wait,
      final boolean bounded) {
    return new IncidentRecordStream(exporter, next, wait, bounded);
  }

  public IncidentRecordStream withJobKey(final long key) {
    return valueFilter(value -> value.getJobKey() == key);
  }

  public IncidentRecordStream withErrorType(final ErrorType type) {
    return valueFilter(value -> value.getErrorType() == type);
  }

  public IncidentRecordStream withTenantId(final String tenant) {
    return valueFilter(value -> Objects.equals(value.getTenantId(), tenant));
  }
}
