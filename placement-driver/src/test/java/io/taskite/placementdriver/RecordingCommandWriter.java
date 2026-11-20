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
package io.taskite.placementdriver;

import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.function.Consumer;
import org.agrona.concurrent.UnsafeBuffer;

/** Captures owned commands without creating a submission Future. */
public final class RecordingCommandWriter implements CommandWriter {
  private final Consumer<LoggedEvent> sink;

  public RecordingCommandWriter(final Consumer<LoggedEvent> sink) {
    this.sink = sink;
  }

  @Override
  public void append(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    final var buffer = new UnsafeBuffer(new byte[value.getLength()]);
    value.write(buffer, 0);
    final var copy = PlacementDriverProcessors.createEventRegistry().create(type, intent);
    copy.wrap(buffer);
    sink.accept(
        LoggedEvent.ofRecord(
            key,
            new RecordMetadata().recordType(RecordType.COMMAND).valueType(type).intent(intent),
            copy));
  }
}
