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
package io.taskite.engine.util.client;

import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;

/** Native command boundary shared by the fluent single-partition test clients. */
public interface CommandWriter {
  RecordingExporter getRecordingExporter();

  Record<?> writeCommand(long key, ValueType type, Intent intent, UnifiedRecordValue value);

  Record<CommandDistributionRecord> createSubscription(
      String name, String jobType, String tenantId);

  Record<CommandDistributionRecord> deleteSubscription(long key, String tenantId);

  default Record<?> writeCommand(
      final int partitionId,
      final long key,
      final ValueType type,
      final Intent intent,
      final UnifiedRecordValue value) {
    if (partitionId != 1) {
      throw new IllegalArgumentException("EngineRule supports only partition 1");
    }
    return writeCommand(key, type, intent, value);
  }

  /** Waits for the requested outcome, skipping intermediate events in the same command chain. */
  default Record<?> awaitResponse(
      final Record<?> command,
      final Intent eventIntent,
      final Long eventKey,
      final boolean expectRejection) {
    final var response =
        getRecordingExporter()
            .recordStream()
            .withRequestId(((TypedRecord<?>) command).getRequestId())
            .filter(
                record ->
                    record.getRecordType() == RecordType.COMMAND_REJECTION
                        || record.getRecordType() == RecordType.EVENT
                            && record.getValueType() == command.getValueType()
                            && record.getIntent() == eventIntent
                            && (eventKey == null || record.getKey() == eventKey))
            .getFirst();
    final boolean rejected = response.getRecordType() == RecordType.COMMAND_REJECTION;
    if (rejected != expectRejection) {
      throw new AssertionError(
          "Expected "
              + (expectRejection ? "command rejection" : eventIntent)
              + ", received "
              + response.getRecordType()
              + "."
              + response.getIntent()
              + ": "
              + response.getRejectionType()
              + ": "
              + response.getRejectionReason());
    }
    return response;
  }

  default <T extends UnifiedRecordValue> Record<T> send(
      final long key,
      final ValueType type,
      final Intent intent,
      final T value,
      final Intent eventIntent,
      final Long eventKey,
      final Class<T> valueClass) {
    final var response =
        awaitResponse(writeCommand(key, type, intent, value), eventIntent, eventKey, false);
    return getRecordingExporter()
        .recordStream(type, valueClass)
        .withPosition(response.getPosition())
        .getFirst();
  }

  default Record<?> reject(
      final long key,
      final ValueType type,
      final Intent intent,
      final UnifiedRecordValue value,
      final Intent eventIntent,
      final Long eventKey) {
    return awaitResponse(writeCommand(key, type, intent, value), eventIntent, eventKey, true);
  }
}
