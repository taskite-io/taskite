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
package io.taskite.streamplatform.command;

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogBatchSizing;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Mutable outcomes accumulated while processing one root command and its admitted follow-ups. */
final class ProcessingBatchResult {
  private final List<LogAppendEntry> records = new ArrayList<>();
  private final Map<RequestKey, LoggedEvent> responses = new LinkedHashMap<>();
  private final List<Runnable> postCommitActions = new ArrayList<>();
  private final int maxBatchBytes;
  private final TypedEventRegistry eventRegistry;
  private long sourcePosition = -1L;
  private long payloadBytes = LogBatchSizing.EMPTY_BATCH_BYTES;

  ProcessingBatchResult(final int maxBatchBytes) {
    this(maxBatchBytes, null);
  }

  ProcessingBatchResult(final int maxBatchBytes, final TypedEventRegistry eventRegistry) {
    LogBatchSizing.validateMaxBatchBytes(maxBatchBytes);
    this.maxBatchBytes = maxBatchBytes;
    this.eventRegistry = eventRegistry;
  }

  void reset(final long sourcePosition) {
    this.sourcePosition = sourcePosition;
    records.clear();
    responses.clear();
    postCommitActions.clear();
    payloadBytes = LogBatchSizing.EMPTY_BATCH_BYTES;
  }

  void append(final LogAppendEntry entry) {
    final long nextPayloadBytes = LogBatchSizing.payloadBytesAfterAppend(payloadBytes, entry);
    LogBatchSizing.ensureWithinLimit(nextPayloadBytes, maxBatchBytes);
    records.add(snapshot(entry));
    payloadBytes = nextPayloadBytes;
  }

  void putResponse(
      final String requestStreamId, final String requestId, final LoggedEvent response) {
    responses.put(
        new RequestKey(requestStreamId, requestId),
        snapshot(Objects.requireNonNull(response, "response")));
  }

  void appendPostCommitAction(final Runnable action) {
    postCommitActions.add(Objects.requireNonNull(action, "action"));
  }

  int size() {
    return records.size();
  }

  LogAppendEntry get(final int index) {
    return records.get(index);
  }

  void markProcessed(final int index) {
    records.set(index, LogAppendEntry.ofProcessed(records.get(index)));
  }

  long sourcePosition() {
    return sourcePosition;
  }

  List<LogAppendEntry> records() {
    return List.copyOf(records);
  }

  long payloadBytes() {
    return payloadBytes;
  }

  List<LoggedEvent> responses() {
    return List.copyOf(responses.values());
  }

  List<Runnable> postCommitActions() {
    return List.copyOf(postCommitActions);
  }

  boolean isEmpty() {
    return records.isEmpty() && responses.isEmpty() && postCommitActions.isEmpty();
  }

  List<LoggedEvent> asLoggedEvents() {
    return records.stream()
        .map(
            record ->
                snapshot(
                    LoggedEvent.ofRecord(
                        record.key(), record.recordMetadata(), record.recordValue())))
        .toList();
  }

  private static LogAppendEntry snapshot(final LogAppendEntry entry) {
    final RecordMetadata metadata = entry.recordMetadata().copy();
    return LogAppendEntry.of(entry.key(), metadata, snapshot(entry.recordValue()));
  }

  private LoggedEvent snapshot(final LoggedEvent event) {
    final RecordMetadata metadata = event.metadata().copy();
    final UnifiedRecordValue value;
    if (eventRegistry == null || metadata.getRecordType() == RecordType.COMMAND_REJECTION) {
      value = snapshot(event.value());
    } else {
      final byte[] bytes = bytes(event.value());
      value = eventRegistry.create(metadata.getValueType(), metadata.getIntent());
      value.wrap(new UnsafeBuffer(bytes));
    }
    return LoggedEvent.ofRecord(event.key(), metadata, value);
  }

  private static UnifiedRecordValue snapshot(final UnifiedRecordValue value) {
    return new BufferedRecordValue(bytes(value));
  }

  private static byte[] bytes(final UnifiedRecordValue value) {
    final byte[] bytes = new byte[value.getLength()];
    value.write(new UnsafeBuffer(bytes), 0);
    return bytes;
  }

  private record RequestKey(String requestStreamId, String requestId) {}

  private static final class BufferedRecordValue extends UnifiedRecordValue {
    private final byte[] bytes;

    private BufferedRecordValue(final byte[] bytes) {
      this.bytes = bytes;
    }

    @Override
    public int getLength() {
      return bytes.length;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {
      buffer.putBytes(offset, bytes);
    }
  }
}
