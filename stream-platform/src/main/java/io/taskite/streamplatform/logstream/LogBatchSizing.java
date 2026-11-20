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
package io.taskite.streamplatform.logstream;

import io.taskite.streamplatform.api.exception.ExceededBatchRecordSizeException;
import java.util.List;
import java.util.Objects;

/** Single size predicate for the final payload encoded by {@link BatchPayloadCodec}. */
public final class LogBatchSizing {
  public static final int DEFAULT_MAX_BATCH_BYTES = 3 * 1024 * 1024;
  public static final int MIN_CONFIGURED_MAX_BATCH_BYTES = 1024;
  public static final int EMPTY_BATCH_BYTES = Integer.BYTES;

  private static final int RECORD_LENGTH_BYTES = Integer.BYTES;

  private LogBatchSizing() {}

  public static long payloadBytes(final List<LogAppendEntry> entries) {
    long payloadBytes = EMPTY_BATCH_BYTES;
    for (final LogAppendEntry entry : entries) {
      payloadBytes = payloadBytesAfterAppend(payloadBytes, entry);
    }
    return payloadBytes;
  }

  public static long payloadBytesAfterAppend(
      final long currentPayloadBytes, final LogAppendEntry entry) {
    Objects.requireNonNull(entry, "entry");
    return Math.addExact(
        currentPayloadBytes,
        RECORD_LENGTH_BYTES
            + (long) LogEntryDescriptor.HEADER_LENGTH
            + entry.recordMetadata().getLength()
            + entry.recordValue().getLength());
  }

  public static void ensureWithinLimit(final long payloadBytes, final int maxBatchBytes) {
    validateMaxBatchBytes(maxBatchBytes);
    if (payloadBytes > maxBatchBytes) {
      throw new ExceededBatchRecordSizeException(payloadBytes, maxBatchBytes);
    }
  }

  public static void validateMaxBatchBytes(final int maxBatchBytes) {
    if (maxBatchBytes <= EMPTY_BATCH_BYTES) {
      throw new IllegalArgumentException("maxBatchBytes must be greater than " + EMPTY_BATCH_BYTES);
    }
  }

  public static void validateConfiguredMaxBatchBytes(final int maxBatchBytes) {
    if (maxBatchBytes < MIN_CONFIGURED_MAX_BATCH_BYTES) {
      throw new IllegalArgumentException(
          "maxBatchBytes must be at least " + MIN_CONFIGURED_MAX_BATCH_BYTES);
    }
  }
}
