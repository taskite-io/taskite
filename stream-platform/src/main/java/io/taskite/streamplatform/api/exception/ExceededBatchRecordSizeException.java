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
package io.taskite.streamplatform.api.exception;

/** Raised before an atomic log batch can exceed its configured final payload limit. */
public final class ExceededBatchRecordSizeException extends RuntimeException {
  private final long actualBytes;
  private final int maxBytes;

  public ExceededBatchRecordSizeException(final long actualBytes, final int maxBytes) {
    super(
        "Log batch payload requires %d bytes but the maximum is %d"
            .formatted(actualBytes, maxBytes));
    this.actualBytes = actualBytes;
    this.maxBytes = maxBytes;
  }

  public long actualBytes() {
    return actualBytes;
  }

  public int maxBytes() {
    return maxBytes;
  }

  public static boolean causedBy(final Throwable error) {
    Throwable cause = error;
    while (cause != null) {
      if (cause instanceof ExceededBatchRecordSizeException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }
}
