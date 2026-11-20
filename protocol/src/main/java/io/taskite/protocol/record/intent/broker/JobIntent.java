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
package io.taskite.protocol.record.intent.broker;

import io.taskite.protocol.record.intent.Intent;

public enum JobIntent implements Intent {
  CREATED((short) 0, true),
  ACTIVATED((short) 1, true),
  COMPLETE((short) 2, false),
  COMPLETED((short) 3, true),
  QUERY((short) 4, false),
  QUERIED((short) 5, true),
  CANCELED((short) 6, true),
  FAIL((short) 7, false),
  FAILED((short) 8, true),
  UPDATE_RETRIES((short) 9, false),
  RETRIES_UPDATED((short) 10, true),
  TIME_OUT((short) 11, false),
  TIMED_OUT((short) 12, true),
  RECUR((short) 13, false),
  RECURRED_AFTER_BACKOFF((short) 14, true),
  YIELD((short) 15, false),
  YIELDED((short) 16, true),
  UPDATE_TIMEOUT((short) 17, false),
  TIMEOUT_UPDATED((short) 18, true),
  UPDATE_PRIORITY((short) 19, false),
  PRIORITY_UPDATED((short) 20, true),
  CREATE((short) 21, false),
  CANCEL((short) 22, false);

  private final short value;
  private final boolean event;

  JobIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static JobIntent from(final short value) {
    for (final var intent : values()) {
      if (intent.value == value) {
        return intent;
      }
    }
    return null;
  }

  @Override
  public short value() {
    return value;
  }

  @Override
  public boolean isEvent() {
    return event;
  }
}
