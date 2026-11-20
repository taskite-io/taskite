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

/** Shared durable command intents for Broker fencing and PD progress tracking. */
public enum JobYieldIntent implements Intent {
  FENCE_SESSION(0, false),
  SESSION_FENCED(1, true),
  COMPLETE_YIELD(2, false),
  YIELD_COMPLETED(3, true),
  UPDATE_PROGRESS(4, false),
  PROGRESS_UPDATED(5, true),
  ACKNOWLEDGE(6, false),
  ACKNOWLEDGED(7, true),
  MIGRATE_INDEX(8, false),
  INDEX_MIGRATED(9, true),
  SYNC_SESSIONS(10, false),
  SESSIONS_SYNCED(11, true),
  DISPATCH(12, false),
  SYNC_PARTITION(13, false),
  UPDATE_PARTITION_SYNC(14, false),
  PARTITION_SYNC_UPDATED(15, true);

  private final short value;
  private final boolean event;

  JobYieldIntent(int value, boolean event) {
    this.value = (short) value;
    this.event = event;
  }

  public static JobYieldIntent from(short value) {
    for (var intent : values()) {
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
