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
package io.taskite.protocol.record.intent.common;

import io.taskite.protocol.record.intent.Intent;

public enum TimerIntent implements Intent {
  CREATED((short) 0, true),
  TRIGGER((short) 1, false),
  TRIGGERED((short) 2, true),
  CANCELED((short) 3, true),
  ASSIGNED((short) 4, true),
  CREATE((short) 5, false),
  CANCEL((short) 6, false);

  private final short value;
  private final boolean event;

  TimerIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static TimerIntent from(final short value) {
    return switch (value) {
      case 0 -> CREATED;
      case 1 -> TRIGGER;
      case 2 -> TRIGGERED;
      case 3 -> CANCELED;
      case 4 -> ASSIGNED;
      case 5 -> CREATE;
      case 6 -> CANCEL;
      default -> null;
    };
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
