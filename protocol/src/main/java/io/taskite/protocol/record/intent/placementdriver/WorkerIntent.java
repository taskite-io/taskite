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
package io.taskite.protocol.record.intent.placementdriver;

import io.taskite.protocol.record.intent.Intent;

public enum WorkerIntent implements Intent {
  REGISTER((short) 0, false),
  REGISTERED((short) 1, true),
  RENEW((short) 2, false),
  RENEWED((short) 3, true),
  DEREGISTER((short) 4, false),
  DEREGISTERED((short) 5, true),
  MARK_OFFLINE((short) 6, false),
  MARKED_OFFLINE((short) 7, true),
  DELETE((short) 8, false),
  DELETED((short) 9, true);

  private final short value;
  private final boolean event;

  WorkerIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static WorkerIntent from(final short value) {
    return switch (value) {
      case 0 -> REGISTER;
      case 1 -> REGISTERED;
      case 2 -> RENEW;
      case 3 -> RENEWED;
      case 4 -> DEREGISTER;
      case 5 -> DEREGISTERED;
      case 6 -> MARK_OFFLINE;
      case 7 -> MARKED_OFFLINE;
      case 8 -> DELETE;
      case 9 -> DELETED;
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
