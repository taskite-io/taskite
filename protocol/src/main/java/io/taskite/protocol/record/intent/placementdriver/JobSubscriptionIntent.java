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

public enum JobSubscriptionIntent implements Intent {
  CREATE((short) 0, false),
  CREATED((short) 1, true),
  DELETE((short) 2, false),
  DELETED((short) 3, true);

  private final short value;
  private final boolean event;

  JobSubscriptionIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static JobSubscriptionIntent from(final short value) {
    return switch (value) {
      case 0 -> CREATE;
      case 1 -> CREATED;
      case 2 -> DELETE;
      case 3 -> DELETED;
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
