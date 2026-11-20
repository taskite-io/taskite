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

public enum CommandDistributionIntent implements Intent {
  START((short) 0, false),
  STARTED((short) 1, true),
  DISPATCH((short) 2, false),
  DISPATCHED((short) 3, true),
  RETRY((short) 4, false),
  RETRIED((short) 5, true),
  ACKNOWLEDGE((short) 6, false),
  ACKNOWLEDGED((short) 7, true),
  COMPLETE((short) 8, false),
  COMPLETED((short) 9, true),
  REDISTRIBUTE((short) 10, false),
  REDISTRIBUTED((short) 11, true),
  QUERY((short) 12, false),
  QUERIED((short) 13, true),
  LIST((short) 14, false),
  LISTED((short) 15, true);

  private final short value;
  private final boolean event;

  CommandDistributionIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static CommandDistributionIntent from(final short value) {
    return switch (value) {
      case 0 -> START;
      case 1 -> STARTED;
      case 2 -> DISPATCH;
      case 3 -> DISPATCHED;
      case 4 -> RETRY;
      case 5 -> RETRIED;
      case 6 -> ACKNOWLEDGE;
      case 7 -> ACKNOWLEDGED;
      case 8 -> COMPLETE;
      case 9 -> COMPLETED;
      case 10 -> REDISTRIBUTE;
      case 11 -> REDISTRIBUTED;
      case 12 -> QUERY;
      case 13 -> QUERIED;
      case 14 -> LIST;
      case 15 -> LISTED;
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
