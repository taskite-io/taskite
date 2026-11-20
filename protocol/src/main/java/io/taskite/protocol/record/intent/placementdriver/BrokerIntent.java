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

public enum BrokerIntent implements Intent {
  REGISTER((short) 0, false),
  REGISTERED((short) 1, true),
  RENEW((short) 2, false),
  RENEWED((short) 3, true),
  MARK_OFFLINE((short) 4, false),
  MARKED_OFFLINE((short) 5, true),
  DRAIN((short) 6, false),
  DRAINED((short) 7, true),
  DEREGISTER((short) 8, false),
  DEREGISTERED((short) 9, true);

  private final short value;
  private final boolean event;

  BrokerIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static BrokerIntent from(final short value) {
    return switch (value) {
      case 0 -> REGISTER;
      case 1 -> REGISTERED;
      case 2 -> RENEW;
      case 3 -> RENEWED;
      case 4 -> MARK_OFFLINE;
      case 5 -> MARKED_OFFLINE;
      case 6 -> DRAIN;
      case 7 -> DRAINED;
      case 8 -> DEREGISTER;
      case 9 -> DEREGISTERED;
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
