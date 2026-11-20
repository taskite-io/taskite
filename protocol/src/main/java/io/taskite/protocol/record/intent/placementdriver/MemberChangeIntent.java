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

/** Shared lifecycle for Broker and placement-driver member changes. */
public enum MemberChangeIntent implements Intent {
  REQUEST((short) 0, false),
  PLANNED((short) 1, true),
  EXECUTE((short) 2, false),
  EXECUTION_RESULT((short) 3, false),
  EXECUTION_UPDATED((short) 4, true),
  JOIN((short) 5, false),
  JOINED((short) 6, true),
  PROMOTE((short) 7, false),
  PROMOTED((short) 8, true),
  TRANSFER_LEADER((short) 9, false),
  LEADER_TRANSFERRED((short) 10, true),
  LEAVE((short) 11, false),
  LEFT((short) 12, true),
  ADVANCE((short) 13, false),
  ADVANCED((short) 14, true),
  CANCELLED((short) 15, true);

  private final short value;
  private final boolean event;

  MemberChangeIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static MemberChangeIntent from(final short value) {
    return switch (value) {
      case 0 -> REQUEST;
      case 1 -> PLANNED;
      case 2 -> EXECUTE;
      case 3 -> EXECUTION_RESULT;
      case 4 -> EXECUTION_UPDATED;
      case 5 -> JOIN;
      case 6 -> JOINED;
      case 7 -> PROMOTE;
      case 8 -> PROMOTED;
      case 9 -> TRANSFER_LEADER;
      case 10 -> LEADER_TRANSFERRED;
      case 11 -> LEAVE;
      case 12 -> LEFT;
      case 13 -> ADVANCE;
      case 14 -> ADVANCED;
      case 15 -> CANCELLED;
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
