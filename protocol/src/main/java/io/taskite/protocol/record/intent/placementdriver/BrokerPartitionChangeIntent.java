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

/** Commands and events driving the PD's per-partition cluster-change orchestration. */
public enum BrokerPartitionChangeIntent implements Intent {
  /** submit a new change plan for a partition. */
  SUBMIT((short) 0, false),
  /** a change plan was accepted and persisted as pending. */
  SUBMITTED((short) 1, true),
  ADMIT((short) 2, false),
  ADMITTED((short) 3, true),
  AUTHORIZE((short) 4, false),
  AUTHORIZED((short) 5, true),
  BLOCK((short) 6, false),
  BLOCKED((short) 7, true),
  EXECUTE((short) 8, false),
  EXECUTION_RESULT((short) 9, false),
  EXECUTION_UPDATED((short) 10, true),
  CREATE((short) 11, false),
  CREATED((short) 12, true),
  JOIN((short) 13, false),
  JOINED((short) 14, true),
  PROMOTE((short) 15, false),
  PROMOTED((short) 16, true),
  CHANGE_PRIORITY((short) 17, false),
  PRIORITY_CHANGED((short) 18, true),
  TRANSFER_LEADER((short) 19, false),
  LEADER_TRANSFERRED((short) 20, true),
  STEP_DOWN_LEADER((short) 21, false),
  LEADER_STEPPED_DOWN((short) 22, true),
  LEAVE((short) 23, false),
  LEFT((short) 24, true),
  RESET_PEERS((short) 25, false),
  PEERS_RESET((short) 26, true),
  /** persist the observable status of the current operation. */
  UPDATE_STATUS((short) 27, false),
  /** the plan's status changed without advancing its operation index. */
  STATUS_UPDATED((short) 28, true),
  SUPERSEDE((short) 29, false),
  SUPERSEDING((short) 30, true),
  REPLACE((short) 31, false),
  REPLACED((short) 32, true),
  /** a change plan was cancelled or failed. */
  CANCELLED((short) 33, true);

  private final short value;
  private final boolean event;

  BrokerPartitionChangeIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static BrokerPartitionChangeIntent from(final short value) {
    return switch (value) {
      case 0 -> SUBMIT;
      case 1 -> SUBMITTED;
      case 2 -> ADMIT;
      case 3 -> ADMITTED;
      case 4 -> AUTHORIZE;
      case 5 -> AUTHORIZED;
      case 6 -> BLOCK;
      case 7 -> BLOCKED;
      case 8 -> EXECUTE;
      case 9 -> EXECUTION_RESULT;
      case 10 -> EXECUTION_UPDATED;
      case 11 -> CREATE;
      case 12 -> CREATED;
      case 13 -> JOIN;
      case 14 -> JOINED;
      case 15 -> PROMOTE;
      case 16 -> PROMOTED;
      case 17 -> CHANGE_PRIORITY;
      case 18 -> PRIORITY_CHANGED;
      case 19 -> TRANSFER_LEADER;
      case 20 -> LEADER_TRANSFERRED;
      case 21 -> STEP_DOWN_LEADER;
      case 22 -> LEADER_STEPPED_DOWN;
      case 23 -> LEAVE;
      case 24 -> LEFT;
      case 25 -> RESET_PEERS;
      case 26 -> PEERS_RESET;
      case 27 -> UPDATE_STATUS;
      case 28 -> STATUS_UPDATED;
      case 29 -> SUPERSEDE;
      case 30 -> SUPERSEDING;
      case 31 -> REPLACE;
      case 32 -> REPLACED;
      case 33 -> CANCELLED;
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
