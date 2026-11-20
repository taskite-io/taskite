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

public enum BrokerPartitionOperationIntent implements Intent {
  ADD((short) 0, false),
  ADDED((short) 1, true),
  JOIN((short) 2, false),
  JOINED((short) 3, true),
  PROMOTE_LEARNER((short) 4, false),
  LEARNER_PROMOTED((short) 5, true),
  LEAVE((short) 6, false),
  LEFT((short) 7, true),
  RECONFIGURE_PRIORITY((short) 8, false),
  PRIORITY_RECONFIGURED((short) 9, true),
  TRANSFER_LEADER((short) 10, false),
  LEADER_TRANSFERRED((short) 11, true),
  STEP_DOWN_IF_NOT_PRIMARY((short) 12, false),
  STEPPED_DOWN_IF_NOT_PRIMARY((short) 13, true),
  RESET_PEERS((short) 14, false),
  PEERS_RESET((short) 15, true);
  private final short value;
  private final boolean event;

  BrokerPartitionOperationIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static BrokerPartitionOperationIntent from(final short value) {
    return value >= 0 && value < values().length ? values()[value] : null;
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
