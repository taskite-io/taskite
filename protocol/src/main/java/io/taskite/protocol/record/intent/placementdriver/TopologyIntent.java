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

public enum TopologyIntent implements Intent {
  CREATE_CLUSTER((short) 0, false),
  CLUSTER_CREATED((short) 1, true),
  CONFIGURE_CLUSTER((short) 2, false),
  CLUSTER_CONFIGURED((short) 3, true),
  UPDATE((short) 4, false),
  UPDATED((short) 5, true),
  COORDINATE((short) 6, false),
  COORDINATED((short) 7, true),
  CANCEL_CLUSTER_CHANGE((short) 8, false),
  CLUSTER_CHANGE_CANCELLED((short) 9, true),
  DELETE_CLUSTER((short) 10, false),
  CLUSTER_DELETED((short) 11, true),
  QUERY((short) 12, false),
  QUERIED((short) 13, true);

  private final short value;
  private final boolean event;

  TopologyIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static TopologyIntent from(final short value) {
    return switch (value) {
      case 0 -> CREATE_CLUSTER;
      case 1 -> CLUSTER_CREATED;
      case 2 -> CONFIGURE_CLUSTER;
      case 3 -> CLUSTER_CONFIGURED;
      case 4 -> UPDATE;
      case 5 -> UPDATED;
      case 6 -> COORDINATE;
      case 7 -> COORDINATED;
      case 8 -> CANCEL_CLUSTER_CHANGE;
      case 9 -> CLUSTER_CHANGE_CANCELLED;
      case 10 -> DELETE_CLUSTER;
      case 11 -> CLUSTER_DELETED;
      case 12 -> QUERY;
      case 13 -> QUERIED;
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
