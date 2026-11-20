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

public enum BrokerPartitionConfigurationIntent implements Intent {
  QUERY((short) 0, false),
  QUERIED((short) 1, true),
  INSTALL_AUTHORITY((short) 2, false),
  AUTHORITY_INSTALLED((short) 3, true);
  private final short value;
  private final boolean event;

  BrokerPartitionConfigurationIntent(final short value, final boolean event) {
    this.value = value;
    this.event = event;
  }

  public static BrokerPartitionConfigurationIntent from(final short value) {
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
