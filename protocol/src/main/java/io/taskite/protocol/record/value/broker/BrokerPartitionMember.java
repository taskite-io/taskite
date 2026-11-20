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
package io.taskite.protocol.record.value.broker;

/** Frozen consensus membership; transport addresses are resolved from current Broker topology. */
public record BrokerPartitionMember(String brokerId, boolean voter, int priority) {
  public BrokerPartitionMember(final String brokerId, final boolean voter) {
    this(brokerId, voter, voter ? 10 : 0);
  }

  public BrokerPartitionMember {
    if (brokerId == null
        || brokerId.isBlank()
        || voter && priority <= 0
        || !voter && priority != 0) {
      throw new IllegalArgumentException("invalid partition member");
    }
  }
}
