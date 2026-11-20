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
package io.taskite.protocol.impl.record.value.placementdriver.commanddistribution;

import io.taskite.protocol.record.UnifiedRecordValue;

public final class CommandDistributionRedistributeRecord extends UnifiedRecordValue {
  private long distributionKey;
  private long expectedGeneration;
  private long requestedAtEpochMillis;

  public long getDistributionKey() {
    return distributionKey;
  }

  public CommandDistributionRedistributeRecord setDistributionKey(final long value) {
    distributionKey = value;
    return this;
  }

  public long getExpectedGeneration() {
    return expectedGeneration;
  }

  public CommandDistributionRedistributeRecord setExpectedGeneration(final long value) {
    expectedGeneration = value;
    return this;
  }

  public long getRequestedAtEpochMillis() {
    return requestedAtEpochMillis;
  }

  public CommandDistributionRedistributeRecord setRequestedAtEpochMillis(final long value) {
    requestedAtEpochMillis = value;
    return this;
  }
}
