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
package io.taskite.broker.client.response;

import io.taskite.topology.PartitionStatus;

/** Signals that a known partition is temporarily unavailable while topology changes converge. */
public final class PartitionNotReadyException extends BrokerClientException {
  private final int partitionId;

  public PartitionNotReadyException() {
    super("Expected to route request, but no partition is ready");
    partitionId = -1;
  }

  public PartitionNotReadyException(final int partitionId, final PartitionStatus status) {
    super(
        String.format(
            "Expected to route request to partition %d, but partition is not ready (status=%s)",
            partitionId, status));
    this.partitionId = partitionId;
  }

  public int partitionId() {
    return partitionId;
  }
}
