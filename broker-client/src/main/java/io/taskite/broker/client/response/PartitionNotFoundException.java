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

/**
 * Thrown when a command cannot be routed because its target partition is unknown, either because it
 * does not exist or because the gateway topology cache has not learned it yet. The gateway maps
 * this failure to gRPC {@code UNAVAILABLE} (code 14), allowing clients to retry after topology
 * refresh.
 */
public class PartitionNotFoundException extends BrokerClientException {
  private static final String DEFAULT_ERROR_MESSAGE =
      "Expected to execute command on partition %d, but either it does not exist, or the gateway is not yet aware of it";

  private final int partitionId;

  public PartitionNotFoundException(final int partitionId) {
    super(String.format(DEFAULT_ERROR_MESSAGE, partitionId));
    this.partitionId = partitionId;
  }

  public int getPartitionId() {
    return partitionId;
  }
}
