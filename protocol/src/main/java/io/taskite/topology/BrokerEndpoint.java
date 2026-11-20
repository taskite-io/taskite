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
package io.taskite.topology;

import io.taskite.util.HostPort;

/**
 * Advertised Broker endpoints.
 *
 * <p>{@code port} serves the public business gRPC API. {@code raftPort} serves only
 * Broker-to-Broker Raft traffic. They are explicit and independent because Raft replication has
 * different connectivity, load, timeout, and access-control requirements and is not multiplexed by
 * the business gRPC server.
 */
public record BrokerEndpoint(String brokerId, String host, int port, int raftPort) {
  public BrokerEndpoint {
    host = host == null ? null : HostPort.normalizeHost(host);
  }
}
