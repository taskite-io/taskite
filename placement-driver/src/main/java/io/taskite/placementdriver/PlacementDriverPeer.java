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
package io.taskite.placementdriver;

import io.taskite.util.HostPort;

/**
 * Explicit seed endpoints for one placement-driver member.
 *
 * <p>The gRPC endpoint serves control-plane clients and member discovery. The Raft endpoint serves
 * consensus replication and elections. They are configured independently and are never inferred
 * from each other.
 */
public record PlacementDriverPeer(String memberId, String host, int grpcPort, int raftPort) {
  public PlacementDriverPeer {
    host = host == null ? null : HostPort.normalizeHost(host);
  }
}
