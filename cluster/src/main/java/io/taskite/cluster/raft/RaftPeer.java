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
package io.taskite.cluster.raft;

import io.taskite.util.HostPort;

public record RaftPeer(String brokerId, String host, int port, boolean voter, int priority) {
  public RaftPeer {
    host = host == null ? null : HostPort.normalizeHost(host);
    if ((voter && priority <= 0) || (!voter && priority != 0)) {
      throw new IllegalArgumentException(
          "Voter priority must be positive; learner priority must be zero");
    }
  }

  public RaftPeer(final String brokerId, final String host, final int port, final boolean voter) {
    this(brokerId, host, port, voter, voter ? 10 : 0);
  }

  public RaftPeer(String brokerId, String host, int port) {
    this(brokerId, host, port, true, 10);
  }
}
