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
package io.taskite.cluster.raft.group;

import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import java.util.List;
import java.util.Objects;

public record PartitionGroupDescriptor(
    int groupId,
    String groupName,
    long term,
    RaftRole role,
    List<RaftPeer> peers,
    long recoveryEpoch) {
  public PartitionGroupDescriptor {
    Objects.requireNonNull(groupName, "groupName");
    Objects.requireNonNull(role, "role");
    peers = List.copyOf(peers == null ? List.of() : peers);
    recoveryEpoch = Math.max(0L, recoveryEpoch);
  }

  /**
   * Backwards-compatible descriptor without an explicit fenced-recovery epoch (Q7), which defaults
   * to {@code 0}. Only the PD-driven topology path supplies a real per-partition recovery epoch;
   * bootstrap and tests stay unchanged.
   */
  public PartitionGroupDescriptor(
      final int groupId,
      final String groupName,
      final long term,
      final RaftRole role,
      final List<RaftPeer> peers) {
    this(groupId, groupName, term, role, peers, 0L);
  }
}
