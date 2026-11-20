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
package io.taskite.streamplatform.runtime.transition;

import io.taskite.cluster.raft.RaftRole;

/**
 * The role a partition's business services are installed for, the target of a {@link
 * PartitionTransition}. Distinct from {@link RaftRole} because the business layer collapses every
 * non-leader consensus role to {@code FOLLOWER} (replay-only) and adds {@code INACTIVE}, entered
 * while importing a snapshot in place and on shutdown.
 */
public enum PartitionRole {
  LEADER,
  FOLLOWER,
  INACTIVE;

  /**
   * Maps a consensus role to the business role to install: only {@link RaftRole#LEADER} runs the
   * processing loop; candidates, followers and learners all replay.
   */
  public static PartitionRole fromRaftRole(final RaftRole raftRole) {
    return raftRole == RaftRole.LEADER ? LEADER : FOLLOWER;
  }
}
