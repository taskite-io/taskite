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
package io.taskite.cluster.raft.jraft.runtime;

import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;

/**
 * Bridges jraft's role/leader lifecycle callbacks (on the FSM thread) back to the partition
 * (migration decision "role events"). {@link JRaftStateMachine} invokes it on leader start/stop and
 * follower start/stop; {@link JRaftPartition} fans it out to the business layer's role-change
 * ({@code Consumer<RaftRole>}) and leader-change ({@code Consumer<String>}) listeners, matching the
 * in-house {@code RaftPartition.addRoleChangeListener} / {@code addLeaderChangeListener} contract
 * so the {@code StreamPartition} lifecycle (PROCESSING/REPLAY install/tear-down) is driven
 * identically.
 */
public interface JRaftStateListener {

  /** This node transitioned to {@code role} ({@code LEADER} or {@code FOLLOWER}). */
  void onRoleChange(RaftRole role);

  /**
   * The observed leader changed to {@code leaderId} (a jraft {@code PeerId} string), or {@code
   * null} when leadership is currently unknown (e.g. this node stepped down or lost its leader).
   */
  void onLeaderChange(String leaderId);

  /**
   * The raft state machine hit an unrecoverable error (jraft {@code onError}). Invoked on the FSM
   * thread; implementations must not perform I/O here and must hop to the partition's {@code
   * ConcurrencyControl} before reacting. Default no-op so existing listeners are unaffected.
   */
  default void onFault(final String reason) {}
}
