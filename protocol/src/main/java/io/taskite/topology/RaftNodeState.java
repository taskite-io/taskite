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

import com.fasterxml.jackson.annotation.JsonCreator;

/** JRaft observation names exposed by the partition election contract. */
public enum RaftNodeState {
  UNKNOWN,
  BOOTSTRAP,
  RECOVERED,
  SNAPSHOT,
  JOINT_SNAPSHOT_BASELINE,
  STATE_LEADER,
  STATE_TRANSFERRING,
  STATE_CANDIDATE,
  STATE_FOLLOWER,
  STATE_ERROR,
  STATE_UNINITIALIZED,
  STATE_SHUTTING,
  STATE_SHUTDOWN,
  STATE_END;

  @JsonCreator
  public static RaftNodeState parse(final String value) {
    return value == null || value.isBlank() ? UNKNOWN : valueOf(value);
  }
}
