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
package io.taskite.placementdriver.broker.cluster;

/**
 * The kind of a single {@link TopologyOperation} in a partition {@link ChangePlan}. These are the
 * low-level, per-partition steps the PD sequences to move a partition from its current membership
 * to the desired one, following the "add learner first, then promote, then remove old voter"
 * ordering.
 */
public enum TopologyOperationType {
  /** Bootstrap a new partition on its first voter. */
  ADD,
  /** Add the target broker to the partition as a non-voting learner. */
  JOIN,
  /** Promote the target broker from learner to voter. */
  PROMOTE_LEARNER,
  /** Remove the target broker from the partition's voter/learner set. */
  LEAVE,
  /** Update the target broker's election priority (no membership change). */
  RECONFIGURE_PRIORITY,
  /** Transfer leadership to the target broker. */
  TRANSFER_LEADER,
  /** Transfer leadership when the current broker is not the desired primary. */
  STEP_DOWN_IF_NOT_PRIMARY,
  /**
   * Fenced disaster-recovery membership reset (M5/Q4). Replaces the partition's entire voter set
   * with the single surviving broker without an existing leader, used only by the audited,
   * epoch-fenced quorum-recovery path — never by ordinary reconciliation or rebalance. Must be the
   * only operation in its {@link ChangePlan}.
   */
  RESET_PEERS;

  public static TopologyOperationType parse(final String name) {
    return valueOf(name);
  }
}
