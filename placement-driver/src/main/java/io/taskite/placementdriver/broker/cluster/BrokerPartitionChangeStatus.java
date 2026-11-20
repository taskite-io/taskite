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

/** Persisted lifecycle status used to resume a partition change after a PD restart. */
public enum BrokerPartitionChangeStatus {
  /** Accepted and persisted, not yet started. */
  PENDING,
  /** At least one operation has been applied; more remain. */
  IN_PROGRESS,
  /** Waiting for the current operation's target replica to become ready before advancing. */
  WAITING_READY,
  /** Waiting for leadership to converge onto the desired primary. */
  WAITING_LEADER_CONVERGE,
  /** All operations applied. */
  COMPLETED,
  /** Aborted; will not make further progress. */
  FAILED,
  /** Dispatch stopped; waiting for a committed authority and actual configuration. */
  SUPERSEDING,
  /** Replaced atomically by a linked successor. */
  SUPERSEDED,
  /** Persisted target waiting for migration-budget admission; no Broker operations are allowed. */
  QUEUED
}
