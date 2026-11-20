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
 * A single low-level step within a {@link ChangePlan}. Immutable value; the target broker is the
 * subject of the operation and {@code priority} is only meaningful for {@link
 * TopologyOperationType#RECONFIGURE_PRIORITY}.
 *
 * <p>The {@code opId} is the stable idempotency key (see the plan doc §17.3): a broker persists the
 * last-applied {@code opId} so a redelivered operation is not executed twice.
 */
public record TopologyOperation(
    String opId,
    TopologyOperationType type,
    String targetBroker,
    int priority,
    BrokerOperationConfiguration configuration) {

  public TopologyOperation(
      final String opId,
      final TopologyOperationType type,
      final String targetBroker,
      final int priority) {
    this(opId, type, targetBroker, priority, null);
  }

  public TopologyOperation {
    opId = opId == null ? "" : opId;
    targetBroker = targetBroker == null ? "" : targetBroker;
  }

  public static TopologyOperation add(final int partitionId, final String targetBroker) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.ADD, targetBroker, 0),
        TopologyOperationType.ADD,
        targetBroker,
        0);
  }

  public static TopologyOperation join(final int partitionId, final String targetBroker) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.JOIN, targetBroker, 0),
        TopologyOperationType.JOIN,
        targetBroker,
        0);
  }

  public static TopologyOperation promoteLearner(final int partitionId, final String targetBroker) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.PROMOTE_LEARNER, targetBroker, 0),
        TopologyOperationType.PROMOTE_LEARNER,
        targetBroker,
        0);
  }

  public static TopologyOperation leave(final int partitionId, final String targetBroker) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.LEAVE, targetBroker, 0),
        TopologyOperationType.LEAVE,
        targetBroker,
        0);
  }

  public static TopologyOperation reconfigurePriority(
      final int partitionId, final String targetBroker, final int priority) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.RECONFIGURE_PRIORITY, targetBroker, priority),
        TopologyOperationType.RECONFIGURE_PRIORITY,
        targetBroker,
        priority);
  }

  public static TopologyOperation transferLeader(final int partitionId, final String targetBroker) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.TRANSFER_LEADER, targetBroker, 0),
        TopologyOperationType.TRANSFER_LEADER,
        targetBroker,
        0);
  }

  public static TopologyOperation stepDownIfNotPrimary(
      final int partitionId, final String desiredPrimary) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY, desiredPrimary, 0),
        TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY,
        desiredPrimary,
        0);
  }

  /**
   * Fenced disaster-recovery reset: replaces the partition's voter set with the single surviving
   * broker. {@code priority} carries the target recovery epoch (Q6) so the broker can reject a
   * stale reset; it is not an election priority here.
   */
  public static TopologyOperation resetPeers(
      final int partitionId, final String survivor, final int recoveryEpoch) {
    return new TopologyOperation(
        opId(partitionId, TopologyOperationType.RESET_PEERS, survivor, recoveryEpoch),
        TopologyOperationType.RESET_PEERS,
        survivor,
        recoveryEpoch);
  }

  private static String opId(
      final int partitionId,
      final TopologyOperationType type,
      final String targetBroker,
      final int priority) {
    return partitionId + ":" + type.name() + ":" + targetBroker + ":" + priority;
  }
}
