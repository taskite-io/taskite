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

import io.taskite.scheduler.future.ActorFuture;

/**
 * One installable/removable concern in a partition's role transition. A partition moves to a new
 * role by running every step's {@link #transitionTo} in order; before that, the steps installed by
 * the previous transition run {@link #prepareTransition} in reverse to relinquish their old
 * services.
 */
public interface PartitionTransitionStep {

  /**
   * Fired immediately when a new raft role is known, on every step, before prepare/transition work.
   * Must return instantly; use it only to cancel ongoing activity that a pending transition would
   * otherwise wait on.
   */
  default void onNewRaftRole(
      final PartitionTransitionContext context, final PartitionRole newRole) {}

  /**
   * Relinquishes the service this step installed in the previous transition so the partition can
   * assume a neutral stance before {@link #transitionTo}. Steps are prepared in reverse order.
   */
  ActorFuture<Void> prepareTransition(
      PartitionTransitionContext context, long term, PartitionRole targetRole);

  /** Installs or removes this step's service for {@code targetRole}. */
  ActorFuture<Void> transitionTo(
      PartitionTransitionContext context, long term, PartitionRole targetRole);

  /** A short name for logging. */
  String getName();
}
