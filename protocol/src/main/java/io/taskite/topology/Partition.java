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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record Partition(
    int partitionId,
    Leadership leadership,
    Membership membership,
    PartitionStatus status,
    boolean serviceReady) {

  public Partition(
      final int partitionId,
      final Leadership leadership,
      final Membership membership,
      final PartitionStatus status) {
    this(
        partitionId,
        leadership,
        membership,
        status,
        status == PartitionStatus.ACTIVE && !leadership.leaderId().isBlank());
  }

  public String leaderId() {
    return leadership.leaderId();
  }

  public long term() {
    return leadership.term();
  }

  public List<String> replicas() {
    return membership.replicas();
  }

  public List<String> learners() {
    return membership.learners();
  }

  public Map<String, Integer> priorities() {
    return membership.priorities();
  }

  public long recoveryEpoch() {
    return membership.recoveryEpoch();
  }

  public Partition {
    Objects.requireNonNull(leadership, "leadership");
    Objects.requireNonNull(membership, "membership");
    final LinkedHashSet<String> followers = new LinkedHashSet<>(membership.replicas());
    followers.remove(leadership.leaderId());
    membership =
        new Membership(
            List.copyOf(followers),
            membership.learners(),
            membership.priorities(),
            membership.recoveryEpoch());
    status = status == null ? PartitionStatus.ACTIVE : status;
    serviceReady =
        serviceReady
            && !leadership.leaderId().isBlank()
            && (status == PartitionStatus.ACTIVE || status == PartitionStatus.REBALANCING);
  }

  public record Leadership(String leaderId, long term) {
    public Leadership {
      leaderId = leaderId == null ? "" : leaderId;
    }
  }

  public record Membership(
      List<String> replicas,
      List<String> learners,
      Map<String, Integer> priorities,
      long recoveryEpoch) {
    public Membership {
      replicas = List.copyOf(replicas == null ? List.of() : replicas);
      learners = List.copyOf(learners == null ? List.of() : learners);
      priorities = Map.copyOf(priorities == null ? Map.of() : priorities);
      recoveryEpoch = Math.max(0L, recoveryEpoch);
    }
  }

  /** Complete raft voter set: the current leader followed by all follower replicas. */
  public List<String> voters() {
    final LinkedHashSet<String> voters = new LinkedHashSet<>();
    if (!leaderId().isBlank()) {
      voters.add(leaderId());
    }
    voters.addAll(replicas());
    return List.copyOf(voters);
  }
}
