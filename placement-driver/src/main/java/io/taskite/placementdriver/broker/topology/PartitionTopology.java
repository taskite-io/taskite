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
package io.taskite.placementdriver.broker.topology;

import io.taskite.topology.PartitionStatus;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The PD-internal, persisted placement of a single partition — the source of truth the placement
 * driver drives via topology operations. Leader, raft term and topology version are intentionally
 * absent: leadership is observed from brokers' self-elected raft roles and overlaid at read time,
 * while the version/term come from the PD's own raft partition.
 *
 * <p>In Phase 1 {@code currentVoters} always equals {@code desiredVoters} (a stable round-robin
 * assignment) and {@code status} is {@link PartitionStatus#ACTIVE}; later phases let operations
 * move {@code currentVoters} toward {@code desiredVoters} step by step.
 */
public record PartitionTopology(
    Identity identity,
    Placement placement,
    Membership membership,
    PartitionStatus status,
    Map<String, ReplicaState> replicaStates,
    boolean initializing) {

  public PartitionTopology(
      final Identity identity,
      final Placement placement,
      final Membership membership,
      final PartitionStatus status,
      final Map<String, ReplicaState> replicaStates) {
    this(identity, placement, membership, status, replicaStates, false);
  }

  public PartitionTopology withInitializing(final boolean initializing) {
    return new PartitionTopology(
        identity, placement, membership, status, replicaStates, initializing);
  }

  public static final int PRIMARY_PRIORITY = 100;
  public static final int VOTER_PRIORITY = 10;
  public static final int LEARNER_PRIORITY = 0;

  public String clusterId() {
    return identity.clusterId();
  }

  public int partitionId() {
    return identity.partitionId();
  }

  public int replicationFactor() {
    return placement.replicationFactor();
  }

  public List<String> desiredVoters() {
    return placement.desiredVoters();
  }

  public String desiredPrimary() {
    return placement.desiredPrimary();
  }

  public List<String> currentVoters() {
    return membership.currentVoters();
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

  public PartitionTopology {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(placement, "placement");
    Objects.requireNonNull(membership, "membership");
    status = status == null ? PartitionStatus.ACTIVE : status;
    replicaStates = Map.copyOf(replicaStates == null ? Map.of() : replicaStates);
  }

  public record Identity(String clusterId, int partitionId) {}

  public record Placement(
      int replicationFactor, List<String> desiredVoters, String desiredPrimary) {
    public Placement {
      desiredVoters = List.copyOf(desiredVoters == null ? List.of() : desiredVoters);
      desiredPrimary = desiredPrimary == null ? "" : desiredPrimary;
    }
  }

  public record Membership(
      List<String> currentVoters,
      List<String> learners,
      Map<String, Integer> priorities,
      long recoveryEpoch) {
    public Membership {
      currentVoters = List.copyOf(currentVoters == null ? List.of() : currentVoters);
      learners = List.copyOf(learners == null ? List.of() : learners);
      priorities = Map.copyOf(priorities == null ? Map.of() : priorities);
      recoveryEpoch = Math.max(0L, recoveryEpoch);
    }

    public static Membership initial(
        final List<String> voters, final List<String> learners, final String primary) {
      return new Membership(voters, learners, defaultPriorities(voters, learners, primary), 0);
    }
  }

  private static Map<String, Integer> defaultPriorities(
      final List<String> voters, final List<String> learners, final String desiredPrimary) {
    final Map<String, Integer> priorities = new LinkedHashMap<>();
    if (voters != null) {
      for (final String voter : voters) {
        priorities.put(voter, voter.equals(desiredPrimary) ? PRIMARY_PRIORITY : VOTER_PRIORITY);
      }
    }
    if (learners != null) {
      for (final String learner : learners) {
        priorities.putIfAbsent(learner, LEARNER_PRIORITY);
      }
    }
    return priorities;
  }
}
