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
package io.taskite.broker.partition;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.broker.topology.BrokerTopologyValidation;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.group.PartitionGroupDescriptor;
import io.taskite.cluster.raft.group.PartitionGroupManager;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.topology.BrokerEndpoint;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerPartitionManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerPartitionManager.class);
  private static final int DEFAULT_VOTER_PRIORITY = 10;
  private static final int DEFAULT_LEARNER_PRIORITY = 0;
  private final String localBrokerId;
  private final String localHost;
  private final int localRaftPort;
  private final Path dataRootDirectory;
  private final boolean remoteReplication;
  private final PartitionGroupManager<StreamPartition> groups;
  private final Set<Integer> awaitingAssignments = new HashSet<>();
  private final Map<Integer, Long> pendingCreations = new LinkedHashMap<>();
  private volatile ClusterTopology topology;

  public BrokerPartitionManager(
      final BrokerPartitionManagerConfig config,
      final JRaftServer jraftServer,
      final ActorScheduler scheduler,
      final MeterRegistry meterRegistry) {
    Objects.requireNonNull(config, "config");
    localBrokerId = config.localBrokerId();
    localHost = config.network().host();
    localRaftPort = config.network().raftPort();
    dataRootDirectory = config.dataRootDirectory();
    remoteReplication = config.remoteReplication();
    Objects.requireNonNull(jraftServer, "jraftServer");
    Objects.requireNonNull(scheduler, "scheduler");
    Objects.requireNonNull(meterRegistry, "meterRegistry");
    this.groups =
        new PartitionGroupManager<>(
            descriptor -> {
              final Path groupDir =
                  groupDataDirectory(descriptor.groupName(), descriptor.groupId());
              // Q7: a returning node whose local generation predates a fenced recovery must not
              // reuse its superseded raft log/meta/snapshot — quarantine it and rejoin empty.
              BrokerPartitionRecoveryGuard.quarantineIfStale(groupDir, descriptor.recoveryEpoch());
              return BrokerPartitionServices.createGroup(
                  descriptor, groupDir, config, jraftServer, scheduler, meterRegistry);
            },
            (existing, descriptor) -> {
              // Q7: a topology refresh that carries a newer recovery epoch than this broker has
              // recorded locally means the open group belongs to a superseded generation; refuse
              // reuse so it is closed and recreated through the quarantining factory above.
              final Path groupDir =
                  groupDataDirectory(descriptor.groupName(), descriptor.groupId());
              if (descriptor.recoveryEpoch() > BrokerPartitionRecoveryGuard.localEpoch(groupDir)) {
                return false;
              }
              // A descriptor can come from a frozen operation. Remember its identities without
              // replacing newer addresses; reconcile publishes authoritative topology below.
              existing.observePeerIdentities(descriptor.peers());
              return true;
            });
  }

  public synchronized void reconcile(ClusterTopology newTopology) {
    Objects.requireNonNull(newTopology, "newTopology");
    if (topology != null && newTopology.version() < topology.version()) {
      return;
    }
    final Map<Integer, PartitionGroupDescriptor> localAssignments;
    try {
      localAssignments = localAssignments(newTopology);
    } catch (final IllegalStateException incomplete) {
      // Keep the last usable snapshot and all open groups until PD publishes complete endpoints.
      LOG.warn(
          "broker {} deferred topology reconciliation: {}", localBrokerId, incomplete.getMessage());
      return;
    }
    final List<RaftPeer> endpoints = knownEndpoints(newTopology);
    for (final PartitionGroupDescriptor descriptor : localAssignments.values()) {
      groups.upsertOne(descriptor).observeTopology(endpoints);
      awaitingAssignments.remove(descriptor.groupId());
      pendingCreations.remove(descriptor.groupId());
    }
    pendingCreations
        .entrySet()
        .removeIf(
            entry ->
                newTopology.version() > entry.getValue()
                    && newTopology
                        .partition(entry.getKey())
                        .map(partition -> partition.status() != PartitionStatus.CREATING)
                        .orElse(true));
    for (final StreamPartition group : List.copyOf(groups.groups())) {
      if (!localAssignments.containsKey(group.partitionId())
          && !awaitingAssignments.contains(group.partitionId())
          && !pendingCreations.containsKey(group.partitionId())) {
        groups.remove(group.partitionId());
      } else if (!localAssignments.containsKey(group.partitionId())) {
        group.observeTopology(endpoints);
      }
    }
    topology = newTopology;
  }

  public synchronized List<PartitionGroupDescriptor> localPartitionDescriptors(
      ClusterTopology newTopology) {
    return new ArrayList<>(localAssignments(newTopology).values());
  }

  public synchronized StreamPartition bootstrapPartition(
      ClusterTopology newTopology, PartitionGroupDescriptor descriptor) {
    Objects.requireNonNull(newTopology, "newTopology");
    Objects.requireNonNull(descriptor, "descriptor");
    topology = newTopology;
    if (descriptor.role() == RaftRole.LEARNER) {
      // A joining Broker creates the local learner before PD persists JOIN. Preserve it across
      // topology refreshes until the authoritative assignment appears.
      awaitingAssignments.add(descriptor.groupId());
    }
    return groups.upsertOne(descriptor);
  }

  public Optional<StreamPartition> group(int partitionId) {
    return groups.group(partitionId);
  }

  public Optional<RaftPartition> partition(int partitionId) {
    return groups.group(partitionId).map(StreamPartition::raftPartition);
  }

  public Collection<StreamPartition> groups() {
    return groups.groups();
  }

  public int groupCount() {
    return groups.groupCount();
  }

  public synchronized boolean isAwaitingAssignment(final int partitionId) {
    return awaitingAssignments.contains(partitionId) || pendingCreations.containsKey(partitionId);
  }

  public synchronized boolean isLocalLearner(final int partitionId) {
    if (awaitingAssignments.contains(partitionId)) {
      return true;
    }
    return topology != null
        && topology
            .partition(partitionId)
            .map(partition -> partition.learners().contains(localBrokerId))
            .orElse(false);
  }

  /**
   * The fenced-recovery epoch (Q7) this broker has recorded on disk for {@code partitionId}, read
   * from the pre-open marker. Returns {@code 0} when the partition never went through fenced
   * recovery. Used to report the local generation to the placement driver and to gate promotion
   * readiness until the local epoch has caught up with the topology's target epoch (Q5).
   */
  public long localRecoveryEpoch(final int partitionId) {
    final String clusterId = topology != null ? topology.clusterId() : null;
    if (clusterId == null) {
      return 0L;
    }
    return BrokerPartitionRecoveryGuard.localEpoch(groupDataDirectory(clusterId, partitionId));
  }

  public ClusterTopology topology() {
    return topology;
  }

  @Override
  public void close() {
    groups.close();
  }

  /**
   * Builds the jraft peer set for a partition. Unlike the in-house engine (which excluded the local
   * node), jraft requires the local node to appear in its group's {@code initialConf} to
   * self-elect, so this set <em>includes self</em>. Every remote endpoint uses the explicit Raft
   * port advertised through PD topology; the business API port is never used for Raft traffic.
   */
  private List<RaftPeer> peers(ClusterTopology newTopology, Partition distribution) {
    BrokerTopologyValidation.requireVoterEndpoints(newTopology, distribution, localBrokerId);
    final List<RaftPeer> peers = new ArrayList<>();
    final List<String> voters = distribution.voters();
    final boolean localIsVoter = voters.contains(localBrokerId);
    final boolean localIsLearner = distribution.learners().contains(localBrokerId);
    if (localIsVoter || localIsLearner) {
      // Membership is authoritative: only a listed voter may self-elect. A joining learner
      // remains a non-voter and catches up against the complete persisted voter set.
      final boolean selfVoter = localIsVoter;
      peers.add(
          new RaftPeer(
              localBrokerId,
              localHost,
              localRaftPort,
              selfVoter,
              distribution
                  .priorities()
                  .getOrDefault(
                      localBrokerId,
                      selfVoter ? DEFAULT_VOTER_PRIORITY : DEFAULT_LEARNER_PRIORITY)));
    }
    if (!remoteReplication) {
      return peers;
    }
    for (String brokerId : voters) {
      if (brokerId.equals(localBrokerId)) {
        continue;
      }
      final BrokerEndpoint endpoint = newTopology.brokers().get(brokerId);
      peers.add(
          raftPeer(
              endpoint,
              true,
              distribution.priorities().getOrDefault(brokerId, DEFAULT_VOTER_PRIORITY)));
    }
    for (String brokerId : distribution.learners()) {
      if (brokerId.equals(localBrokerId)) {
        continue;
      }
      BrokerEndpoint endpoint = newTopology.brokers().get(brokerId);
      if (endpoint != null) {
        peers.add(
            raftPeer(
                endpoint,
                false,
                distribution.priorities().getOrDefault(brokerId, DEFAULT_LEARNER_PRIORITY)));
      }
    }
    return peers;
  }

  private static RaftPeer raftPeer(
      final BrokerEndpoint endpoint, final boolean voter, final int priority) {
    return new RaftPeer(endpoint.brokerId(), endpoint.host(), endpoint.raftPort(), voter, priority);
  }

  /** Addresses may belong to recovered members outside the current partition target roster. */
  private List<RaftPeer> knownEndpoints(final ClusterTopology current) {
    return current.brokers().values().stream()
        .filter(
            endpoint ->
                endpoint.host() != null
                    && !endpoint.host().isBlank()
                    && endpoint.raftPort() > 0
                    && endpoint.raftPort() <= 65535)
        .map(
            endpoint ->
                endpoint.brokerId().equals(localBrokerId)
                    ? new RaftPeer(localBrokerId, localHost, localRaftPort, false, 0)
                    : raftPeer(endpoint, false, 0))
        .toList();
  }

  private Map<Integer, PartitionGroupDescriptor> localAssignments(ClusterTopology newTopology) {
    Objects.requireNonNull(newTopology, "newTopology");
    Map<Integer, PartitionGroupDescriptor> localAssignments = new LinkedHashMap<>();
    for (Partition distribution : newTopology.partitions()) {
      RaftRole expectedRole = expectedRole(distribution);
      if (expectedRole != null) {
        localAssignments.put(
            distribution.partitionId(),
            new PartitionGroupDescriptor(
                distribution.partitionId(),
                newTopology.clusterId(),
                distribution.term(),
                expectedRole,
                peers(newTopology, distribution),
                distribution.recoveryEpoch()));
      }
    }
    return localAssignments;
  }

  /**
   * Maps topology membership to an initial raft role. Topology decides <em>membership</em> (which
   * brokers are voters/replicas vs learners); leadership is decided by the broker's own raft
   * election, so a replica always starts as {@code FOLLOWER} and stands for election on timeout —
   * the topology {@code leaderId} is intentionally ignored here.
   */
  private RaftRole expectedRole(Partition distribution) {
    if (distribution.voters().contains(localBrokerId)) {
      return RaftRole.FOLLOWER;
    }
    if (distribution.learners().contains(localBrokerId)) {
      return RaftRole.LEARNER;
    }
    return null;
  }

  synchronized void confirmAssignment(final int partitionId) {
    awaitingAssignments.remove(partitionId);
  }

  synchronized StreamPartition createPartition(final PartitionGroupDescriptor descriptor) {
    // ADD precedes its actual membership publication. Keep the group through same-version
    // refreshes until PD ACK publishes an assignment, or a newer topology cancels the creation.
    pendingCreations.put(descriptor.groupId(), topology == null ? 0L : topology.version());
    try {
      final var group = groups.upsertOne(descriptor);
      if (topology != null) {
        group.observeTopology(knownEndpoints(topology));
      }
      return group;
    } catch (final RuntimeException error) {
      pendingCreations.remove(descriptor.groupId());
      throw error;
    }
  }

  String localBrokerId() {
    return localBrokerId;
  }

  int localRaftPort() {
    return localRaftPort;
  }

  Path groupDataDirectory(String clusterId, int partitionId) {
    if (dataRootDirectory == null) {
      try {
        return Files.createTempDirectory(
            "broker-" + localBrokerId + "-" + clusterId + "-partition-" + partitionId + "-");
      } catch (Exception e) {
        throw new IllegalStateException("failed to create broker partition data directory", e);
      }
    }
    return partitionDataDirectory(dataRootDirectory, clusterId, localBrokerId, partitionId);
  }

  static Path partitionDataDirectory(
      final Path dataRootDirectory,
      final String clusterId,
      final String brokerId,
      final int partitionId) {
    return Objects.requireNonNull(dataRootDirectory, "dataRootDirectory")
        .resolve(clusterId)
        .resolve(brokerId)
        .resolve("partitions")
        .resolve(Integer.toString(partitionId));
  }
}
