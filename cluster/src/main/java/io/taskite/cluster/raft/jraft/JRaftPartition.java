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
package io.taskite.cluster.raft.jraft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.log.JRaftAppendClosure;
import io.taskite.cluster.raft.jraft.log.JRaftJournalReader;
import io.taskite.cluster.raft.jraft.log.JRaftLogEntryCodec;
import io.taskite.cluster.raft.jraft.log.JRaftSnapshotProgress;
import io.taskite.cluster.raft.jraft.log.MeasuringJRaftServiceFactory;
import io.taskite.cluster.raft.jraft.membership.JRaftAuthorityCoordinator;
import io.taskite.cluster.raft.jraft.membership.JRaftElectionCoordinator;
import io.taskite.cluster.raft.jraft.membership.JRaftExecutionAuthority;
import io.taskite.cluster.raft.jraft.membership.JRaftMemberView;
import io.taskite.cluster.raft.jraft.membership.JRaftMembershipManager;
import io.taskite.cluster.raft.jraft.membership.JRaftPeerMapper;
import io.taskite.cluster.raft.jraft.membership.JRaftStartupElectionGuard;
import io.taskite.cluster.raft.jraft.runtime.JRaftEndpointDirectory;
import io.taskite.cluster.raft.jraft.runtime.JRaftLogicalRpcFactory;
import io.taskite.cluster.raft.jraft.runtime.JRaftPartitionListeners;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.jraft.runtime.JRaftStateMachine;
import io.taskite.cluster.raft.log.PersistedRaftRecord;
import io.taskite.cluster.raft.log.RaftJournalReader;
import io.taskite.cluster.raft.log.RaftLogListener;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.snapshot.SnapshotProgressSource;
import io.taskite.snapshot.SnapshotResult;
import io.taskite.snapshot.SnapshotStateBackend;
import io.taskite.state.PartitionStorage;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SOFAJRaft-backed partition facade. It owns one group lifecycle and delegates member projection,
 * listener fan-out, log framing, snapshots, and membership coordination to focused components.
 */
public final class JRaftPartition implements RaftPartition, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(JRaftPartition.class);

  private final String groupId;
  private final PeerId serverId;
  private final RaftPartitionConfig config;
  private final Configuration initialConfiguration;
  private final JRaftServer server;
  private final RaftStorageLayout layout;
  private final JRaftStateMachine stateMachine;
  private final JRaftMemberView memberView;
  private final JRaftPartitionListeners listeners;
  private final JRaftMembershipManager membershipManager;
  private final JRaftAuthorityCoordinator authorityCoordinator;
  private final JRaftSnapshotProgress snapshotProgress;
  private final PartitionStorage storage;
  private volatile Node node;
  private final JRaftElectionCoordinator election;
  private final JRaftEndpointDirectory endpoints;

  private JRaftPartition(final Builder builder) {
    config = Objects.requireNonNull(builder.config, "config");
    server = Objects.requireNonNull(builder.server, "server");
    layout =
        RaftStorageLayout.of(Objects.requireNonNull(builder.groupDirectory, "groupDirectory"))
            .create();
    storage = builder.storage;
    groupId = JRaftPeerMapper.groupId(config.clusterId(), config.partitionId());
    final var peerMapper = builder.peerMapper;
    serverId = localPeerId(config, peerMapper);
    endpoints = new JRaftEndpointDirectory(groupId, config.peers(), peerMapper);
    initialConfiguration = peerMapper.configuration(config.peers(), builder.priorityFunction);
    memberView =
        new JRaftMemberView(
            config.localBrokerId(),
            serverId,
            initialConfiguration,
            config.peers(),
            () -> node,
            peerMapper);
    listeners = new JRaftPartitionListeners(memberView, this::leaderId);
    election =
        new JRaftElectionCoordinator(
            new JRaftElectionCoordinator.RecoveryConfiguration(
                layout, initialConfiguration, Duration.ofMillis(100)),
            new JRaftElectionCoordinator.Context(() -> node, listeners::onFault, memberView));
    snapshotProgress = new JRaftSnapshotProgress();
    stateMachine =
        new JRaftStateMachine(groupId, listeners, builder.snapshotBackend, snapshotProgress);
    stateMachine.setConfigurationListener(election::requestReconciliation);
    stateMachine.setSnapshotConfigurationListener(election::snapshotLoaded);
    stateMachine.setSnapshotCertificateSupplier(election::snapshotCertificate);
    membershipManager = new JRaftMembershipManager(groupId, serverId, () -> node, memberView);
    final var executionAuthority = new JRaftExecutionAuthority(layout.metaDir());
    stateMachine.setExecutionAuthority(executionAuthority);
    authorityCoordinator =
        new JRaftAuthorityCoordinator(
            () -> node, this::term, membershipManager, memberView, executionAuthority);
  }

  public static Builder builder(
      final RaftPartitionConfig config, final JRaftServer server, final Path groupDirectory) {
    return new Builder(config, server, groupDirectory);
  }

  public static final class Builder {
    private final RaftPartitionConfig config;
    private final JRaftServer server;
    private final Path groupDirectory;
    private SnapshotStateBackend snapshotBackend;
    private ToIntFunction<RaftPeer> priorityFunction;
    private JRaftPeerMapper peerMapper = JRaftPeerMapper.physical();
    private PartitionStorage storage;

    private Builder(
        final RaftPartitionConfig config, final JRaftServer server, final Path groupDirectory) {
      this.config = config;
      this.server = server;
      this.groupDirectory = groupDirectory;
    }

    public Builder snapshotBackend(final SnapshotStateBackend snapshotBackend) {
      this.snapshotBackend = snapshotBackend;
      return this;
    }

    public Builder priorityFn(final ToIntFunction<RaftPeer> priorityFunction) {
      this.priorityFunction = priorityFunction;
      return this;
    }

    /** Uses stable Broker identities; PD groups retain their physical peer configuration. */
    public Builder logicalMemberIdentity() {
      peerMapper = JRaftPeerMapper.logical(config.clusterId());
      return this;
    }

    public Builder storage(final PartitionStorage storage) {
      this.storage = storage;
      return this;
    }

    public JRaftPartition build() {
      return new JRaftPartition(this);
    }
  }

  public void start() {
    if (node != null) {
      return;
    }
    final NodeOptions options = new NodeOptions();
    if (!config.electionTimeout().isZero()) {
      options.setElectionTimeoutMs(Math.toIntExact(config.electionTimeout().toMillis()));
    }
    options.setElectionPriority(election.startupPriority(serverId));
    // Priority is a preference during JRaft's first two election timeouts. Then permit every
    // positive-priority voter to campaign, even when the preferred replica has an older log.
    // PreVote, log freshness, quorum and learner exclusion remain enforced by JRaft.
    options.setDecayPriorityGap(Integer.MAX_VALUE);
    options.setSnapshotIntervalSecs(0);
    options.setServiceFactory(
        new MeasuringJRaftServiceFactory(snapshotProgress, server.endpoint()));
    JRaftStartupElectionGuard.verify(groupId, serverId, layout, options.getElectionPriority());
    node =
        JRaftLogicalRpcFactory.inGroup(
            endpoints,
            () ->
                server.addGroup(
                    groupId, serverId, initialConfiguration, stateMachine, layout, options));
    try {
      election.start();
    } catch (final RuntimeException error) {
      server.removeGroup(groupId);
      node = null;
      throw error;
    }
    server.start();
    LOG.info(
        "Started jraft partition group {} (self={}, electionPriority={})",
        groupId,
        serverId,
        options.getElectionPriority());
  }

  public String groupId() {
    return groupId;
  }

  public PeerId serverId() {
    return serverId;
  }

  public Node node() {
    return node;
  }

  public TaskiteElectionLifecycle.Observation electionObservation() {
    return election.observation();
  }

  @Override
  public ElectionState electionState() {
    return election.state();
  }

  @Override
  public int partitionId() {
    return config.partitionId();
  }

  @Override
  public RaftRole role() {
    final Node current = node;
    return current != null && current.isLeader() ? RaftRole.LEADER : RaftRole.FOLLOWER;
  }

  @Override
  public long term() {
    return stateMachine.currentTerm();
  }

  @Override
  public String leaderId() {
    return memberView.leaderId();
  }

  @Override
  public List<RaftMember> members() {
    final var observation = electionObservation();
    return observation == null ? List.of() : memberView.members(observation.configuration());
  }

  public String appliedConfigurationDigest() {
    return election.digest();
  }

  public void observeTopology(final List<RaftPeer> peers) {
    endpoints.update(peers);
    memberView.observeTopology(peers);
  }

  public void observePeerIdentities(final List<RaftPeer> peers) {
    endpoints.remember(peers);
    memberView.observePeerIdentities(peers);
  }

  @Override
  public long committedPosition() {
    return stateMachine.committedPosition();
  }

  @Override
  public long committedIndex() {
    final Node current = node;
    return current == null ? 0 : current.getLastCommittedIndex();
  }

  @Override
  public long lastIndex() {
    final Node current = node;
    return current == null ? 0 : current.getLastLogIndex();
  }

  @Override
  public long lastAppliedIndex() {
    final Node current = node;
    return current == null ? 0 : current.getLastAppliedLogIndex();
  }

  @Override
  public PartitionStorage storage() {
    return storage;
  }

  @Override
  public Path runtimeDirectory() {
    return layout.partitionDirectory();
  }

  @Override
  public void setAppliedPositionSupplier(final LongSupplier appliedPositionSupplier) {
    stateMachine.setProcessedPositionSupplier(appliedPositionSupplier);
  }

  @Override
  public void subscribe(final RaftLogListener listener) {
    stateMachine.setLogListener(listener);
  }

  @Override
  public void addRoleChangeListener(final Consumer<RaftRole> listener) {
    listeners.addRoleListener(listener);
  }

  @Override
  public void addLeaderChangeListener(final Consumer<String> listener) {
    listeners.addLeaderListener(listener);
  }

  public void addFaultListener(final Consumer<String> listener) {
    listeners.addFaultListener(listener);
  }

  public SnapshotProgressSource snapshotProgress() {
    return snapshotProgress;
  }

  @Override
  public ActorFuture<PersistedRaftRecord> append(
      final long lowestPosition, final long highestPosition, final byte[] data) {
    final Node current = node;
    if (current == null) {
      return notStarted();
    }
    final byte[] payload = data == null ? new byte[0] : data;
    final CompletableActorFuture<PersistedRaftRecord> result = new CompletableActorFuture<>();
    final Task task = new Task();
    task.setData(JRaftLogEntryCodec.encode(lowestPosition, highestPosition, payload));
    task.setDone(new JRaftAppendClosure(lowestPosition, highestPosition, payload, result));
    current.apply(task);
    return result;
  }

  @Override
  public RaftJournalReader openLogReader() {
    final Node current = node;
    if (current == null) {
      throw new IllegalStateException("partition not started: " + groupId);
    }
    return new JRaftJournalReader(current);
  }

  @Override
  public long snapshotPosition() {
    return stateMachine.snapshotPosition();
  }

  public ActorFuture<Void> snapshot() {
    return takeSnapshot().thenApply(ignored -> null);
  }

  public ActorFuture<SnapshotResult> takeSnapshot() {
    final Node current = node;
    if (current == null) {
      return notStarted();
    }
    final CompletableActorFuture<SnapshotResult> result = new CompletableActorFuture<>();
    final long saveGeneration = stateMachine.snapshotSaveGeneration();
    current.snapshot(
        (final Status status) -> {
          if (status.isOk()) {
            final long snapshotIndex = stateMachine.lastSavedSnapshotIndex();
            final long snapshotTerm = stateMachine.lastSavedSnapshotTerm();
            if (stateMachine.snapshotSaveGeneration() == saveGeneration) {
              result.complete(SnapshotResult.skipped(snapshotIndex, snapshotTerm));
            } else {
              final long snapshotSizeBytes = stateMachine.lastSavedSnapshotSizeBytes();
              snapshotProgress.onSnapshotPersisted(snapshotIndex, snapshotSizeBytes);
              result.complete(
                  SnapshotResult.persisted(snapshotIndex, snapshotTerm, snapshotSizeBytes));
            }
          } else {
            result.completeExceptionally(new IllegalStateException("snapshot failed: " + status));
          }
        });
    return result;
  }

  public ActorFuture<Void> updateRaftMembership(final List<RaftPeer> peers) {
    return updateRaftMembership(peers, 0, "");
  }

  public ActorFuture<Void> updateRaftMembership(
      final List<RaftPeer> peers, final long generation, final String owner) {
    endpoints.remember(peers);
    return membershipManager.updateMembership(peers, () -> executionAuthorized(generation, owner));
  }

  @Override
  public ExecutionAuthority executionAuthority() {
    return authorityCoordinator.current();
  }

  @Override
  public ActorFuture<CommittedConfiguration> readCommittedConfiguration() {
    return authorityCoordinator.readConfiguration();
  }

  @Override
  public boolean executionAuthorized(final long generation, final String owner) {
    return authorityCoordinator.authorized(generation, owner);
  }

  @Override
  public ActorFuture<ExecutionAuthority> installExecutionAuthority(
      final long expectedGeneration, final long generation, final String owner) {
    return authorityCoordinator.install(expectedGeneration, generation, owner);
  }

  public ActorFuture<Void> addRaftLearners(final List<RaftPeer> peers) {
    endpoints.remember(peers);
    return membershipManager.admitLearners(peers, () -> executionAuthorized(0, ""));
  }

  public ActorFuture<Void> resetRaftMembership(final List<RaftPeer> peers) {
    endpoints.remember(peers);
    return membershipManager.resetMembership(peers);
  }

  public ActorFuture<Void> resetRaftMembership(
      final List<RaftPeer> peers, final String authorizedSourceDigest) {
    final var source = electionObservation();
    if (source == null || !authorizedSourceDigest.equals(appliedConfigurationDigest())) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalStateException("STALE_CONFIGURATION"));
    }
    endpoints.remember(peers);
    return membershipManager.resetMembership(peers, source);
  }

  public List<RaftPeer> appliedPeers() {
    return authorityCoordinator.appliedPeers(electionObservation());
  }

  public ActorFuture<Void> transferLeadership(final String targetMemberId) {
    return transferLeadership(targetMemberId, 0, "");
  }

  public ActorFuture<Void> transferLeadership(
      final String targetMemberId, final long generation, final String owner) {
    return membershipManager.transferLeadership(
        targetMemberId, () -> executionAuthorized(generation, owner));
  }

  @Override
  public void close() {
    final boolean started;
    election.close();
    synchronized (this) {
      started = node != null;
      node = null;
    }
    if (started) {
      server.removeGroup(groupId);
    }
  }

  private static PeerId localPeerId(
      final RaftPartitionConfig config, final JRaftPeerMapper peerMapper) {
    return config.peers().stream()
        .filter(peer -> peer.brokerId().equals(config.localBrokerId()))
        .findFirst()
        .map(peerMapper::peerId)
        .orElseGet(PeerId::new);
  }

  private <T> ActorFuture<T> notStarted() {
    return CompletableActorFuture.completedExceptionally(
        new IllegalStateException("partition not started: " + groupId));
  }
}
