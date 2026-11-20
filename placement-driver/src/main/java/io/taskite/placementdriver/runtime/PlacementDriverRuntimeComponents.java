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
package io.taskite.placementdriver.runtime;

import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.PlacementDriverPeer;
import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCalculator;
import io.taskite.placementdriver.broker.topology.BrokerTopologyCommittedRecordListener;
import io.taskite.placementdriver.broker.topology.BrokerTopologyWatchListener;
import io.taskite.placementdriver.broker.topology.BrokerTopologyWatchPublisher;
import io.taskite.placementdriver.metrics.PlacementDriverMetrics;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.worker.registry.WorkerRegistration;
import io.taskite.protocol.impl.record.value.placementdriver.job.JobYieldPlan;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import io.taskite.util.HostPort;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/** Leader-scoped actors, committed-record reactions, and topology watch publishers. */
public final class PlacementDriverRuntimeComponents
    implements AutoCloseable, PlacementDriverEndpointManager.WorkerRegistryQueryPort {
  private static final Duration METRICS_REFRESH_INTERVAL = Duration.ofSeconds(1);

  private final PlacementDriverConfiguration config;
  private final ConcurrencyControl concurrencyControl;
  private final PlacementDriverRaftRuntime raftRuntime;
  private final ProcessingState processingState;
  private final BrokerTopologyCalculator brokerTopologyCalculator;
  private final BrokerTopologyWatchPublisher brokerTopologyWatchPublisher =
      new BrokerTopologyWatchPublisher();
  private final PlacementDriverClusterTopologyWatchPublisher clusterTopologyWatchPublisher =
      new PlacementDriverClusterTopologyWatchPublisher();
  private final PlacementDriverClusterLeadershipListener clusterLeadershipListener;
  private final PlacementDriverMemberRegistrationActor memberRegistrationActor;
  private final List<PlacementDriverCommittedRecordSubscription> committedRecordSubscriptions;
  private final PlacementDriverMetrics metrics;
  private ScheduledTimer metricsRefreshTimer;
  private boolean closed;

  public PlacementDriverRuntimeComponents(
      final PlacementDriverConfiguration config,
      final ConcurrencyControl concurrencyControl,
      final PlacementDriverRaftRuntime raftRuntime) {
    this(config, concurrencyControl, raftRuntime, () -> {}, null);
  }

  public PlacementDriverRuntimeComponents(
      final PlacementDriverConfiguration config,
      final ConcurrencyControl concurrencyControl,
      final PlacementDriverRaftRuntime raftRuntime,
      final Runnable localMemberRemoved) {
    this(config, concurrencyControl, raftRuntime, localMemberRemoved, null);
  }

  public PlacementDriverRuntimeComponents(
      final PlacementDriverConfiguration config,
      final ConcurrencyControl concurrencyControl,
      final PlacementDriverRaftRuntime raftRuntime,
      final Runnable localMemberRemoved,
      final MeterRegistry meterRegistry) {
    this.config = config;
    this.concurrencyControl = concurrencyControl;
    this.raftRuntime = raftRuntime;
    processingState = raftRuntime.processingState();
    metrics =
        meterRegistry == null
            ? null
            : new PlacementDriverMetrics(
                meterRegistry, config.nodeId(), config.changeHistoryLimit());
    brokerTopologyCalculator = new BrokerTopologyCalculator(processingState);
    clusterLeadershipListener =
        new PlacementDriverClusterLeadershipListener(
            config.nodeId(),
            concurrencyControl,
            raftRuntime.leaderLocator()::isLocalLeader,
            this::localPlacementDriverTopology,
            () -> processingState.getPlacementDriverTopologyState().topology(),
            raftRuntime.commandWriter());
    memberRegistrationActor =
        new PlacementDriverMemberRegistrationActor(
            new PlacementDriverMemberRegistrationActor.Endpoint(
                config.nodeId(),
                config.network().host(),
                raftRuntime.nodeSnapshot().httpPort(),
                raftRuntime.nodeSnapshot().grpcPort(),
                raftRuntime.localRaftPort()),
            new PlacementDriverMemberRegistrationActor.Context(
                concurrencyControl,
                () ->
                    processingState
                        .getPlacementDriverTopologyState()
                        .topology()
                        .map(
                            topology ->
                                topology.getMembers().stream()
                                    .anyMatch(
                                        member ->
                                            config.nodeId().equals(member.getMemberId())
                                                && member.getRole().isAdmitted()))
                        .orElse(false),
                localMemberRemoved,
                raftRuntime::nodeSnapshot,
                raftRuntime::updateTopology));
    processingState
        .getPlacementDriverTopologyState()
        .topology()
        .ifPresent(raftRuntime::observeMemberIdentities);
    final PlacementDriverCommittedRecordObserver observer = raftRuntime.committedRecordObserver();
    final List<PlacementDriverCommittedRecordSubscription> subscriptions = new ArrayList<>();
    subscriptions.add(
        observer.subscribeToCommittedRecords(
            new BrokerTopologyCommittedRecordListener(
                brokerTopologyCalculator, brokerTopologyWatchPublisher)));
    subscriptions.add(
        observer.subscribeToCommittedRecords(
            new PlacementDriverTopologyCommittedRecordListener(
                this::forLocalMember,
                clusterTopologyWatchPublisher,
                raftRuntime::observeMemberIdentities)));
    subscriptions.add(
        observer.subscribeToCommittedRecords(
            new PlacementDriverLearnerAdmissionListener(
                raftRuntime,
                () ->
                    concurrencyControl.run(
                        clusterLeadershipListener::onClusterLeadershipChanged))));
    if (metrics != null) {
      subscriptions.add(observer.subscribeToCommittedRecords(ignored -> scheduleMetricsRefresh()));
    }
    committedRecordSubscriptions = List.copyOf(subscriptions);
    raftRuntime.raftPartition().addRoleChangeListener(this::onRoleChanged);
    raftRuntime.raftPartition().addLeaderChangeListener(ignored -> onLeaderChanged());
  }

  public ActorFuture<Void> start() {
    final ActorFuture<Void> registered = memberRegistrationActor.start();
    refreshMetrics();
    final ActorFuture<Void> initialized = clusterLeadershipListener.initializeClusterLeadership();
    return registered.thenCompose(ignored -> initialized);
  }

  private Optional<MemberTopologyRecord> currentPlacementDriverClusterView() {
    return processingState.getPlacementDriverTopologyState().topology().map(this::forLocalMember);
  }

  public AutoCloseable watchBrokerTopology(
      final String clusterId,
      final Consumer<ClusterTopology> topologyListener,
      final Consumer<String> unavailableListener) {
    final BrokerTopologyWatchListener listener =
        new BrokerTopologyWatchListener() {
          @Override
          public void onBrokerTopology(final ClusterTopology topology) {
            topologyListener.accept(topology);
          }

          @Override
          public void onBrokerTopologyUnavailable(final String unavailableClusterId) {
            unavailableListener.accept(unavailableClusterId);
          }
        };
    final AutoCloseable subscription =
        brokerTopologyWatchPublisher.subscribeToBrokerTopology(clusterId, listener);
    concurrencyControl.run(
        () -> {
          try {
            listener.onBrokerTopology(currentTopology(clusterId));
          } catch (final RuntimeException ignored) {
            // A later committed topology update will push the first available snapshot.
          }
        });
    return subscription;
  }

  public AutoCloseable watchPlacementDriverTopology(
      final Consumer<MemberTopologyRecord> topologyListener) {
    final PlacementDriverClusterTopologyWatchPublisher.PlacementDriverClusterTopologyWatchListener
        listener = topologyListener::accept;
    final AutoCloseable subscription =
        clusterTopologyWatchPublisher.subscribeToPlacementDriverClusterTopology(listener);
    concurrencyControl.run(
        () ->
            currentPlacementDriverClusterView()
                .ifPresent(listener::onPlacementDriverClusterTopology));
    return subscription;
  }

  @Override
  public ActorFuture<List<WorkerRegistration>> queryWorkers(
      final String clusterId, final String tenantId, final String name) {
    return concurrencyControl.call(
        () ->
            processingState.getWorkerRegistrationState().registrations().stream()
                .filter(
                    worker ->
                        clusterId == null
                            || clusterId.isBlank()
                            || worker.clusterId().equals(clusterId))
                .filter(
                    worker ->
                        tenantId == null
                            || tenantId.isBlank()
                            || worker.tenantId().equals(tenantId))
                .filter(worker -> name == null || name.isBlank() || worker.name().equals(name))
                .toList());
  }

  @Override
  public ActorFuture<List<JobYieldPlan>> queryYields(
      final String clusterId,
      final String tenantId,
      final String workerId,
      final String workerLeaseToken,
      final String yieldId) {
    return concurrencyControl.call(
        () ->
            processingState.getJobYieldState().plans().stream()
                .filter(
                    plan ->
                        clusterId == null
                            || clusterId.isBlank()
                            || plan.clusterId().equals(clusterId))
                .filter(
                    plan ->
                        tenantId == null || tenantId.isBlank() || plan.tenantId().equals(tenantId))
                .filter(
                    plan ->
                        workerId == null || workerId.isBlank() || plan.workerId().equals(workerId))
                .filter(
                    plan ->
                        workerLeaseToken == null
                            || workerLeaseToken.isBlank()
                            || Long.toString(plan.workerLeaseToken()).equals(workerLeaseToken))
                .filter(
                    plan ->
                        yieldId == null
                            || yieldId.isBlank()
                            || Long.toString(plan.yieldId()).equals(yieldId))
                .sorted(Comparator.comparingLong(JobYieldPlan::createdAtEpochMillis).reversed())
                .limit(200)
                .toList());
  }

  @Override
  public void close() {
    closed = true;
    committedRecordSubscriptions.forEach(PlacementDriverCommittedRecordSubscription::close);
    if (metricsRefreshTimer != null) {
      metricsRefreshTimer.cancel();
      metricsRefreshTimer = null;
    }
    memberRegistrationActor.close();
    clusterLeadershipListener.close();
    brokerTopologyWatchPublisher.close();
    clusterTopologyWatchPublisher.close();
    if (metrics != null) {
      metrics.close();
    }
  }

  private ClusterTopology currentTopology(final String clusterId) {
    return brokerTopologyCalculator.calculateBrokerTopology(clusterId, Instant.EPOCH);
  }

  private MemberTopologyRecord localPlacementDriverTopology() {
    final List<MemberTopologyRecord.MemberRecord> members = new ArrayList<>();
    final Map<String, MemberTopologyRecord.MemberRecord> committedMembers = new LinkedHashMap<>();
    processingState
        .getPlacementDriverTopologyState()
        .topology()
        .ifPresent(
            topology ->
                topology
                    .getMembers()
                    .forEach(member -> committedMembers.put(member.getMemberId(), member)));
    final PlacementDriverNodeSnapshot localSnapshot = raftRuntime.nodeSnapshot();
    for (final RaftMember member : raftRuntime.raftPartition().members()) {
      final boolean self = config.nodeId().equals(member.memberId());
      final MemberTopologyRecord.MemberRecord committed = committedMembers.get(member.memberId());
      final PlacementDriverPeer configuredPeer =
          config.consensus().bootstrapPeers().stream()
              .filter(peer -> peer.memberId().equals(member.memberId()))
              .findFirst()
              .orElse(null);
      final String host =
          self
              ? config.network().host()
              : committed != null && !committed.getHost().isBlank()
                  ? committed.getHost()
                  : configuredPeer == null ? "" : configuredPeer.host();
      final int grpcPort =
          self
              ? localSnapshot.grpcPort()
              : committed != null && committed.getGrpcPort() > 0
                  ? committed.getGrpcPort()
                  : configuredPeer == null ? 0 : configuredPeer.grpcPort();
      final int raftPort =
          self
              ? raftRuntime.localRaftPort()
              : committed != null && committed.getRaftPort() > 0
                  ? committed.getRaftPort()
                  : configuredPeer == null ? 0 : configuredPeer.raftPort();
      members.add(
          new MemberTopologyRecord.MemberRecord()
              .setMemberId(member.memberId())
              .setEndpoint(grpcPort > 0 ? endpoint(host, grpcPort) : "")
              .setRole(
                  member.role() == null
                      ? MemberRole.UNKNOWN
                      : MemberRole.parse(member.role().name()))
              .setSelf(self)
              .setHost(host)
              .setHttpPort(
                  self ? localSnapshot.httpPort() : committed == null ? 0 : committed.getHttpPort())
              .setGrpcPort(grpcPort)
              .setRaftPort(raftPort)
              .setStatus(
                  self
                      ? RegistrationStatus.ONLINE
                      : committed == null ? RegistrationStatus.UNKNOWN : committed.getStatus())
              .setIncarnationId(
                  committed == null ? member.memberId() + "-0" : committed.getIncarnationId())
              .setEndpointEpoch(committed == null ? 0 : committed.getEndpointEpoch())
              .setCommitIndex(self ? localSnapshot.commitIndex() : 0)
              .setLastApplied(self ? localSnapshot.lastApplied() : 0)
              .setLastLogIndex(self ? localSnapshot.lastLogIndex() : 0)
              .setReadyForPromotion(committed != null && committed.isReadyForPromotion()));
    }
    final String leaderId = Optional.ofNullable(raftRuntime.raftPartition().leaderId()).orElse("");
    return new MemberTopologyRecord()
        .setLocalMemberId(config.nodeId())
        .setLeaderId(leaderId)
        .setLeaderEndpoint(
            leaderId.equals(config.nodeId())
                ? endpoint(config.network().host(), localSnapshot.grpcPort())
                : peerEndpoint(leaderId))
        .setTerm(raftRuntime.raftPartition().term())
        .setConfigurationEpoch(processingState.getVersion())
        .setMembers(members);
  }

  private MemberTopologyRecord forLocalMember(final MemberTopologyRecord topology) {
    final List<MemberTopologyRecord.MemberRecord> members =
        topology.getMembers().stream()
            .map(
                member ->
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId(member.getMemberId())
                        .setEndpoint(member.getEndpoint())
                        .setRole(member.getRole())
                        .setSelf(config.nodeId().equals(member.getMemberId()))
                        .setHost(member.getHost())
                        .setHttpPort(member.getHttpPort())
                        .setGrpcPort(member.getGrpcPort())
                        .setRaftPort(member.getRaftPort())
                        .setStatus(member.getStatus())
                        .setIncarnationId(member.getIncarnationId())
                        .setEndpointEpoch(member.getEndpointEpoch())
                        .setCommitIndex(member.getCommitIndex())
                        .setLastApplied(member.getLastApplied())
                        .setLastLogIndex(member.getLastLogIndex())
                        .setReadyForPromotion(member.isReadyForPromotion()))
            .toList();
    return new MemberTopologyRecord()
        .setLocalMemberId(config.nodeId())
        .setLeaderId(topology.getLeaderId())
        .setLeaderEndpoint(topology.getLeaderEndpoint())
        .setTerm(topology.getTerm())
        .setConfigurationEpoch(topology.getConfigurationEpoch())
        .setMembers(members);
  }

  private String peerEndpoint(final String memberId) {
    return processingState
        .getPlacementDriverTopologyState()
        .topology()
        .flatMap(
            topology ->
                topology.getMembers().stream()
                    .filter(member -> memberId.equals(member.getMemberId()))
                    .map(MemberTopologyRecord.MemberRecord::getEndpoint)
                    .filter(value -> !value.isBlank())
                    .findFirst())
        .or(
            () ->
                config.consensus().bootstrapPeers().stream()
                    .filter(peer -> peer.memberId().equals(memberId))
                    .map(PlacementDriverRuntimeComponents::endpoint)
                    .findFirst())
        .orElse("");
  }

  private void onRoleChanged(final RaftRole role) {
    concurrencyControl.run(
        () -> {
          refreshMetrics();
          clusterLeadershipListener.onClusterLeadershipChanged();
        });
  }

  private void onLeaderChanged() {
    concurrencyControl.run(
        () -> {
          refreshMetrics();
          clusterLeadershipListener.onClusterLeadershipChanged();
        });
  }

  private void refreshMetrics() {
    if (metrics != null) {
      metrics.update(raftRuntime.raftPartition(), processingState);
    }
  }

  private void scheduleMetricsRefresh() {
    if (closed || metrics == null || metricsRefreshTimer != null) {
      return;
    }
    metricsRefreshTimer =
        concurrencyControl.schedule(
            METRICS_REFRESH_INTERVAL,
            () -> {
              metricsRefreshTimer = null;
              if (!closed) {
                refreshMetrics();
              }
            });
  }

  private static String endpoint(final PlacementDriverPeer peer) {
    return endpoint(peer.host(), peer.grpcPort());
  }

  private static String endpoint(final String host, final int port) {
    return HostPort.format(host, port);
  }
}
