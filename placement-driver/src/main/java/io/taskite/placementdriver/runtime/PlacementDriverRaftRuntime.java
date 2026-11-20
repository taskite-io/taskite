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

import com.alipay.sofa.jraft.util.Endpoint;
import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.broker.client.BrokerClient;
import io.taskite.broker.client.BrokerClientConfig;
import io.taskite.broker.client.impl.BrokerClientImpl;
import io.taskite.broker.client.transport.GrpcBrokerClientTransport;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.PlacementDriverPeer;
import io.taskite.placementdriver.processing.PlacementDriverProcessorDependencies;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.snapshot.SnapshotMetrics;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.metrics.CommandDispatchMetrics;
import io.taskite.streamplatform.api.metrics.LogAppendMetrics;
import io.taskite.streamplatform.api.metrics.MicrometerSnapshotMetrics;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.streamplatform.runtime.StreamPartitionConfig;
import io.taskite.streamplatform.runtime.StreamPartitionDependencies;
import io.taskite.streamplatform.runtime.StreamPartitionFactory;
import io.taskite.topology.MemberRole;
import io.taskite.util.HostPort;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns the PD's raft group, deterministic state, command submitter, and committed-record bus. */
public final class PlacementDriverRaftRuntime implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(PlacementDriverRaftRuntime.class);
  private static final String CLUSTER_ID = "placement-driver";
  private static final int PARTITION_ID = 1;

  private final TypedEventRegistry eventRegistry;
  private final PlacementDriverConfiguration config;
  private final int httpPort;
  private final int grpcPort;
  private final JRaftServer jraftServer;
  private final StreamPartition raftGroup;
  private final RaftPartition raftPartition;
  private final PlacementDriverClusterLeaderLocator leaderLocator;
  private final PlacementDriverCommandSubmitter commandSubmitter;
  private final RoutingPlacementDriverCommandSubmitter routingCommandSubmitter;
  private final PlacementDriverCommittedRecordObserver committedRecordObserver;
  private final ProcessingState processingState;
  private final CommandWriter commandWriter;
  private final PlacementDriverTopologyManifestWriter topologyManifestWriter =
      new PlacementDriverTopologyManifestWriter();
  private final int localRaftPort;
  private final AtomicBoolean closed = new AtomicBoolean();

  private BrokerClient createBrokerClient() {
    return new BrokerClientImpl(
        new GrpcBrokerClientTransport(),
        new BrokerClientConfig(
            config.broker().client().requestTimeout(), config.broker().client().maxRetries()));
  }

  private boolean isLeader() {
    return raftPartition != null && raftPartition.role() == RaftRole.LEADER;
  }

  public record Endpoints(int httpPort, int grpcPort) {}

  public record Bootstrap(List<RaftPeer> peers, String leaderEndpoint) {
    public Bootstrap {
      peers = List.copyOf(peers);
    }
  }

  public record Configuration(
      PlacementDriverConfiguration node, Endpoints endpoints, Bootstrap bootstrap) {}

  public record Context(
      ConcurrencyControl concurrencyControl,
      ActorScheduler scheduler,
      TypedEventRegistry eventRegistry,
      PlacementDriverProcessorDependencies processorDependencies,
      MeterRegistry meterRegistry) {}

  public PlacementDriverRaftRuntime(final Configuration configuration, final Context context)
      throws IOException, RocksDBException {
    final var config = configuration.node();
    final int httpPort = configuration.endpoints().httpPort();
    final int grpcPort = configuration.endpoints().grpcPort();
    final var concurrencyControl = context.concurrencyControl();
    final var scheduler = context.scheduler();
    final var eventRegistry = context.eventRegistry();
    final var processorDependencies = context.processorDependencies();
    commandWriter = processorDependencies.commandWriter();
    final var meterRegistry = context.meterRegistry();
    this.config = config;
    this.httpPort = httpPort;
    this.grpcPort = grpcPort;
    this.eventRegistry = eventRegistry;
    final List<PlacementDriverPeer> peers = List.copyOf(config.consensus().bootstrapPeers());
    final List<RaftPeer> restoredPeers = configuration.bootstrap().peers();
    localRaftPort = config.network().raftPort();
    jraftServer = new JRaftServer(new Endpoint(config.network().host(), localRaftPort));
    final List<RaftPeer> jraftPeers = new ArrayList<>();
    if (!restoredPeers.isEmpty()) {
      restoredPeers.stream()
          .map(
              peer ->
                  peer.brokerId().equals(config.nodeId())
                      ? new RaftPeer(
                          peer.brokerId(),
                          config.network().host(),
                          localRaftPort,
                          peer.voter(),
                          peer.priority())
                      : peer)
          .forEach(jraftPeers::add);
    } else {
      jraftPeers.add(new RaftPeer(config.nodeId(), config.network().host(), localRaftPort, true));
      for (final PlacementDriverPeer peer : peers) {
        jraftPeers.add(new RaftPeer(peer.memberId(), peer.host(), peer.raftPort(), true));
      }
    }
    committedRecordObserver = new PlacementDriverCommittedRecordObserver(concurrencyControl::run);
    final SnapshotMetrics.Factory snapshotMetricsFactory =
        meterRegistry == null
            ? SnapshotMetrics.Factory.noop()
            : partitionId -> new MicrometerSnapshotMetrics(meterRegistry, CLUSTER_ID, partitionId);
    final StreamPartitionDependencies dependencies =
        new StreamPartitionDependencies(
            Protocol.PLACEMENT_DRIVER_CLUSTER_KEY,
            "placement-driver-" + config.nodeId(),
            eventRegistry,
            processorContext ->
                PlacementDriverProcessors.createPlacementDriverProcessors(
                    processorContext,
                    processorDependencies,
                    this::createBrokerClient,
                    this::isLeader,
                    config
                        .broker()
                        .client()
                        .requestTimeout()
                        .multipliedBy((long) config.broker().client().maxRetries() + 1)
                        .plusSeconds(15),
                    config.broker().client().requestTimeout(),
                    () -> new RaftPdMemberChangeTransport(this)),
            (partitionId, storage, keyGenerator, writers) ->
                PlacementDriverProcessors.createProcessorContext(
                    partitionId,
                    storage,
                    keyGenerator,
                    writers,
                    this::committedPosition,
                    this::raftTerm,
                    config.changeHistoryLimit(),
                    config.registrationPolicy()),
            committedRecordObserver,
            new PlacementDriverTopologySnapshotListener(
                config.storage().dataDir(), config.nodeId()),
            ProcessingMetrics.Factory.noop(),
            CommandDispatchMetrics.Factory.noop(),
            LogAppendMetrics.Factory.noop(),
            snapshotMetricsFactory,
            ignored ->
                new PartitionRequestLimiter<>(
                    config.backpressure(), PlacementDriverBackpressure::isExempt));
    final StreamPartitionConfig partitionConfig =
        StreamPartitionConfig.builder(
                CLUSTER_ID, PARTITION_ID, config.nodeId(), config.storage().dataDir())
            .initialRole(RaftRole.FOLLOWER)
            .peers(jraftPeers)
            .snapshotPolicy(config.storage().snapshot().policy())
            .electionTimeout(config.consensus().electionTimeout())
            .heartbeatInterval(config.consensus().heartbeatInterval())
            .build();
    raftGroup =
        StreamPartitionFactory.create(partitionConfig, jraftServer, scheduler, dependencies);
    raftPartition = raftGroup.raftPartition();
    processingState =
        new MutableProcessingState(
            new StateStoreProcessingStore(raftPartition.storage()),
            this::committedPosition,
            this::raftTerm,
            config.changeHistoryLimit(),
            config.registrationPolicy());
    leaderLocator =
        new PlacementDriverClusterLeaderLocator(
            config.nodeId(),
            raftPartition,
            new PlacementDriverClusterEndpointResolver(
                config.nodeId(),
                config.network().host(),
                grpcPort,
                peers,
                () -> processingState.getPlacementDriverTopologyState().topology()));
    final PlacementDriverCommandSubmitter localSubmitter =
        new RaftPlacementDriverCommandSubmitter(raftGroup, leaderLocator);
    routingCommandSubmitter =
        new RoutingPlacementDriverCommandSubmitter(
            localSubmitter,
            this::updateLocalTopology,
            leaderLocator,
            eventRegistry,
            configuration.bootstrap().leaderEndpoint());
    commandSubmitter = routingCommandSubmitter;
  }

  public TypedEventRegistry eventRegistry() {
    return eventRegistry;
  }

  public RaftPartition raftPartition() {
    return raftPartition;
  }

  public ActorFuture<Void> startupFuture() {
    return raftGroup.startupFuture();
  }

  public PlacementDriverClusterLeaderLocator leaderLocator() {
    return leaderLocator;
  }

  public PlacementDriverCommandSubmitter commandSubmitter() {
    return commandSubmitter;
  }

  public CommandWriter commandWriter() {
    return commandWriter;
  }

  public ActorFuture<MemberTopologyRecord> updateTopology(final MemberTopologyRecord topology) {
    return routingCommandSubmitter.updateTopology(topology);
  }

  public ActorFuture<MemberTopologyRecord> updateLocalTopology(
      final MemberTopologyRecord topology) {
    if (!leaderLocator.isLocalLeader()) {
      return CompletableActorFuture.completedExceptionally(
          new PlacementDriverNotLeaderException(
              leaderLocator.locateOtherLeaderEndpointText().orElse(null)));
    }
    final LoggedEvent command =
        LoggedEvent.ofRecord(
            0,
            new RecordMetadata()
                .recordVersion(LoggedEvent.DEFAULT_RECORD_VERSION)
                .recordType(RecordType.COMMAND)
                .valueType(ValueType.TOPOLOGY)
                .intent(TopologyIntent.UPDATE),
            topology);
    return raftGroup
        .submit(command)
        .thenApply(
            result -> {
              if (result.metadata().getRecordType() == RecordType.COMMAND_REJECTION) {
                throw new PlacementDriverRejectionException(
                    result.metadata().getRejectionType(), result.metadata().getRejectionReason());
              }
              return (MemberTopologyRecord) result.value();
            });
  }

  public PlacementDriverCommittedRecordObserver committedRecordObserver() {
    return committedRecordObserver;
  }

  public ProcessingState processingState() {
    return processingState;
  }

  public PlacementDriverNodeSnapshot nodeSnapshot() {
    return new PlacementDriverNodeSnapshot(
        config.nodeId(),
        config.network().host(),
        httpPort,
        grpcPort,
        raftPartition.committedIndex(),
        raftPartition.lastAppliedIndex(),
        raftPartition.lastIndex());
  }

  public int localRaftPort() {
    return localRaftPort;
  }

  public ActorFuture<Void> updateRaftMembership(final List<RaftPeer> peers) {
    return raftGroup.updateRaftMembership(peers);
  }

  public ActorFuture<Void> addRaftLearners(final List<RaftPeer> peers) {
    return raftGroup.addRaftLearners(peers);
  }

  public void observeMemberIdentities(final MemberTopologyRecord topology) {
    final List<RaftPeer> peers =
        topology.getMembers().stream()
            .filter(member -> !member.getMemberId().isBlank())
            .filter(member -> !member.getHost().isBlank())
            .filter(member -> member.getRaftPort() > 0)
            .map(
                member ->
                    new RaftPeer(
                        member.getMemberId(),
                        member.getHost(),
                        member.getRaftPort(),
                        member.getRole() != MemberRole.LEARNER
                            && member.getRole() != MemberRole.REGISTERED))
            .toList();
    raftGroup.observePeerIdentities(peers);
  }

  public ActorFuture<Void> transferLeadership(final String targetMemberId) {
    return raftGroup.transferLeadership(targetMemberId);
  }

  private long committedPosition() {
    return raftPartition.committedPosition();
  }

  private long raftTerm() {
    return raftPartition.term();
  }

  private void writeTopologyManifestOnShutdown() {
    try {
      final MemberTopologyRecord topology =
          processingState.getPlacementDriverTopologyState().topology().orElse(null);
      final Map<String, RaftRole> roles = new LinkedHashMap<>();
      raftPartition.members().forEach(member -> roles.put(member.memberId(), member.role()));
      final List<PlacementDriverTopologyManifest.Member> members = new ArrayList<>();
      if (topology != null) {
        topology
            .getMembers()
            .forEach(
                member -> {
                  final RaftRole role = roles.get(member.getMemberId());
                  if (role == null) {
                    return;
                  }
                  final boolean learner =
                      role == RaftRole.LEARNER || member.getRole() == MemberRole.LEARNER;
                  members.add(
                      new PlacementDriverTopologyManifest.Member(
                          member.getMemberId(),
                          member.getHost().isBlank()
                              ? hostOf(member.getEndpoint())
                              : member.getHost(),
                          member.getHttpPort(),
                          member.getGrpcPort() > 0
                              ? member.getGrpcPort()
                              : portOf(member.getEndpoint()),
                          member.getRaftPort(),
                          learner
                              ? PlacementDriverTopologyManifest.Role.LEARNER
                              : PlacementDriverTopologyManifest.Role.VOTER,
                          member.getIncarnationId().isBlank()
                              ? member.getMemberId() + "-0"
                              : member.getIncarnationId(),
                          member.getEndpointEpoch()));
                });
      }
      if (members.isEmpty()) {
        final Map<String, PlacementDriverPeer> configured = new LinkedHashMap<>();
        config.consensus().bootstrapPeers().forEach(peer -> configured.put(peer.memberId(), peer));
        raftPartition
            .members()
            .forEach(
                member -> {
                  final boolean self = config.nodeId().equals(member.memberId());
                  final PlacementDriverPeer peer = configured.get(member.memberId());
                  final String host =
                      self ? config.network().host() : peer == null ? "" : peer.host();
                  final int memberGrpcPort = self ? grpcPort : peer == null ? 0 : peer.grpcPort();
                  final int memberRaftPort =
                      self ? localRaftPort : peer == null ? 0 : peer.raftPort();
                  if (!host.isBlank() && memberGrpcPort > 0 && memberRaftPort > 0) {
                    members.add(
                        new PlacementDriverTopologyManifest.Member(
                            member.memberId(),
                            host,
                            self ? httpPort : 0,
                            memberGrpcPort,
                            memberRaftPort,
                            member.role() == RaftRole.LEARNER
                                ? PlacementDriverTopologyManifest.Role.LEARNER
                                : PlacementDriverTopologyManifest.Role.VOTER,
                            member.memberId() + "-0",
                            0));
                  }
                });
      }
      if (members.stream().noneMatch(member -> config.nodeId().equals(member.memberId()))) {
        LOG.warn("skip topology manifest because local member {} is absent", config.nodeId());
        return;
      }
      final PlacementDriverTopologyManifest manifest =
          new PlacementDriverTopologyManifest(
              PlacementDriverTopologyManifest.CURRENT_VERSION,
              PlacementDriverTopologyManifest.CLUSTER_ID,
              PlacementDriverTopologyManifest.PARTITION_ID,
              config.nodeId(),
              PlacementDriverTopologyManifest.Source.SHUTDOWN,
              topology == null ? 0 : topology.getConfigurationEpoch(),
              topology == null ? 0 : topology.getConfigurationEpoch(),
              raftPartition.lastAppliedIndex(),
              null,
              raftPartition.term(),
              Optional.ofNullable(raftPartition.leaderId()).orElse(""),
              Instant.now(),
              members,
              null);
      topologyManifestWriter.write(config.storage().dataDir(), manifest);
    } catch (final RuntimeException error) {
      LOG.error("failed to write placement-driver topology manifest during shutdown", error);
    }
  }

  private static String hostOf(final String endpoint) {
    final int separator = endpoint == null ? -1 : endpoint.lastIndexOf(':');
    return separator <= 0 ? "" : HostPort.normalizeHost(endpoint.substring(0, separator));
  }

  private static int portOf(final String endpoint) {
    final int separator = endpoint == null ? -1 : endpoint.lastIndexOf(':');
    if (separator <= 0 || separator == endpoint.length() - 1) {
      return 0;
    }
    try {
      return Integer.parseInt(endpoint.substring(separator + 1));
    } catch (final NumberFormatException ignored) {
      return 0;
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    writeTopologyManifestOnShutdown();
    committedRecordObserver.close();
    routingCommandSubmitter.close();
    LOG.info(
        "closing RocksDB runtime database for placement-driver member {} at {}",
        config.nodeId(),
        config.storage().dataDir().resolve("runtime"));
    try {
      raftGroup.close();
      LOG.info("closed RocksDB runtime database for placement-driver member {}", config.nodeId());
    } finally {
      jraftServer.close();
    }
  }
}
