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
package io.taskite.broker.runtime;

import com.alipay.sofa.jraft.util.Endpoint;
import io.micrometer.core.instrument.MeterRegistry;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.client.request.BrokerExecuteCommand;
import io.taskite.broker.client.response.BrokerResponse;
import io.taskite.broker.partition.BrokerPartitionChangeExecutor;
import io.taskite.broker.partition.BrokerPartitionManager;
import io.taskite.broker.partition.BrokerPartitionManagerConfig;
import io.taskite.broker.partition.JRaftBrokerPartitionChangeExecutor;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.group.PartitionGroupDescriptor;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.topology.ClusterTopology;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerRuntime implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerRuntime.class);
  private static final String KEY_IDENTITY_MISMATCH =
      "Broker %s (clusterKey %d) cannot execute key %d in partition %d";

  private final String brokerId;
  private final long clusterKey;
  private final Path dataRootDirectory;
  private final int localRaftPort;
  private final BrokerPartitionManager raftGroups;
  private final BrokerPartitionChangeExecutor partitionChangeExecutor;
  private final Actor partitionChangeActor;
  private final JRaftServer jraftServer;
  private final ActorScheduler scheduler;
  private final boolean ownsScheduler;
  private final AtomicBoolean closed = new AtomicBoolean();

  public BrokerRuntime(
      final BrokerConfig config,
      final long clusterKey,
      final ActorScheduler scheduler,
      final MeterRegistry meterRegistry) {
    Objects.requireNonNull(config, "config");
    Protocol.validateClusterKey(clusterKey);
    this.clusterKey = clusterKey;
    brokerId = config.brokerId();
    dataRootDirectory = config.dataDirectory();
    this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
    ownsScheduler = false;
    final int localRaftPort = config.network().raftPort();
    if (localRaftPort <= 0 || localRaftPort > 65_535) {
      throw new IllegalArgumentException("localRaftPort must be between 1 and 65535");
    }
    this.localRaftPort = localRaftPort;
    jraftServer = new JRaftServer(new Endpoint(config.network().host(), localRaftPort));
    this.raftGroups =
        new BrokerPartitionManager(
            BrokerPartitionManagerConfig.from(config, clusterKey, true),
            jraftServer,
            scheduler,
            meterRegistry);
    // A dedicated single-thread actor serializes topology-operation apply flows off the gRPC worker
    // thread, keeping every manifest RocksDB access on one consistent thread (M2).
    this.partitionChangeActor =
        Actor.newActor().name("broker-partition-change-" + brokerId).build();
    scheduler.submitActor(partitionChangeActor).join();
    partitionChangeExecutor =
        new JRaftBrokerPartitionChangeExecutor(raftGroups, partitionChangeActor);
  }

  public String brokerId() {
    return brokerId;
  }

  public int localRaftPort() {
    return localRaftPort;
  }

  public void start() {
    jraftServer.start();
  }

  public void updateTopology(ClusterTopology topology) {
    raftGroups.reconcile(topology);
  }

  public List<PartitionGroupDescriptor> localPartitionDescriptors(ClusterTopology topology) {
    return raftGroups.localPartitionDescriptors(topology);
  }

  public StreamPartition bootstrapPartition(
      ClusterTopology topology, PartitionGroupDescriptor descriptor) {
    return raftGroups.bootstrapPartition(topology, descriptor);
  }

  public BrokerPartitionManager raftGroups() {
    return raftGroups;
  }

  public BrokerPartitionChangeExecutor partitionChangeExecutor() {
    return partitionChangeExecutor;
  }

  public ActorFuture<BrokerResponse<UnifiedRecordValue>> submit(BrokerExecuteCommand<?> command) {
    final int partitionId;
    try {
      if (command.getKey() < 0) {
        throw new IllegalArgumentException(
            KEY_IDENTITY_MISMATCH.formatted(
                brokerId, clusterKey, command.getKey(), command.getPartitionId()));
      }
      partitionId =
          command.addressesSpecificPartition()
              ? command.getPartitionId()
              : Protocol.decodePartitionId(command.getKey());
      Protocol.validatePartitionId(partitionId);
    } catch (final IllegalArgumentException error) {
      return failed(error);
    }
    if (command.getKey() > 0
        && command.getValueType() != ValueType.COMMAND_DISTRIBUTION
        && (Protocol.decodeClusterKey(command.getKey()) != clusterKey
            || Protocol.decodePartitionId(command.getKey()) != partitionId)) {
      return failed(
          new IllegalArgumentException(
              KEY_IDENTITY_MISMATCH.formatted(
                  brokerId, clusterKey, command.getKey(), partitionId)));
    }
    return raftGroups
        .group(partitionId)
        .map(group -> submit(group, command))
        .orElseGet(
            () ->
                failed(
                    new IllegalArgumentException(
                        "broker " + brokerId + " has no raft group for partition " + partitionId)));
  }

  public ActorFuture<LoggedEvent> submit(final LoggedEvent record) {
    try {
      final int partitionId = Protocol.decodePartitionId(record.key());
      if (Protocol.decodeClusterKey(record.key()) != clusterKey) {
        return failed(
            new IllegalArgumentException(
                KEY_IDENTITY_MISMATCH.formatted(brokerId, clusterKey, record.key(), partitionId)));
      }
      return raftGroups
          .group(partitionId)
          .map(group -> group.submit(record))
          .orElseGet(
              () ->
                  failed(
                      new IllegalArgumentException(
                          "broker "
                              + brokerId
                              + " has no raft group for partition "
                              + partitionId)));
    } catch (final Throwable error) {
      return failed(error);
    }
  }

  private ActorFuture<BrokerResponse<UnifiedRecordValue>> submit(
      final StreamPartition group, final BrokerExecuteCommand<?> command) {
    final LoggedEvent commandRecord = commandRecord(command);
    final ActorFuture<BrokerResponse<UnifiedRecordValue>> result = new CompletableActorFuture<>();
    group
        .submit(commandRecord)
        .whenComplete(
            (eventRecord, error) -> {
              if (error != null) {
                result.completeExceptionally(error);
                return;
              }
              result.complete(
                  new BrokerResponse<>(
                      eventRecord.value(), group.partitionId(), eventRecord.key()));
            });
    return result;
  }

  private LoggedEvent commandRecord(final BrokerExecuteCommand<?> command) {
    return LoggedEvent.ofRecord(
        command.getKey() > 0 && command.getValueType() != ValueType.COMMAND_DISTRIBUTION
            ? command.getKey()
            : Protocol.encodeKey(clusterKey, command.getPartitionId(), 0),
        new RecordMetadata()
            .requestId(UUID.randomUUID().toString())
            .recordVersion(LoggedEvent.DEFAULT_RECORD_VERSION)
            .recordType(RecordType.COMMAND)
            .valueType(command.getValueType())
            .intent(command.getIntent()),
        command.getRecordValue());
  }

  /**
   * Resolves the local raft partition serving the given id. With jraft, peer raft RPCs are served
   * by the shared {@link JRaftServer} on its own port, so the business gRPC server no longer routes
   * them; this remains for callers that inspect the local partition through the engine-agnostic
   * {@link RaftPartition}.
   */
  public Optional<RaftPartition> partition(int partitionId) {
    return raftGroups.partition(partitionId);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    LOG.info(
        "closing partition runtime databases for broker {} under data root {}",
        brokerId,
        dataRootDirectory);
    partitionChangeActor.closeAsync().join();
    raftGroups.close();
    try {
      jraftServer.close();
      LOG.info("closed partition runtime databases for broker {}", brokerId);
    } finally {
      if (ownsScheduler) {
        scheduler.stop().join();
      }
    }
  }

  private static <T> ActorFuture<T> failed(Throwable error) {
    ActorFuture<T> future = new CompletableActorFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
