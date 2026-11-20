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

import io.taskite.broker.state.BrokerPartitionOperationManifestStore;
import io.taskite.cluster.raft.RaftPartition.ElectionState;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftConfigurationDigest;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.util.HostPort;
import io.taskite.util.RaftMemberIdentity;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Executes Phase 2 operations through the existing jraft membership reconfiguration path. */
public final class JRaftBrokerPartitionChangeExecutor implements BrokerPartitionChangeExecutor {
  private final BrokerPartitionManager partitions;
  private final BrokerPartitionMembershipManager membership;
  private final ConcurrencyControl concurrencyControl;
  private final Map<Integer, Execution> executions = new HashMap<>();

  public JRaftBrokerPartitionChangeExecutor(
      final BrokerPartitionManager partitions, final ConcurrencyControl concurrencyControl) {
    this.partitions = Objects.requireNonNull(partitions, "partitions");
    membership = new BrokerPartitionMembershipManager(partitions);
    this.concurrencyControl = Objects.requireNonNull(concurrencyControl, "concurrencyControl");
  }

  @Override
  public ActorFuture<Void> apply(final BrokerPartitionOperation request) {
    // M2: run the whole apply flow on the broker actor thread. The manifest store reads/writes
    // (dedupe check, IN_PROGRESS, and the terminal APPLIED/FAILED write in the completion callback)
    // must not straddle the gRPC worker thread and the jraft completion thread; dispatching here
    // keeps every manifest access on one consistent actor thread (reads via latestOperation run on
    // the same broker actor), even though the store itself is internally synchronized.
    final CompletableActorFuture<Void> result = new CompletableActorFuture<>();
    concurrencyControl.run(
        () -> {
          try {
            applyOnActor(request, result);
          } catch (final RuntimeException error) {
            if (!result.isDone()) {
              result.completeExceptionally(error);
            }
          }
        });
    return result;
  }

  @Override
  public ActorFuture<Void> add(final BrokerPartitionOperation request) {
    return membership.addPartition(request);
  }

  @Override
  public ActorFuture<Void> join(final BrokerPartitionOperation request) {
    return membership.reconfigurePartition(request);
  }

  @Override
  public ActorFuture<Void> promoteLearner(final BrokerPartitionOperation request) {
    return membership.reconfigurePartition(request);
  }

  @Override
  public ActorFuture<Void> leave(final BrokerPartitionOperation request) {
    return membership.reconfigurePartition(request);
  }

  @Override
  public ActorFuture<Void> reconfigurePriority(final BrokerPartitionOperation request) {
    return membership.reconfigurePartition(request);
  }

  @Override
  public ActorFuture<Void> transferLeadership(final BrokerPartitionOperation request) {
    return membership.transferLeadership(request);
  }

  @Override
  public ActorFuture<Void> resetPeers(final BrokerPartitionOperation request) {
    return membership.resetPeers(request);
  }

  @Override
  public Optional<BrokerPartitionOperationManifest> latestOperation(final int partitionId) {
    return manifestStore(partitionId).flatMap(BrokerPartitionOperationManifestStore::latest);
  }

  @Override
  public ActorFuture<BrokerPartitionConfiguration> readConfiguration(
      final String clusterId, final int partitionId) {
    if (partitions.topology() == null || !clusterId.equals(partitions.topology().clusterId())) {
      return CompletableActorFuture.completedExceptionally(
          new IllegalArgumentException("clusterId"));
    }
    return partitions
        .group(partitionId)
        .map(
            group ->
                group
                    .raftPartition()
                    .readCommittedConfiguration()
                    .thenApply(
                        source ->
                            new BrokerPartitionConfiguration(
                                clusterId,
                                partitionId,
                                source.leaderId(),
                                source.term(),
                                source.configurationIndex(),
                                source.digest(),
                                source.peers().stream()
                                    .map(
                                        peer ->
                                            new BrokerPartitionMember(
                                                peer.brokerId(), peer.voter(), peer.priority()))
                                    .toList(),
                                partitions.localRecoveryEpoch(partitionId),
                                source.authority().generation(),
                                source.authority().owner(),
                                source.authority().index())))
        .orElseGet(
            () ->
                CompletableActorFuture.completedExceptionally(
                    new IllegalStateException("PARTITION_NOT_FOUND")));
  }

  @Override
  public ActorFuture<BrokerPartitionConfiguration> installAuthority(
      final String clusterId,
      final int partitionId,
      final long expectedGeneration,
      final long generation,
      final String owner,
      final long recoveryEpoch) {
    final var result = new CompletableActorFuture<BrokerPartitionConfiguration>();
    concurrencyControl.run(
        () -> {
          if (partitions.topology() == null
              || !clusterId.equals(partitions.topology().clusterId())
              || partitions.localRecoveryEpoch(partitionId) != recoveryEpoch) {
            result.completeExceptionally(new IllegalStateException("STALE_OPERATION"));
            return;
          }
          final var group = partitions.group(partitionId).orElse(null);
          if (group == null) {
            result.completeExceptionally(new IllegalStateException("PARTITION_NOT_FOUND"));
            return;
          }
          concurrencyControl.runOnCompletion(
              group
                  .raftPartition()
                  .installExecutionAuthority(expectedGeneration, generation, owner),
              (authority, error) -> {
                if (error != null) {
                  result.completeExceptionally(error);
                  return;
                }
                final var running = executions.get(partitionId);
                if (running != null && !authorized(running.request())) {
                  running
                      .result()
                      .completeExceptionally(
                          new IllegalStateException("PARTITION_OPERATION_SUPERSEDED"));
                }
                membership.authorityInstalled(partitionId);
                readConfiguration(clusterId, partitionId)
                    .onComplete(
                        (configuration, readError) -> {
                          if (readError == null) {
                            result.complete(configuration);
                          } else {
                            result.completeExceptionally(readError);
                          }
                        });
              });
        });
    return result;
  }

  private boolean authorized(final BrokerPartitionOperation request) {
    if (request.type() == TopologyOperationType.ADD) {
      final var topology = partitions.topology();
      return topology != null
          && request.clusterId().equals(topology.clusterId())
          && request.authority().generation() == 0
          && request.targetBroker().equals(partitions.localBrokerId())
          && request.members().stream()
              .anyMatch(member -> member.brokerId().equals(partitions.localBrokerId()))
          && topology
              .partition(request.partitionId())
              .filter(
                  partition ->
                      partition.voters().contains(partitions.localBrokerId())
                          || partition.learners().contains(partitions.localBrokerId())
                          // Actual members are published only after ADD. The frozen initial
                          // configuration is the bootstrap evidence while that projection is empty.
                          || partition.status() == PartitionStatus.CREATING
                              && partition.voters().isEmpty()
                              && partition.learners().isEmpty()
                              && request.expectedMembershipEpoch() == 0
                              && RaftConfigurationDigest.of(List.of())
                                  .equals(request.expectedConfigurationDigest())
                              && request.members().size() == 1
                              && request.members().getFirst().voter()
                              && topology.brokers().containsKey(partitions.localBrokerId()))
              .isPresent()
          && partitions
              .group(request.partitionId())
              .map(group -> group.raftPartition().executionAuthority().generation() == 0)
              .orElse(true);
    }
    return request.type() == TopologyOperationType.RESET_PEERS
        || partitions
            .group(request.partitionId())
            .map(
                group ->
                    group
                        .raftPartition()
                        .executionAuthorized(
                            request.authority().generation(), request.authority().owner()))
            .orElse(false);
  }

  private void applyOnActor(
      final BrokerPartitionOperation request, final CompletableActorFuture<Void> result) {
    if (!authorized(request)) {
      result.completeExceptionally(new IllegalStateException("PARTITION_OPERATION_SUPERSEDED"));
      return;
    }
    final var running = executions.get(request.partitionId());
    if (running != null) {
      if (!running.request().equals(request)) {
        result.completeExceptionally(new IllegalStateException("PARTITION_OPERATION_IN_PROGRESS"));
      } else {
        running
            .result()
            .onComplete(
                (ignored, error) -> {
                  if (error == null) {
                    result.complete(null);
                  } else {
                    result.completeExceptionally(error);
                  }
                });
      }
      return;
    }
    final Optional<BrokerPartitionOperationManifestStore> existingStore =
        manifestStore(request.partitionId());
    final long localEpoch = partitions.localRecoveryEpoch(request.partitionId());
    final boolean reset = request.type() == TopologyOperationType.RESET_PEERS;
    if (request.opId().isBlank()
        || partitions.topology() == null
        || !request.clusterId().equals(partitions.topology().clusterId())
        || (request.expectedRecoveryEpoch() != localEpoch
            && !(reset && request.targetRecoveryEpoch() == localEpoch && effectApplied(request)))) {
      result.completeExceptionally(
          new IllegalStateException("STALE_OPERATION: identity or recovery epoch"));
      return;
    }
    existingStore.ifPresent(store -> store.pinRequest(request));
    final var applied =
        existingStore
            .flatMap(store -> store.find(request.opId()))
            .filter(manifest -> manifest.status() == BrokerPartitionOperationStatus.APPLIED);
    if (applied.isPresent()) {
      if (effectApplied(request)) {
        result.complete(null);
      } else {
        result.completeExceptionally(
            new IllegalStateException("OPERATION_SUPERSEDED: " + request.opId()));
      }
      return;
    }
    final var before = partitions.group(request.partitionId());
    if (request.type() != TopologyOperationType.ADD && !effectApplied(request)) {
      final var observation =
          before.map(group -> group.raftPartition().electionState()).orElse(null);
      final String actualDigest =
          before.map(group -> group.raftPartition().appliedConfigurationDigest()).orElse("");
      final boolean authorizedResume =
          existingStore.map(store -> store.canResume(request, actualDigest)).orElse(false);
      if (!authorizedResume
          && (observation == null
              || request.expectedConfigurationDigest().isBlank()
              || request.expectedMembershipEpoch() > 0
                  && observation.configurationIndex() > 0
                  && (observation.configurationIndex() < request.expectedMembershipEpoch()
                      || reset
                          && observation.configurationIndex() != request.expectedMembershipEpoch())
              || !request.expectedConfigurationDigest().equals(actualDigest))) {
        result.completeExceptionally(
            new IllegalStateException("STALE_CONFIGURATION: " + request.opId()));
        return;
      }
    }
    if (before.isPresent()
        && request
            .expectedConfigurationDigest()
            .equals(before.get().raftPartition().appliedConfigurationDigest())) {
      existingStore.ifPresent(
          store ->
              store.authorizeConfigurations(
                  request,
                  configurationStages(before.get().raftPartition().appliedPeers(), request)));
    }
    final var executionIdentity = new Execution(request, result, before.orElse(null));
    executions.put(request.partitionId(), executionIdentity);
    result.onComplete(
        (ignored, error) -> executions.remove(request.partitionId(), executionIdentity));
    try {
      existingStore.ifPresent(
          store ->
              store.put(
                  new BrokerPartitionOperationManifest(
                      new BrokerPartitionOperationManifest.Identity(
                          request.partitionId(), request.opId()),
                      new BrokerPartitionOperationManifest.Result(
                          BrokerPartitionOperationStatus.IN_PROGRESS, 0, ""),
                      BrokerPartitionOperationManifest.Evidence.UNKNOWN)));
    } catch (final RuntimeException persistenceError) {
      result.completeExceptionally(persistenceError);
      return;
    }
    final ActorFuture<Void> execution =
        effectApplied(request)
            ? CompletableActorFuture.completed(null)
            : BrokerPartitionChangeExecutor.super.apply(request);
    final long deadline = System.currentTimeMillis() + 10_000;
    // A lost/delayed JRaft closure must not retain the partition execution slot forever.
    // Only an applied configuration certificate can turn this fallback into an ACK.
    concurrencyControl.schedule(
        Duration.ofSeconds(10), () -> awaitAppliedEffect(request, result, null, deadline));
    concurrencyControl.runOnCompletion(
        execution,
        (ignored, error) -> {
          awaitAppliedEffect(request, result, error, deadline);
        });
  }

  private void awaitAppliedEffect(
      final BrokerPartitionOperation request,
      final CompletableActorFuture<Void> result,
      final Throwable executionError,
      final long deadline) {
    final var executing = executions.get(request.partitionId());
    if (executing == null || executing.result() != result || result.isDone()) {
      return;
    }
    if (executing.group() != null
        && partitions.group(request.partitionId()).orElse(null) != executing.group()) {
      result.completeExceptionally(new IllegalStateException("PARTITION_OPERATION_SUPERSEDED"));
      return;
    }
    if (!authorized(request)) {
      // A successful transfer makes its source stop being Leader before it can certify the
      // effect. Keep mutation fencing intact and let PD observe the effect on the new Leader.
      final String reason =
          executing.group() != null && executing.group().role() != RaftRole.LEADER
              ? "NOT_LEADER"
              : "PARTITION_OPERATION_SUPERSEDED";
      result.completeExceptionally(new IllegalStateException(reason));
      return;
    }
    final boolean applied = effectApplied(request);
    if (executionError == null && !applied && System.currentTimeMillis() < deadline) {
      concurrencyControl.schedule(
          Duration.ofMillis(50), () -> awaitAppliedEffect(request, result, null, deadline));
      return;
    }
    final Throwable error =
        applied
            ? null
            : executionError != null
                ? executionError
                : new IllegalStateException("CONFIGURATION_NOT_APPLIED: " + request.opId());
    final Optional<BrokerPartitionOperationManifestStore> completedStore =
        manifestStore(request.partitionId());
    if (completedStore.isEmpty()) {
      if (error != null) {
        result.completeExceptionally(error);
      } else {
        result.completeExceptionally(
            new IllegalStateException(
                "partition storage is unavailable: " + request.partitionId()));
      }
      return;
    }
    try {
      completedStore.orElseThrow().pinRequest(request);
      completedStore
          .orElseThrow()
          .put(
              new BrokerPartitionOperationManifest(
                  new BrokerPartitionOperationManifest.Identity(
                      request.partitionId(), request.opId()),
                  new BrokerPartitionOperationManifest.Result(
                      error == null
                          ? BrokerPartitionOperationStatus.APPLIED
                          : BrokerPartitionOperationStatus.FAILED,
                      System.currentTimeMillis(),
                      error == null ? "" : messageOf(error)),
                  new BrokerPartitionOperationManifest.Evidence(
                      new RaftConfigurationReference(
                          partitions
                              .group(request.partitionId())
                              .map(group -> group.raftPartition().electionState())
                              .map(ElectionState::configurationIndex)
                              .orElse(0L),
                          partitions
                              .group(request.partitionId())
                              .map(group -> group.raftPartition().appliedConfigurationDigest())
                              .orElse("")),
                      partitions.localRecoveryEpoch(request.partitionId()))));
    } catch (final RuntimeException persistenceError) {
      if (error != null) {
        persistenceError.addSuppressed(error);
      }
      result.completeExceptionally(persistenceError);
      return;
    }
    if (error != null) {
      result.completeExceptionally(error);
    } else {
      result.complete(null);
    }
  }

  private boolean effectApplied(final BrokerPartitionOperation request) {
    final var group = partitions.group(request.partitionId());
    if (group.isEmpty()) {
      return false;
    }
    final var observation = group.get().raftPartition().electionState();
    return observation != null
        && observation.configurationIndex() <= group.get().raftPartition().lastAppliedIndex()
        && request
            .targetConfigurationDigest()
            .equals(group.get().raftPartition().appliedConfigurationDigest())
        && ((request.type() != TopologyOperationType.TRANSFER_LEADER
                && request.type() != TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY)
            || request.targetBroker().equals(group.get().raftPartition().leaderId()))
        && (request.type() != TopologyOperationType.RESET_PEERS
            || request.targetRecoveryEpoch()
                == partitions.localRecoveryEpoch(request.partitionId()));
  }

  private static List<String> configurationStages(
      final List<RaftPeer> source, final BrokerPartitionOperation request) {
    final var target =
        request.members().stream()
            .map(
                member -> {
                  final var identity =
                      HostPort.parse(RaftMemberIdentity.of(request.clusterId(), member.brokerId()));
                  return new RaftPeer(
                      member.brokerId(),
                      identity.host(),
                      identity.port(),
                      member.voter(),
                      member.priority());
                })
            .toList();
    if (request.type() == TopologyOperationType.RESET_PEERS) {
      return List.of(digest(source), digest(target));
    }
    final var retained =
        source.stream()
            .filter(
                peer ->
                    peer.voter()
                        || target.stream()
                            .anyMatch(
                                member ->
                                    !member.voter() && member.brokerId().equals(peer.brokerId())))
            .toList();
    final var votersInstalled = new ArrayList<>(target.stream().filter(RaftPeer::voter).toList());
    votersInstalled.addAll(retained.stream().filter(peer -> !peer.voter()).toList());
    return List.of(digest(source), digest(retained), digest(votersInstalled), digest(target));
  }

  private static String digest(final List<RaftPeer> peers) {
    return RaftConfigurationDigest.of(
        peers.stream()
            .map(
                peer ->
                    new RaftConfigurationDigest.Member(
                        peer.brokerId(), peer.host(), peer.port(), peer.voter(), peer.priority()))
            .toList());
  }

  private Optional<BrokerPartitionOperationManifestStore> manifestStore(final int partitionId) {
    return partitions
        .group(partitionId)
        .map(group -> new BrokerPartitionOperationManifestStore(group.raftPartition().storage()));
  }

  private static String messageOf(final Throwable error) {
    final String message = error.getMessage();
    return message == null || message.isBlank() ? error.getClass().getSimpleName() : message;
  }

  private record Execution(
      BrokerPartitionOperation request,
      CompletableActorFuture<Void> result,
      StreamPartition group) {}
}
