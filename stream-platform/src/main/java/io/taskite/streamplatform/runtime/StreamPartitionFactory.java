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
package io.taskite.streamplatform.runtime;

import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.scheduler.SchedulingHints;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.snapshot.PartitionStorageSnapshotBackend;
import io.taskite.snapshot.SnapshotDirector;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import io.taskite.streamplatform.api.backpressure.PartitionRequestLimiter;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.command.StreamPartitionCommandDispatcher;
import io.taskite.streamplatform.logstream.LogStream;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.processing.StreamProcessor;
import io.taskite.streamplatform.processing.StreamProcessorContext;
import io.taskite.streamplatform.processing.StreamProcessorMode;
import io.taskite.streamplatform.runtime.snapshot.StreamPartitionSnapshotLifecycle;
import io.taskite.streamplatform.runtime.transition.PartitionRole;
import io.taskite.streamplatform.runtime.transition.PartitionTransition;
import io.taskite.streamplatform.runtime.transition.PartitionTransitionContext;
import io.taskite.streamplatform.runtime.transition.SnapshotDirectorTransitionStep;
import io.taskite.streamplatform.runtime.transition.StreamProcessorTransitionStep;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

/** Creates and wires one {@link StreamPartition}, rolling resources back on synchronous failure. */
public final class StreamPartitionFactory {
  private static final AtomicLong ACTOR_SEQUENCE = new AtomicLong();

  private StreamPartitionFactory() {}

  public static StreamPartition create(
      final StreamPartitionConfig config,
      final JRaftServer jraftServer,
      final ActorScheduler scheduler,
      final StreamPartitionDependencies dependencies) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(jraftServer, "jraftServer");
    Objects.requireNonNull(scheduler, "scheduler");
    Objects.requireNonNull(dependencies, "dependencies");

    final Deque<Runnable> rollback = new ArrayDeque<>();
    try {
      final long infrastructureSequence = ACTOR_SEQUENCE.incrementAndGet();
      final Actor partitionActor =
          Actor.newActor()
              .name(
                  dependencies.actorNamePrefix()
                      + "-raft-group-"
                      + config.partitionId()
                      + "-"
                      + infrastructureSequence)
              .build();
      final Actor lifecycleActor =
          Actor.newActor()
              .name(
                  dependencies.actorNamePrefix()
                      + "-partition-lifecycle-"
                      + config.partitionId()
                      + "-"
                      + infrastructureSequence)
              .build();
      final ActorFuture<Void> partitionActorUp =
          scheduler.submitActor(partitionActor, SchedulingHints.ioBound());
      final ActorFuture<Void> lifecycleActorUp =
          scheduler.submitActor(lifecycleActor, SchedulingHints.ioBound());
      rollback.push(() -> partitionActor.closeAsync().join());
      rollback.push(() -> lifecycleActor.closeAsync().join());

      final PartitionStorage storage =
          new PartitionStorage(RaftStorageLayout.runtimeDirectory(config.dataDirectory()));
      rollback.push(storage::close);

      final AtomicReference<StreamPartition> partitionReference = new AtomicReference<>();
      final StreamPartitionSnapshotLifecycle snapshotLifecycle =
          new StreamPartitionSnapshotLifecycle(
              () -> partitionReference.get().transitionToInactive(),
              () -> partitionReference.get().restoreCurrentRole());
      final JRaftPartition raftPartition =
          createRaftPartition(config, jraftServer, storage, dependencies, snapshotLifecycle);
      rollback.push(raftPartition::close);

      final LogStream logStream =
          new LogStream(
              config.partitionId(),
              raftPartition,
              partitionActor,
              dependencies.logAppendMetricsFactory().create(config.partitionId()),
              config.maxBatchBytes());
      rollback.push(logStream::close);
      final ProcessingStateStore processingStore = new StateStoreProcessingStore(storage);
      final AtomicLong appliedPosition = new AtomicLong(processingStore.getLastProcessedPosition());
      raftPartition.setAppliedPositionSupplier(appliedPosition::get);

      final StreamPartitionCommandDispatcher dispatcher =
          new StreamPartitionCommandDispatcher(
              config.partitionId(),
              dependencies.requestLimiterFactory() == null
                  ? new PartitionRequestLimiter<>(
                      BackpressureConfiguration.fixed(config.maxInFlight()), ignored -> false)
                  : dependencies.requestLimiterFactory().apply(config.partitionId()),
              () -> raftPartition.role() == RaftRole.LEADER,
              logStream::append,
              partitionActor,
              dependencies.commandDispatchMetricsFactory().create(config.partitionId()));
      rollback.push(dispatcher::close);

      final CommittedRecordObserver committedObserver = dependencies.committedRecordObserver();
      final AtomicInteger generation = new AtomicInteger();
      final PartitionTransitionContext transitionContext =
          new PartitionTransitionContext(config.partitionId(), logStream);
      transitionContext.setCurrentTerm(config.initialTerm());
      transitionContext.setCurrentRole(PartitionRole.INACTIVE);
      transitionContext.setConcurrencyControl(lifecycleActor);
      final PartitionTransition transition =
          new PartitionTransition(
              List.of(
                  new StreamProcessorTransitionStep(
                      role ->
                          newStreamProcessor(
                              config,
                              role,
                              generation.incrementAndGet(),
                              dependencies,
                              processingStore,
                              position -> appliedPosition.accumulateAndGet(position, Math::max),
                              raftPartition,
                              logStream,
                              dispatcher,
                              committedObserver),
                      scheduler),
                  new SnapshotDirectorTransitionStep(
                      () ->
                          new SnapshotDirector(
                              config.partitionId(),
                              config.snapshotPolicy(),
                              raftPartition.snapshotProgress(),
                              raftPartition::takeSnapshot,
                              dependencies.snapshotMetricsFactory().create(config.partitionId()),
                              lifecycleActor))));
      transition.setConcurrencyControl(lifecycleActor);
      transition.updateTransitionContext(transitionContext);

      final StreamPartition partition =
          new StreamPartition(
              config,
              raftPartition,
              new StreamPartitionRuntime(
                  new StreamPartitionRuntime.Actors(partitionActor, lifecycleActor, scheduler),
                  new StreamPartitionRuntime.Processing(
                      storage, logStream, transition, transitionContext, dispatcher),
                  appliedPosition::get));
      partitionReference.set(partition);
      logStream.registerRecordAvailableListener(partition::onCommittedLogstreamRecord);
      raftPartition.addRoleChangeListener(partition::onRoleChange);
      raftPartition.addFaultListener(partition::onFault);
      partition.start(partitionActorUp, lifecycleActorUp);
      rollback.clear();
      return partition;
    } catch (final Throwable error) {
      rollback(rollback, error);
      if (config.ownsScheduler()) {
        scheduler.stop().join();
      }
      throw error;
    }
  }

  static void rollback(final Deque<Runnable> resources, final Throwable failure) {
    while (!resources.isEmpty()) {
      try {
        resources.pop().run();
      } catch (final Throwable suppressed) {
        failure.addSuppressed(suppressed);
      }
    }
  }

  private static JRaftPartition createRaftPartition(
      final StreamPartitionConfig config,
      final JRaftServer server,
      final PartitionStorage storage,
      final StreamPartitionDependencies dependencies,
      final StreamPartitionSnapshotLifecycle snapshotLifecycle) {
    final RaftPartitionConfig raftConfig =
        RaftPartitionConfig.builder(
                config.clusterId(), config.partitionId(), config.localMemberId())
            .term(config.initialTerm())
            .role(config.initialRole())
            .peers(config.peers())
            .dataDirectory(config.dataDirectory())
            .electionTimeout(config.electionTimeout())
            .build();
    return JRaftPartition.builder(raftConfig, server, config.dataDirectory())
        .logicalMemberIdentity()
        .storage(storage)
        .snapshotBackend(
            new PartitionStorageSnapshotBackend(
                storage, dependencies.snapshotLifecycleListener(), snapshotLifecycle))
        .priorityFn(RaftPeer::priority)
        .build();
  }

  private static StreamProcessor newStreamProcessor(
      final StreamPartitionConfig config,
      final PartitionRole role,
      final int generation,
      final StreamPartitionDependencies dependencies,
      final ProcessingStateStore store,
      final LongConsumer appliedPositionConsumer,
      final JRaftPartition raftPartition,
      final LogStream logStream,
      final StreamPartitionCommandDispatcher dispatcher,
      final CommittedRecordObserver committedObserver) {
    final StreamProcessorMode mode =
        role == PartitionRole.LEADER ? StreamProcessorMode.PROCESSING : StreamProcessorMode.REPLAY;
    final long sequence = ACTOR_SEQUENCE.incrementAndGet();
    final LogStreamReader reader = logStream.newLogStreamReader();
    return new StreamProcessor(
        new StreamProcessorContext(
            new StreamProcessorContext.Configuration(
                new StreamProcessorContext.Identity(
                    dependencies.clusterKey(),
                    config.partitionId(),
                    dependencies.actorNamePrefix()
                        + "-stream-processor-"
                        + generation
                        + "-"
                        + sequence,
                    mode),
                new StreamProcessorContext.Batching(
                    config.maxCommandsInBatch(),
                    config.maxRecordsInBatch(),
                    config.maxBatchDuration(),
                    config.maxBatchBytes())),
            new StreamProcessorContext.Processing(
                dependencies.eventRegistry(),
                dependencies.typedRecordProcessorsFactory(),
                dependencies.processorContextFactory()),
            new StreamProcessorContext.State(
                store, raftPartition::committedPosition, appliedPositionConsumer),
            new StreamProcessorContext.Log(reader, logStream::append),
            new StreamProcessorContext.Observers(
                dispatcher::onFailure,
                dispatcher::onResponse,
                committedObserver,
                dependencies.processingMetricsFactory())));
  }
}
