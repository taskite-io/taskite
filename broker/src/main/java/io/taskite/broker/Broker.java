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
package io.taskite.broker;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.taskite.broker.bootstrap.BrokerBootstrapTopology;
import io.taskite.broker.bootstrap.BrokerStartupContext;
import io.taskite.broker.bootstrap.BrokerStartupStep;
import io.taskite.broker.bootstrap.BrokerStartupSteps;
import io.taskite.broker.runtime.BrokerRuntime;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.engine.state.job.DbJobSubscriptionState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.scheduler.Actor;
import io.taskite.scheduler.SchedulingHints;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.ClusterTopology;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Broker {
  private static final Logger LOG = LoggerFactory.getLogger(Broker.class);

  private final BrokerStartupContext context;
  private final AtomicReference<ActorFuture<Broker>> stopFuture = new AtomicReference<>();

  public Broker(BrokerConfig config) {
    this(config, Metrics.globalRegistry);
  }

  public Broker(final BrokerConfig config, final MeterRegistry meterRegistry) {
    context = new BrokerStartupContext(config, meterRegistry, this::stopAfterLocalRemoval);
  }

  public Broker(final BrokerConfig config, final Runnable localBrokerRemoved) {
    this(config, Metrics.globalRegistry, localBrokerRemoved);
  }

  public Broker(
      final BrokerConfig config,
      final MeterRegistry meterRegistry,
      final Runnable localBrokerRemoved) {
    context = new BrokerStartupContext(config, meterRegistry, localBrokerRemoved);
  }

  public BrokerConfig config() {
    return context.config();
  }

  public int port() {
    return context.server().port();
  }

  public int raftPort() {
    return context.runtime().localRaftPort();
  }

  /** Local readiness is independent of process liveness and does not block group startup. */
  public boolean ready() {
    if (!context.schedulerStarted() || stopFuture.get() != null) {
      return false;
    }
    final BrokerRuntime runtime = context.initializedRuntime().orElse(null);
    if (runtime == null) {
      return false;
    }
    final ClusterTopology topology = runtime.raftGroups().topology();
    if (topology == null) {
      return false;
    }
    for (final var assignment : topology.partitions()) {
      if (!assignment.voters().contains(config().brokerId())
          && !assignment.learners().contains(config().brokerId())) {
        continue;
      }
      final var group = runtime.raftGroups().group(assignment.partitionId()).orElse(null);
      if (group == null
          || group.faulted()
          || !group.businessReady()
          || runtime.raftGroups().isAwaitingAssignment(assignment.partitionId())
          || group.lastAppliedIndex() < group.lastIndex()
          || group.raftPartition().leaderId() == null
          || group.raftPartition().leaderId().isBlank()) {
        return false;
      }
      final var election = group.raftPartition().electionState();
      if (election == null
          || group.raftPartition().members().stream()
              .noneMatch(member -> member.memberId().equals(config().brokerId()))
          || runtime.raftGroups().localRecoveryEpoch(assignment.partitionId())
              < assignment.recoveryEpoch()
          || assignment.voters().contains(config().brokerId()) && election.priority() <= 0) {
        return false;
      }
    }
    return runtime.raftGroups().groups().stream()
        .noneMatch(group -> runtime.raftGroups().isAwaitingAssignment(group.partitionId()));
  }

  public Optional<JobSubscriptionRecord> jobSubscription(
      final int partitionId, final long distributionKey) {
    return context
        .runtime()
        .raftGroups()
        .group(partitionId)
        .map(
            group -> {
              final StateStoreProcessingStore store =
                  new StateStoreProcessingStore(group.raftPartition().storage());
              try {
                return new DbJobSubscriptionState(store).get(distributionKey);
              } finally {
                try {
                  store.getCurrentTransaction().rollback();
                } catch (Exception ignored) {
                  // Best-effort cleanup for the read-only transaction context.
                }
              }
            });
  }

  /**
   * Blocks until at least one local raft partition has elected itself leader, or the timeout
   * elapses. Returns {@code true} if a partition leadership was observed. Useful after {@code
   * start()} since partitions bootstrap as followers and elect a leader asynchronously.
   */
  public boolean awaitPartitionLeadership(Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    BrokerRuntime runtime = context.runtime();
    while (System.nanoTime() < deadline) {
      boolean hasLeader =
          runtime.raftGroups().groups().stream().anyMatch(group -> group.role() == RaftRole.LEADER);
      if (hasLeader) {
        return true;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  /**
   * Blocks until the requested local raft partition has elected itself leader, or the timeout
   * elapses.
   */
  public boolean awaitPartitionLeadership(final int partitionId, final Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    BrokerRuntime runtime = context.runtime();
    while (System.nanoTime() < deadline) {
      boolean isLeader =
          runtime
              .raftGroups()
              .group(partitionId)
              .map(group -> group.role() == RaftRole.LEADER)
              .orElse(false);
      if (isLeader) {
        return true;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  public ActorFuture<Broker> start() {
    ActorFuture<Broker> result = new CompletableActorFuture<>();
    context.onInitializationFailure(result::completeExceptionally);
    try {
      context.scheduler().start();
      context.markSchedulerStarted();
      context
          .scheduler()
          .submitActor((Actor) context.concurrencyControl(), SchedulingHints.ioBound())
          .onComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  result.completeExceptionally(failure);
                } else {
                  context
                      .concurrencyControl()
                      .run(() -> startup(0, BrokerStartupSteps.build(), result));
                }
              });
    } catch (Throwable error) {
      result.completeExceptionally(error);
    }
    return result;
  }

  public ActorFuture<Broker> stop() {
    final ActorFuture<Broker> existing = stopFuture.get();
    if (existing != null) {
      return existing;
    }
    final ActorFuture<Broker> result = new CompletableActorFuture<>();
    if (!stopFuture.compareAndSet(null, result)) {
      return stopFuture.get();
    }
    Runnable shutdown =
        () -> {
          try {
            context.close();
          } catch (Throwable error) {
            result.completeExceptionally(error);
            return;
          }
          stopScheduler(result);
        };
    if (context.schedulerStarted()) {
      context.concurrencyControl().run(shutdown);
    } else {
      shutdown.run();
    }
    return result;
  }

  private void stopAfterLocalRemoval() {
    Thread.ofPlatform()
        .name("broker-removed-shutdown-" + config().brokerId())
        .start(
            () -> {
              try {
                stop().join();
              } catch (final RuntimeException error) {
                LOG.warn("failed to stop removed broker {}", config().brokerId(), error);
              }
            });
  }

  private void startup(
      int stepIndex,
      List<BrokerStartupStep<BrokerStartupContext>> steps,
      ActorFuture<Broker> result) {
    if (result.isDone()) {
      return;
    }
    if (stepIndex >= steps.size()) {
      result.complete(this);
      return;
    }

    BrokerStartupStep<BrokerStartupContext> step = steps.get(stepIndex);
    ActorFuture<BrokerStartupContext> startup;
    try {
      startup = step.startup(context);
    } catch (Throwable error) {
      LOG.error("Failed to start step {}, tearing down context", step.name(), error);
      stop()
          .onComplete(
              (ignored, closeError) -> {
                if (closeError != null) {
                  error.addSuppressed(closeError);
                }
                result.completeExceptionally(error);
              });
      return;
    }
    context
        .concurrencyControl()
        .runOnCompletion(
            startup,
            (ignored, failure) -> {
              if (failure != null) {
                LOG.error("Failed to complete step {}, tearing down context", step.name(), failure);
                stop()
                    .onComplete(
                        (ignoredClose, closeError) -> {
                          if (closeError != null) {
                            failure.addSuppressed(closeError);
                          }
                          result.completeExceptionally(failure);
                        });
              } else {
                LOG.info("- Startup {}", step.name());
                startup(stepIndex + 1, steps, result);
              }
            });
  }

  private void stopScheduler(ActorFuture<Broker> result) {
    if (!context.schedulerStarted()) {
      result.complete(this);
      return;
    }
    try {
      context
          .scheduler()
          .stop()
          .onComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  result.completeExceptionally(failure);
                } else {
                  context.markSchedulerStopped();
                  result.complete(this);
                }
              });
    } catch (Throwable error) {
      result.completeExceptionally(error);
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    context.server().blockUntilShutdown();
  }

  static ClusterTopology bootstrapTopology(
      final BrokerConfig config, final BrokerClusterConfig clusterConfig) {
    return BrokerBootstrapTopology.bootstrapTopology(config, clusterConfig);
  }
}
