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
package io.taskite.cluster.raft.jraft.membership;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.TaskiteElectionLifecycle;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import io.taskite.cluster.raft.RaftNodeState;
import io.taskite.cluster.raft.RaftPartition;
import io.taskite.cluster.raft.storage.RaftStorageLayout;
import io.taskite.topology.RaftConfigurationDigest;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Owns crash recovery certificates, configuration application and election lifecycle repair. */
public final class JRaftElectionCoordinator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(JRaftElectionCoordinator.class);
  private final ScheduledExecutorService worker;
  private final Supplier<Node> nodes;
  private final Consumer<String> faults;
  private final JRaftMemberView members;
  private final JRaftAppliedConfigurationStore store;
  private volatile TaskiteElectionLifecycle.Observation observation;
  private final AtomicReference<TaskiteElectionLifecycle.Observation> pendingSnapshot =
      new AtomicReference<>();
  private final AtomicBoolean queued = new AtomicBoolean();
  private final long intervalMillis;
  private volatile boolean closed;
  private volatile boolean started;

  public record RecoveryConfiguration(
      RaftStorageLayout layout, Configuration initial, Duration verificationInterval) {
    public RecoveryConfiguration {
      Objects.requireNonNull(layout, "layout");
      initial = Objects.requireNonNull(initial, "initial").copy();
      if (verificationInterval == null || verificationInterval.toMillis() <= 0) {
        throw new IllegalArgumentException("verificationInterval must be positive");
      }
    }
  }

  public record Context(Supplier<Node> nodes, Consumer<String> faults, JRaftMemberView members) {}

  public JRaftElectionCoordinator(
      final RecoveryConfiguration configuration, final Context context) {
    nodes = Objects.requireNonNull(context.nodes(), "nodes");
    faults = Objects.requireNonNull(context.faults(), "faults");
    members = context.members();
    final var layout = configuration.layout();
    intervalMillis = configuration.verificationInterval().toMillis();
    store = new JRaftAppliedConfigurationStore(layout.partitionDirectory());
    observation = store.load();
    if (observation == null) {
      observation =
          new TaskiteElectionLifecycle.Observation(
              new TaskiteElectionLifecycle.Observation.Membership(configuration.initial(), 0, 0),
              new TaskiteElectionLifecycle.Observation.Runtime(0, 0, RaftNodeState.BOOTSTRAP, 0));
      try (final var files = Files.list(layout.logDir())) {
        if (files.findAny().isEmpty()) {
          store.save(observation);
        } else {
          observation = JRaftSnapshotElectionCertificate.load(layout.snapshotDir());
          if (observation == null) {
            throw new IllegalStateException(
                "UNCERTIFIED_RAFT_STORAGE: "
                    + layout.partitionDirectory()
                    + "; create a committed snapshot with the previous version before upgrade");
          }
          store.save(observation);
        }
      } catch (final IOException error) {
        throw new IllegalStateException("Cannot inspect Raft bootstrap storage", error);
      }
    }
    worker =
        Executors.newSingleThreadScheduledExecutor(
            task -> {
              final Thread thread =
                  new Thread(task, "taskite-election-" + layout.partitionDirectory().getFileName());
              thread.setDaemon(true);
              return thread;
            });
  }

  public int startupPriority(final PeerId self) {
    return observation.configuration().listPeers().stream()
        .filter(self::equals)
        .mapToInt(PeerId::getPriority)
        .findFirst()
        .orElse(0);
  }

  /** The FSM only publishes an immutable certificate; persistence runs on the partition worker. */
  public void snapshotLoaded(final TaskiteElectionLifecycle.Observation loaded) {
    pendingSnapshot.accumulateAndGet(
        loaded,
        (old, next) -> old == null || next.coveredIndex() >= old.coveredIndex() ? next : old);
    requestReconciliation();
  }

  public void start() {
    reconcile(true);
    started = true;
    worker.scheduleWithFixedDelay(
        () -> reconcile(false), intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    requestReconciliation();
  }

  /** Coalesces notifications without waiting for the coordinator, the node lock, or storage. */
  public void requestReconciliation() {
    if (closed || !started || !queued.compareAndSet(false, true)) {
      return;
    }
    try {
      worker.execute(
          () -> {
            queued.set(false);
            reconcile(false);
          });
    } catch (final RejectedExecutionException error) {
      queued.set(false);
      if (!closed) {
        throw error;
      }
    }
  }

  public TaskiteElectionLifecycle.Observation snapshotCertificate(
      final RaftOutter.SnapshotMeta meta) {
    return nodes.get() instanceof NodeImpl node
        ? TaskiteElectionLifecycle.snapshotCertificate(node, meta, observation)
        : null;
  }

  private void reconcile(final boolean startup) {
    if (closed || !(nodes.get() instanceof NodeImpl node)) {
      return;
    }
    if (!node.getNodeState().isActive()) {
      close();
      return;
    }
    try {
      final var loaded = pendingSnapshot.getAndSet(null);
      if (loaded != null && loaded.coveredIndex() >= observation.coveredIndex()) {
        store.save(loaded);
        observation = loaded;
      }
      final var observed = TaskiteElectionLifecycle.reconcile(node, observation);
      if (observed != null && observed.coveredIndex() > observation.coveredIndex()) {
        store.save(observed);
      }
      observation = observed;
    } catch (final RuntimeException error) {
      LOG.error("Group {} election configuration is invalid", node.getGroupId(), error);
      faults.accept(error.getMessage());
      if (startup) {
        throw error;
      }
    }
  }

  public TaskiteElectionLifecycle.Observation observation() {
    return observation;
  }

  public RaftPartition.ElectionState state() {
    final var observed = observation();
    return observed == null
        ? null
        : new RaftPartition.ElectionState(
            observed.configurationIndex(),
            observed.priority(),
            observed.targetPriority(),
            observed.state(),
            observed.term());
  }

  public String digest() {
    final var observed = observation();
    if (observed == null) {
      return "";
    }
    final ArrayList<RaftConfigurationDigest.Member> entries = new ArrayList<>();
    observed.configuration().listPeers().forEach(peer -> entries.add(entry(peer, true)));
    observed.configuration().listLearners().forEach(peer -> entries.add(entry(peer, false)));
    return RaftConfigurationDigest.of(entries);
  }

  private RaftConfigurationDigest.Member entry(final PeerId peer, final boolean voter) {
    return new RaftConfigurationDigest.Member(
        members.memberIdOf(peer),
        peer.getEndpoint().getIp(),
        peer.getEndpoint().getPort(),
        voter,
        voter ? peer.getPriority() : 0);
  }

  @Override
  public void close() {
    closed = true;
    worker.shutdownNow();
  }
}
