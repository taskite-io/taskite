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
package com.alipay.sofa.jraft.core;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.RepeatedTimer;
import io.taskite.cluster.raft.RaftNodeState;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.stream.Collectors;

/**
 * JRaft 1.4.1 integration boundary. All election state changes run under NodeImpl's write lock.
 * This bridge never changes membership, term, log storage, or disaster-recovery fencing.
 */
public final class TaskiteElectionLifecycle {
  private static final Field LOG_MANAGER = logManagerField();
  private static final Field ELECTION_TIMER = nodeField("electionTimer");
  private static final Field TIMER_STOPPED = timerStoppedField();
  private static final Field CONFIGURATION_CONTEXT = nodeField("confCtx");
  private static final Method CONFIGURATION_BUSY = configurationBusyMethod();

  private static Method configurationBusyMethod() {
    try {
      final var method = CONFIGURATION_CONTEXT.getType().getDeclaredMethod("isBusy");
      method.setAccessible(true);
      return method;
    } catch (final ReflectiveOperationException error) {
      throw new ExceptionInInitializerError(error);
    }
  }

  private static Field nodeField(final String name) {
    try {
      final var field = NodeImpl.class.getDeclaredField(name);
      field.setAccessible(true);
      return field;
    } catch (final ReflectiveOperationException error) {
      throw new ExceptionInInitializerError(error);
    }
  }

  private static Field timerStoppedField() {
    try {
      final var field = RepeatedTimer.class.getDeclaredField("stopped");
      field.setAccessible(true);
      return field;
    } catch (final ReflectiveOperationException error) {
      throw new ExceptionInInitializerError(error);
    }
  }

  private static boolean electionTimerStopped(final NodeImpl node) {
    try {
      return TIMER_STOPPED.getBoolean(ELECTION_TIMER.get(node));
    } catch (final IllegalAccessException error) {
      throw new IllegalStateException("JRaft 1.4.1 election timer inspection failed", error);
    }
  }

  private static Field logManagerField() {
    try {
      final var field = NodeImpl.class.getDeclaredField("logManager");
      field.setAccessible(true);
      return field;
    } catch (final ReflectiveOperationException error) {
      throw new ExceptionInInitializerError(error);
    }
  }

  private TaskiteElectionLifecycle() {}

  /** Captures a complete certificate under the same lock as membership and role changes. */
  public static void withNodeLock(final NodeImpl node, final Runnable action) {
    node.writeLock.lock();
    try {
      action.run();
    } finally {
      node.writeLock.unlock();
    }
  }

  /**
   * Reads the applied configuration directly; the asynchronous election cache may lag a closure.
   */
  public static Observation committedObservation(final NodeImpl node) {
    node.writeLock.lock();
    try {
      if (!configurationSettled(node)) {
        return null;
      }
      final var logs = (LogManager) LOG_MANAGER.get(node);
      final var entry = logs.getConfiguration(node.getLastAppliedLogIndex());
      if (entry == null
          || !entry.isStable()
          || entry.getConf().isEmpty()
          || entry.getId().getIndex() < node.getConf().getId().getIndex()
          || entry.getId().getIndex() > node.getLastAppliedLogIndex()
          || !sameConfiguration(entry.getConf(), node.getConf().getConf())) {
        return null;
      }
      // Snapshot compaction rebases the log manager's configuration entry to the snapshot
      // index, while NodeImpl can retain the original configuration index. Both certificates
      // must be applied and have exactly the same voters, learners and priorities.
      return new Observation(
          new Observation.Membership(
              entry.getConf().copy(), entry.getId().getIndex(), node.getLastAppliedLogIndex()),
          new Observation.Runtime(
              node.getOptions().getElectionPriority(),
              node.getNodeTargetPriority(),
              RaftNodeState.valueOf(node.getNodeState().name()),
              node.getCurrentTerm()));
    } catch (final IllegalAccessException error) {
      throw new IllegalStateException("Cannot read applied configuration", error);
    } finally {
      node.writeLock.unlock();
    }
  }

  /** Called with the node lock; a stable tail alone does not prove its closure has settled. */
  public static boolean configurationSettled(final NodeImpl node) {
    node.writeLock.lock();
    try {
      final var context = CONFIGURATION_CONTEXT.get(node);
      return !(boolean) CONFIGURATION_BUSY.invoke(context)
          && node.getConf().isStable()
          && node.getConf().getId().getIndex() <= node.getLastAppliedLogIndex();
    } catch (final ReflectiveOperationException error) {
      throw new IllegalStateException("Cannot inspect JRaft configuration context", error);
    } finally {
      node.writeLock.unlock();
    }
  }

  /** Source validation and reset installation share the node lock, including every EBUSY retry. */
  public static Status resetConfiguration(
      final NodeImpl node, final Observation source, final Configuration target) {
    node.writeLock.lock();
    try {
      final var actual = node.getConf();
      // Bootstrap's configuration entry is synthesized at index 1. An unknown version may
      // admit that entry, or the covered snapshot index, but never a newer configuration tail.
      final long covered =
          source.configurationIndex() == 0
              ? Math.max(1, source.coveredIndex())
              : source.coveredIndex();
      if (!actual.isStable()
          || actual.getId().getIndex() > covered
          || !sameConfiguration(actual.getConf(), source.configuration())) {
        return new Status(RaftError.EINVAL, "STALE_CONFIGURATION");
      }
      return node.resetPeers(target);
    } finally {
      node.writeLock.unlock();
    }
  }

  private static boolean sameConfiguration(final Configuration a, final Configuration b) {
    return a.listPeers().stream()
            .map(PeerId::toString)
            .collect(Collectors.toSet())
            .equals(b.listPeers().stream().map(PeerId::toString).collect(Collectors.toSet()))
        && a.listLearners().stream()
            .map(PeerId::toString)
            .collect(Collectors.toSet())
            .equals(b.listLearners().stream().map(PeerId::toString).collect(Collectors.toSet()));
  }

  public record Observation(Membership membership, Runtime runtime) {
    public record Membership(Configuration configuration, long index, long coveredIndex) {}

    public record Runtime(int priority, int targetPriority, RaftNodeState state, long term) {}

    public Configuration configuration() {
      return membership.configuration();
    }

    public long configurationIndex() {
      return membership.index();
    }

    public long coveredIndex() {
      return membership.coveredIndex();
    }

    public int priority() {
      return runtime.priority();
    }

    public int targetPriority() {
      return runtime.targetPriority();
    }

    public RaftNodeState state() {
      return runtime.state();
    }

    public long term() {
      return runtime.term();
    }
  }

  /** Called by the FSM snapshot writer. Never takes the node lock or mutates election state. */
  public static Observation snapshotCertificate(
      final NodeImpl node, final RaftOutter.SnapshotMeta meta, final Observation previous) {
    try {
      final var logs = (LogManager) LOG_MANAGER.get(node);
      var entry = logs.getConfiguration(meta.getLastIncludedIndex());
      if (entry != null && !entry.isStable()) {
        entry = logs.getConfiguration(entry.getId().getIndex() - 1);
      }
      if (entry == null || !entry.isStable() || entry.getConf().isEmpty()) {
        return null;
      }
      final long index = entry.getId().getIndex();
      if (previous != null && index <= previous.coveredIndex()) {
        return previous;
      }
      return new Observation(
          new Observation.Membership(entry.getConf().copy(), index, meta.getLastIncludedIndex()),
          new Observation.Runtime(0, 0, RaftNodeState.SNAPSHOT, meta.getLastIncludedTerm()));
    } catch (final IllegalAccessException error) {
      throw new IllegalStateException("Cannot capture snapshot election certificate", error);
    }
  }

  /**
   * Reconciles stable applied membership, or the configuration JRaft itself recovered at startup.
   * Joint/unapplied configurations cannot enable a learner. Callers retain the last observation.
   */
  public static Observation reconcile(final NodeImpl node, final Observation previous) {
    if (!node.writeLock.tryLock()) {
      return previous;
    }
    try {
      if (!node.getNodeState().isActive()) {
        return previous;
      }
      final LogManager logs;
      try {
        logs = (LogManager) LOG_MANAGER.get(node);
      } catch (final IllegalAccessException error) {
        throw new IllegalStateException("JRaft 1.4.1 log manager access failed", error);
      }
      final ConfigurationEntry entry = logs.getConfiguration(node.getLastAppliedLogIndex());
      final boolean applied =
          entry != null
              && entry.isStable()
              && entry.getId().getIndex() > 0
              && (previous == null || entry.getId().getIndex() > previous.coveredIndex())
              && entry.getId().getIndex() <= node.getLastAppliedLogIndex()
              && !entry.getConf().isEmpty();
      // Uncommitted log entries must never authorize a learner promotion on restart. A durable
      // applied certificate (or bootstrap membership for a new group) supplies the safe baseline.
      if (!applied && previous == null) {
        return previous;
      }
      final Configuration configuration =
          applied ? entry.getConf().copy() : previous.configuration().copy();
      final PeerId self = node.getServerId();
      final PeerId voter =
          configuration.listPeers().stream().filter(self::equals).findFirst().orElse(null);
      final int priority = voter == null ? ElectionPriority.NotElected : voter.getPriority();
      if (voter != null && priority != ElectionPriority.Disabled && priority <= 0) {
        throw new IllegalStateException("Invalid recovered voter election priority: " + voter);
      }
      self.setPriority(priority);
      node.getOptions().setElectionPriority(priority);
      // Repair only the timer. Same-term stepDown clears a healthy leader and may leave the timer
      // stopped again when JRaft's unapplied tail still lists self as a learner.
      if (node.getNodeState() == State.STATE_FOLLOWER) {
        try {
          final var timer = (RepeatedTimer) ELECTION_TIMER.get(node);
          if (voter == null) {
            timer.stop();
          } else if (electionTimerStopped(node)) {
            timer.restart();
          }
        } catch (final IllegalAccessException error) {
          throw new IllegalStateException("JRaft election timer repair failed", error);
        }
      }
      return new Observation(
          new Observation.Membership(
              configuration,
              applied ? entry.getId().getIndex() : previous.configurationIndex(),
              applied ? entry.getId().getIndex() : previous.coveredIndex()),
          new Observation.Runtime(
              priority,
              node.getNodeTargetPriority(),
              RaftNodeState.valueOf(node.getNodeState().name()),
              node.getCurrentTerm()));
    } finally {
      node.writeLock.unlock();
    }
  }
}
