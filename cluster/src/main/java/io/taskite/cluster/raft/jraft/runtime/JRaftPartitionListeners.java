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
package io.taskite.cluster.raft.jraft.runtime;

import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.membership.JRaftMemberView;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Additive listener registry bridging JRaft FSM callbacks to partition consumers. */
public final class JRaftPartitionListeners implements JRaftStateListener {
  private final JRaftMemberView memberView;
  private final Supplier<String> leaderIdSupplier;
  private final CopyOnWriteArrayList<Consumer<RaftRole>> roleListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Consumer<String>> leaderListeners =
      new CopyOnWriteArrayList<>();
  private final CopyOnWriteArrayList<Consumer<String>> faultListeners =
      new CopyOnWriteArrayList<>();

  public JRaftPartitionListeners(
      final JRaftMemberView memberView, final Supplier<String> leaderIdSupplier) {
    this.memberView = Objects.requireNonNull(memberView, "memberView");
    this.leaderIdSupplier = Objects.requireNonNull(leaderIdSupplier, "leaderIdSupplier");
  }

  public void addRoleListener(final Consumer<RaftRole> listener) {
    roleListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  public void addLeaderListener(final Consumer<String> listener) {
    leaderListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  public void addFaultListener(final Consumer<String> listener) {
    faultListeners.add(Objects.requireNonNull(listener, "listener"));
  }

  @Override
  public void onRoleChange(final RaftRole role) {
    roleListeners.forEach(listener -> listener.accept(role));
  }

  @Override
  public void onLeaderChange(final String leaderId) {
    final String memberId =
        leaderId == null ? leaderIdSupplier.get() : memberView.memberIdOf(leaderId);
    leaderListeners.forEach(listener -> listener.accept(memberId));
  }

  @Override
  public void onFault(final String reason) {
    faultListeners.forEach(listener -> listener.accept(reason));
  }
}
