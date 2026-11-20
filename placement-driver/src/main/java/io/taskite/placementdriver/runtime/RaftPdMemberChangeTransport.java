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

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.placementdriver.processing.member.PdMemberChangeTransport;
import io.taskite.scheduler.future.ActorFuture;
import java.util.List;

/** Adapts the owned runtime without taking ownership of its Raft lifecycle. */
public final class RaftPdMemberChangeTransport implements PdMemberChangeTransport {
  private final PlacementDriverRaftRuntime runtime;

  public RaftPdMemberChangeTransport(final PlacementDriverRaftRuntime runtime) {
    this.runtime = runtime;
  }

  @Override
  public List<RaftMember> members() {
    return runtime.raftPartition().members();
  }

  @Override
  public String leaderId() {
    return runtime.raftPartition().leaderId();
  }

  @Override
  public long term() {
    return runtime.raftPartition().term();
  }

  @Override
  public long lastApplied() {
    return runtime.raftPartition().lastAppliedIndex();
  }

  @Override
  public ActorFuture<Void> update(final List<RaftPeer> peers) {
    return runtime.updateRaftMembership(peers);
  }

  @Override
  public ActorFuture<Void> transfer(final String memberId) {
    return runtime.transferLeadership(memberId);
  }
}
