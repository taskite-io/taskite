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
package io.taskite.placementdriver.processing.member;

import io.taskite.cluster.raft.RaftMember;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.scheduler.future.ActorFuture;
import java.util.List;

/**
 * Native single-group membership operations and observations, with no business scheduling state.
 */
public interface PdMemberChangeTransport {
  List<RaftMember> members();

  String leaderId();

  long term();

  long lastApplied();

  ActorFuture<Void> update(List<RaftPeer> peers);

  ActorFuture<Void> transfer(String memberId);
}
