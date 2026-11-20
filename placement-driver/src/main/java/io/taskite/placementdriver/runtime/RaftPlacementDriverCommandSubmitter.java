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

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.runtime.StreamPartition;
import java.util.Objects;

public final class RaftPlacementDriverCommandSubmitter implements PlacementDriverCommandSubmitter {
  private final StreamPartition raftGroup;
  private final PlacementDriverClusterLeaderLocator leaderLocator;

  public RaftPlacementDriverCommandSubmitter(
      final StreamPartition raftGroup, final PlacementDriverClusterLeaderLocator leaderLocator) {
    this.raftGroup = Objects.requireNonNull(raftGroup, "raftGroup");
    this.leaderLocator = Objects.requireNonNull(leaderLocator, "leaderLocator");
  }

  @Override
  public ActorFuture<LoggedEvent> submitPlacementDriverCommand(final LoggedEvent command) {
    if (!leaderLocator.isLocalLeader()) {
      return CompletableActorFuture.completedExceptionally(
          new PlacementDriverNotLeaderException(
              leaderLocator.locateOtherLeaderEndpointText().orElse(null)));
    }
    return raftGroup.submit(command);
  }
}
