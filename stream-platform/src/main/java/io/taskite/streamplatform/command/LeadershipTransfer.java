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
package io.taskite.streamplatform.command;

import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.function.Supplier;

/** State of one transfer, accessed under the command dispatcher's admission lock. */
final class LeadershipTransfer {
  final Supplier<ActorFuture<Void>> transfer;
  final ActorFuture<Void> result = new CompletableActorFuture<>();
  ScheduledTimer timer;
  boolean started;
  boolean acknowledged;
  boolean leadershipLost;

  LeadershipTransfer(final Supplier<ActorFuture<Void>> transfer) {
    this.transfer = transfer;
  }
}
