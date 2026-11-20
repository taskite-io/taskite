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

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import java.util.Optional;

/** Broker-side execution boundary for placement-driver topology operations. */
public interface BrokerPartitionChangeExecutor {
  default ActorFuture<BrokerPartitionConfiguration> readConfiguration(
      final String clusterId, final int partitionId) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("configuration read is unsupported"));
  }

  default ActorFuture<BrokerPartitionConfiguration> installAuthority(
      final String clusterId,
      final int partitionId,
      final long expectedGeneration,
      final long generation,
      final String owner,
      final long recoveryEpoch) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("authority installation is unsupported"));
  }

  default ActorFuture<Void> apply(final BrokerPartitionOperation request) {
    return switch (request.type()) {
      case ADD -> add(request);
      case JOIN -> join(request);
      case PROMOTE_LEARNER -> promoteLearner(request);
      case LEAVE -> leave(request);
      case RECONFIGURE_PRIORITY -> reconfigurePriority(request);
      case TRANSFER_LEADER, STEP_DOWN_IF_NOT_PRIMARY -> transferLeadership(request);
      case RESET_PEERS -> resetPeers(request);
      default ->
          CompletableActorFuture.completedExceptionally(
              new UnsupportedOperationException(
                  "topology operation is not supported in Phase 2: " + request.type()));
    };
  }

  ActorFuture<Void> add(BrokerPartitionOperation request);

  ActorFuture<Void> join(BrokerPartitionOperation request);

  ActorFuture<Void> promoteLearner(BrokerPartitionOperation request);

  ActorFuture<Void> leave(BrokerPartitionOperation request);

  default ActorFuture<Void> reconfigurePriority(final BrokerPartitionOperation request) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("priority reconfiguration is not supported"));
  }

  default ActorFuture<Void> transferLeadership(final BrokerPartitionOperation request) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("leadership transfer is not supported"));
  }

  default ActorFuture<Void> resetPeers(final BrokerPartitionOperation request) {
    return CompletableActorFuture.completedExceptionally(
        new UnsupportedOperationException("fenced membership reset is not supported"));
  }

  default Optional<BrokerPartitionOperationManifest> latestOperation(final int partitionId) {
    return Optional.empty();
  }
}
