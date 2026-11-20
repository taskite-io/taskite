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
package io.taskite.broker.bootstrap;

import io.taskite.cluster.raft.group.PartitionGroupDescriptor;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.streamplatform.runtime.StreamPartition;
import io.taskite.topology.ClusterTopology;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerPartitionStep implements BrokerStartupStep<BrokerStartupContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerPartitionStep.class);

  @Override
  public String name() {
    return "Bootstrapped Raft Partition";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    final ActorFuture<BrokerStartupContext> result = context.concurrencyControl().createFuture();
    final ClusterTopology clusterTopology = context.bootstrapTopology();
    final List<PartitionGroupDescriptor> descriptors =
        context.runtime().localPartitionDescriptors(clusterTopology);
    bootstrapPartition(context, clusterTopology, descriptors, 0, result);
    return result;
  }

  private void bootstrapPartition(
      final BrokerStartupContext context,
      final ClusterTopology clusterTopology,
      final List<PartitionGroupDescriptor> descriptors,
      final int index,
      final ActorFuture<BrokerStartupContext> result) {
    if (index >= descriptors.size()) {
      result.complete(context);
      return;
    }

    final PartitionGroupDescriptor descriptor = descriptors.get(index);
    final ActorFuture<StreamPartition> partitionCreation =
        context
            .concurrencyControl()
            .call(() -> context.runtime().bootstrapPartition(clusterTopology, descriptor));

    context
        .concurrencyControl()
        .runOnCompletion(
            partitionCreation,
            (group, failure) -> {
              if (failure != null) {
                result.completeExceptionally(failure);
                return;
              }
              awaitPartitionStartup(context, clusterTopology, descriptors, index, group, result);
            });
  }

  private void awaitPartitionStartup(
      final BrokerStartupContext context,
      final ClusterTopology clusterTopology,
      final List<PartitionGroupDescriptor> descriptors,
      final int index,
      final StreamPartition group,
      final ActorFuture<BrokerStartupContext> result) {
    context
        .concurrencyControl()
        .runOnCompletion(
            group.startupFuture(),
            (ignored, failure) -> {
              if (failure != null) {
                result.completeExceptionally(failure);
                return;
              }
              logPartitionStarted(group);
              bootstrapPartition(context, clusterTopology, descriptors, index + 1, result);
            });
  }

  private static void logPartitionStarted(final StreamPartition group) {
    LOG.info("- Started partition {}", group.partitionId());
    LOG.info("    - Role {}, term {}", group.role(), group.term());
    LOG.info("    - Runtime directory {}", group.runtimeDirectory());
  }
}
