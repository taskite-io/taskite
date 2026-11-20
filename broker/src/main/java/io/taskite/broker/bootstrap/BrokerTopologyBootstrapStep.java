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

import io.grpc.Status;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
import io.taskite.broker.topology.BrokerTopologyValidation;
import io.taskite.placementdriver.client.Client;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.topology.ClusterTopology;
import io.taskite.topology.Partition;
import io.taskite.topology.PartitionStatus;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves bootstrap topology using the PD client and configuration prepared by the prior step. */
public final class BrokerTopologyBootstrapStep implements BrokerStartupStep<BrokerStartupContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerTopologyBootstrapStep.class);
  private static final long INITIAL_RETRY_DELAY_MILLIS = 1000L;
  private static final long MAX_RETRY_DELAY_MILLIS = 10000L;
  private static final long MAX_STARTUP_DURATION_MILLIS = 300_000L;

  @Override
  public String name() {
    return "Cluster Services";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    final ActorFuture<BrokerStartupContext> result = context.concurrencyControl().createFuture();
    final Client client = context.placementDriverClient();
    if (client == null) {
      result.completeExceptionally(
          new IllegalStateException(
              "cluster configuration must be resolved before cluster services"));
      return result;
    }
    resolveTopology(
        context,
        client,
        context.config(),
        result,
        System.currentTimeMillis() + MAX_STARTUP_DURATION_MILLIS,
        INITIAL_RETRY_DELAY_MILLIS);
    return result;
  }

  private void resolveTopology(
      final BrokerStartupContext context,
      final Client client,
      final BrokerConfig config,
      final ActorFuture<BrokerStartupContext> result,
      final long deadline,
      final long retryDelay) {
    client
        .topology(config.clusterId())
        .whenComplete(
            (topology, error) -> {
              if (result.isDone()) {
                return;
              }
              if (error == null) {
                try {
                  requireBootstrapTopology(context, config, topology);
                  complete(context, config, topology, result);
                } catch (final IllegalStateException incomplete) {
                  scheduleTopologyRetry(
                      context, client, config, result, deadline, retryDelay, incomplete);
                } catch (final RuntimeException invalid) {
                  fail(context, result, invalid);
                }
                return;
              }
              if (isNoBrokerRegistered(error)) {
                scheduleTopologyRetry(context, client, config, result, deadline, retryDelay, error);
                return;
              }
              if (!isTransient(error)) {
                fail(context, result, error);
                return;
              }
              scheduleTopologyRetry(context, client, config, result, deadline, retryDelay, error);
            });
  }

  private static void requireBootstrapTopology(
      final BrokerStartupContext context,
      final BrokerConfig config,
      final ClusterTopology topology) {
    if (topology == null || !config.clusterId().equals(topology.clusterId())) {
      throw new IllegalArgumentException("Placement driver returned an invalid bootstrap topology");
    }
    for (int partitionId = 1;
        partitionId <= context.clusterConfig().partitionSize();
        partitionId++) {
      final Partition partition = topology.partition(partitionId).orElse(null);
      if (partition == null) {
        throw new IllegalStateException(
            "Incomplete topology for cluster "
                + config.clusterId()
                + ", partition "
                + partitionId
                + ", version "
                + topology.version()
                + ": missing partition");
      }
      // PD creates new groups with explicit ADD operations. Do not manufacture a local voter
      // or learner before those operations establish the initial membership.
      if (partition.status() == PartitionStatus.CREATING && partition.voters().isEmpty()) {
        continue;
      }
      BrokerTopologyValidation.requireVoterEndpoints(topology, partition, config.brokerId());
    }
  }

  private void scheduleTopologyRetry(
      final BrokerStartupContext context,
      final Client client,
      final BrokerConfig config,
      final ActorFuture<BrokerStartupContext> result,
      final long deadline,
      final long retryDelay,
      final Throwable error) {
    final long remaining = deadline - System.currentTimeMillis();
    if (remaining <= 0) {
      fail(
          context,
          result,
          new IllegalStateException(
              "Failed to resolve bootstrap topology within " + MAX_STARTUP_DURATION_MILLIS + " ms",
              error));
      return;
    }
    final long delay = Math.min(retryDelay, remaining);
    LOG.warn(
        "placement-driver topology is not ready for broker {}; retrying in {} ms, cause={}",
        config.brokerId(),
        delay,
        PlacementDriverErrorClassifier.errorMessage(error));
    context
        .concurrencyControl()
        .schedule(
            Duration.ofMillis(delay),
            () -> {
              if (!result.isDone()) {
                resolveTopology(
                    context,
                    client,
                    config,
                    result,
                    deadline,
                    Math.min(retryDelay * 2, MAX_RETRY_DELAY_MILLIS));
              }
            });
  }

  private static void complete(
      final BrokerStartupContext context,
      final BrokerConfig config,
      final ClusterTopology topology,
      final ActorFuture<BrokerStartupContext> result) {
    complete(
        context,
        BrokerBootstrapTopology.bootstrapTopology(config, context.clusterConfig(), topology),
        result);
  }

  private static void complete(
      final BrokerStartupContext context,
      final ClusterTopology topology,
      final ActorFuture<BrokerStartupContext> result) {
    context
        .concurrencyControl()
        .run(
            () -> {
              try {
                context.bootstrapTopology(topology);
                result.complete(context);
              } catch (final Throwable error) {
                result.completeExceptionally(error);
              }
            });
  }

  private static boolean isNoBrokerRegistered(final Throwable error) {
    final String message = PlacementDriverErrorClassifier.errorMessage(error);
    return message.contains("NO_BROKER_REGISTERED");
  }

  private static boolean isTransient(final Throwable error) {
    final Status status = Status.fromThrowable(PlacementDriverErrorClassifier.unwrap(error));
    if (status.getCode() == Status.Code.DEADLINE_EXCEEDED
        || status.getCode() == Status.Code.UNAVAILABLE) {
      return true;
    }
    return status.getCode() == Status.Code.FAILED_PRECONDITION
        && status.getDescription() != null
        && status.getDescription().startsWith("NOT_LEADER:");
  }

  private static void fail(
      final BrokerStartupContext context,
      final ActorFuture<BrokerStartupContext> result,
      final Throwable error) {
    context.concurrencyControl().run(() -> result.completeExceptionally(error));
  }
}
