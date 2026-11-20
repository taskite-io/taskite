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
import io.taskite.broker.BrokerClusterConfig;
import io.taskite.broker.BrokerConfig;
import io.taskite.broker.common.PlacementDriverBackoff;
import io.taskite.broker.common.PlacementDriverErrorClassifier;
import io.taskite.placementdriver.client.GrpcClient;
import io.taskite.placementdriver.client.request.PlacementDriverRequests;
import io.taskite.placementdriver.client.response.ClusterConfig;
import io.taskite.scheduler.future.ActorFuture;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resolves all cluster-level sizing from PD before any broker runtime service is created. */
public final class BrokerConfigurationStep implements BrokerStartupStep<BrokerStartupContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerConfigurationStep.class);
  private static final long INITIAL_RETRY_DELAY_MILLIS = 1000L;
  private static final long MAX_RETRY_DELAY_MILLIS = 10000L;
  private static final long MAX_STARTUP_DURATION_MILLIS = 300_000L;

  @Override
  public String name() {
    return "Cluster Configuration";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    final BrokerConfig local = context.config();
    if (local.registration().placementDrivers().isBlank()) {
      return failed(
          context,
          new IllegalArgumentException("placement-drivers must be configured for broker startup"));
    }
    LOG.info("Resolving cluster config from placement driver: clusterId={}", local.clusterId());
    final GrpcClient client =
        GrpcClient.connect(
            local.registration().placementDrivers(),
            context.scheduler(),
            local.registration().renewalTimeout());
    context.placementDriverClient(client);
    final ActorFuture<BrokerStartupContext> result = context.concurrencyControl().createFuture();
    registerAndResolve(
        context,
        client,
        local,
        result,
        System.currentTimeMillis() + MAX_STARTUP_DURATION_MILLIS,
        INITIAL_RETRY_DELAY_MILLIS,
        new PlacementDriverBackoff(local.registration()));
    return result;
  }

  private void registerAndResolve(
      final BrokerStartupContext context,
      final GrpcClient client,
      final BrokerConfig local,
      final ActorFuture<BrokerStartupContext> result,
      final long deadline,
      final long retryDelay,
      final PlacementDriverBackoff backoff) {
    client
        .registerBroker(
            PlacementDriverRequests.registerBroker(
                    local.clusterId(),
                    local.brokerId(),
                    local.network().host(),
                    local.network().commandPort(),
                    local.network().raftPort(),
                    local.version(),
                    local.registration().renewalTimeout().toMillis(),
                    List.of())
                .toBuilder()
                .setIncarnationId(context.incarnationId())
                .build())
        .whenComplete(
            (ignored, error) -> {
              if (error == null) {
                backoff.nextDelay(null);
                context.concurrencyControl().run(context::maintainInitializationLease);
                resolve(
                    context, client, local, result, deadline, INITIAL_RETRY_DELAY_MILLIS, backoff);
                return;
              }
              if (isNotRegistered(error)) {
                fail(
                    context,
                    result,
                    new IllegalStateException(
                        "Broker "
                            + local.brokerId()
                            + " cannot start: cluster '"
                            + local.clusterId()
                            + "' is not registered in the placement driver. "
                            + "Register it with POST /clusters before starting brokers.",
                        error));
                return;
              }
              if (isBrokerIdAlreadyRegistered(error)) {
                fail(
                    context,
                    result,
                    new IllegalStateException(
                        "Broker "
                            + local.brokerId()
                            + " cannot start: this broker ID is already active in cluster '"
                            + local.clusterId()
                            + "'. The existing broker must be OFFLINE before the ID can be reused.",
                        error));
                return;
              }
              scheduleRetry(
                  context, client, local, result, error, deadline, retryDelay, true, backoff);
            });
  }

  private void resolve(
      final BrokerStartupContext context,
      final GrpcClient client,
      final BrokerConfig local,
      final ActorFuture<BrokerStartupContext> result,
      final long deadline,
      final long retryDelay,
      final PlacementDriverBackoff backoff) {
    client
        .clusterConfig(local.clusterId())
        .whenComplete(
            (clusterConfig, error) -> {
              if (error == null) {
                complete(context, local, clusterConfig, result);
                return;
              }
              if (isNotRegistered(error)) {
                fail(
                    context,
                    result,
                    new IllegalStateException(
                        "Broker "
                            + local.brokerId()
                            + " cannot start: cluster '"
                            + local.clusterId()
                            + "' is not registered in the placement driver. "
                            + "Register it with POST /clusters before starting brokers.",
                        error));
                return;
              }
              if (!isTransient(error)) {
                fail(context, result, error);
                return;
              }
              scheduleRetry(
                  context, client, local, result, error, deadline, retryDelay, false, backoff);
            });
  }

  private void scheduleRetry(
      final BrokerStartupContext context,
      final GrpcClient client,
      final BrokerConfig local,
      final ActorFuture<BrokerStartupContext> result,
      final Throwable error,
      final long deadline,
      final long retryDelay,
      final boolean register,
      final PlacementDriverBackoff backoff) {
    if (!isTransient(error)) {
      fail(context, result, error);
      return;
    }
    if (System.currentTimeMillis() > deadline) {
      fail(
          context,
          result,
          new IllegalStateException(
              "Failed to resolve cluster configuration within "
                  + MAX_STARTUP_DURATION_MILLIS
                  + " ms",
              error));
      return;
    }
    final long delay =
        PlacementDriverErrorClassifier.isBackpressure(error)
            ? backoff.nextDelay(error).toMillis()
            : ThreadLocalRandom.current()
                .nextLong(Math.max(1, retryDelay / 2), Math.max(2, retryDelay + 1));
    LOG.warn(
        "placement-driver is unavailable while resolving cluster configuration for broker {}; "
            + "retrying in {} ms, cause={}",
        local.brokerId(),
        delay,
        PlacementDriverErrorClassifier.errorMessage(error));
    context
        .concurrencyControl()
        .schedule(
            Duration.ofMillis(delay),
            () -> {
              if (result.isDone()) {
                return;
              }
              final long nextDelay = Math.min(retryDelay * 2, MAX_RETRY_DELAY_MILLIS);
              if (register) {
                registerAndResolve(context, client, local, result, deadline, nextDelay, backoff);
              } else {
                resolve(context, client, local, result, deadline, nextDelay, backoff);
              }
            });
  }

  private void complete(
      final BrokerStartupContext context,
      final BrokerConfig local,
      final ClusterConfig clusterConfig,
      final ActorFuture<BrokerStartupContext> result) {
    context
        .concurrencyControl()
        .run(
            () -> {
              try {
                final BrokerClusterConfig resolved = BrokerClusterConfig.from(local, clusterConfig);
                context.clusterConfig(resolved);
                LOG.info(
                    "Resolved cluster config: clusterKey={}, clusterSize={}, partitionSize={}, replicationFactor={}",
                    resolved.clusterKey(),
                    resolved.clusterSize(),
                    resolved.partitionSize(),
                    resolved.replicationFactor());
                result.complete(context);
              } catch (final Throwable error) {
                result.completeExceptionally(error);
              }
            });
  }

  private static boolean isNotRegistered(final Throwable error) {
    return PlacementDriverErrorClassifier.errorMessage(error).contains("CLUSTER_NOT_REGISTERED");
  }

  private static boolean isBrokerIdAlreadyRegistered(final Throwable error) {
    final Status status = Status.fromThrowable(PlacementDriverErrorClassifier.unwrap(error));
    return status.getCode() == Status.Code.ALREADY_EXISTS
        && PlacementDriverErrorClassifier.errorMessage(error)
            .contains("BROKER_ID_ALREADY_REGISTERED");
  }

  private static boolean isTransient(final Throwable error) {
    final Status status = Status.fromThrowable(PlacementDriverErrorClassifier.unwrap(error));
    if (PlacementDriverErrorClassifier.isBackpressure(error)) {
      return true;
    }
    if (status.getCode() == Status.Code.DEADLINE_EXCEEDED
        || status.getCode() == Status.Code.UNAVAILABLE) {
      return true;
    }
    return status.getCode() == Status.Code.FAILED_PRECONDITION
        && status.getDescription() != null
        && (status.getDescription().startsWith("NOT_LEADER:")
            || status.getDescription().equals("BROKER_REMOVAL_IN_PROGRESS")
            || status.getDescription().startsWith("BROKER_REMOVAL_IN_PROGRESS:"));
  }

  private static ActorFuture<BrokerStartupContext> failed(
      final BrokerStartupContext context, final Throwable error) {
    final ActorFuture<BrokerStartupContext> result = context.concurrencyControl().createFuture();
    result.completeExceptionally(error);
    return result;
  }

  private static void fail(
      final BrokerStartupContext context,
      final ActorFuture<BrokerStartupContext> result,
      final Throwable error) {
    context.concurrencyControl().run(() -> result.completeExceptionally(error));
  }
}
