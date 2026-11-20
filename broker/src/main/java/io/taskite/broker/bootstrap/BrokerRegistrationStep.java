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

import io.taskite.broker.BrokerConfig;
import io.taskite.broker.registry.BrokerRegistrationActor;
import io.taskite.scheduler.future.ActorFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerRegistrationStep implements BrokerStartupStep<BrokerStartupContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BrokerRegistrationStep.class);

  @Override
  public String name() {
    return "Placement Driver Registration";
  }

  @Override
  public ActorFuture<BrokerStartupContext> startup(final BrokerStartupContext context) {
    return context
        .finishInitializationLease()
        .thenCompose(
            ignored ->
                context
                    .concurrencyControl()
                    .call(
                        () -> {
                          context.requireInitializationLease();
                          final BrokerRegistrationActor registration =
                              new BrokerRegistrationActor(
                                  new BrokerRegistrationActor.Configuration(
                                      context.config(), context.incarnationId()),
                                  new BrokerRegistrationActor.Context(
                                      context.runtime(),
                                      context.concurrencyControl(),
                                      context.placementDriverClient(),
                                      context.localBrokerRemoved()));
                          context.registration(registration);
                          registration.start();
                          final BrokerConfig config = context.config();
                          final var clusterConfig = context.clusterConfig();
                          LOG.info(
                              "broker {} started on API port {}, Raft port {}, clusterId={}, clusterSize={}, partitionSize={}, replicationFactor={}, localPartitionGroups={}, placementDrivers={}, dataDir={}",
                              config.brokerId(),
                              context.server().port(),
                              context.runtime().localRaftPort(),
                              config.clusterId(),
                              clusterConfig.clusterSize(),
                              clusterConfig.partitionSize(),
                              clusterConfig.replicationFactor(),
                              context.runtime().raftGroups().groupCount(),
                              config.registration().placementDrivers(),
                              config.dataDirectory());
                          return context;
                        }));
  }
}
