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

import io.taskite.broker.BrokerConfig;
import io.taskite.protocol.Protocol;
import io.taskite.streamplatform.api.backpressure.BackpressureConfiguration;
import java.nio.file.Path;
import java.util.Objects;

/** Configuration owned by the Broker partition runtime. */
public record BrokerPartitionManagerConfig(
    long clusterKey,
    String localBrokerId,
    BrokerConfig.Node node,
    BrokerConfig.Engine engine,
    boolean remoteReplication,
    BackpressureConfiguration backpressure) {

  public BrokerPartitionManagerConfig(
      final long clusterKey,
      final String localBrokerId,
      final BrokerConfig.Node node,
      final BrokerConfig.Engine engine,
      final boolean remoteReplication) {
    this(
        clusterKey,
        localBrokerId,
        node,
        engine,
        remoteReplication,
        BackpressureConfiguration.defaults());
  }

  public BrokerPartitionManagerConfig {
    Protocol.validateClusterKey(clusterKey);
    Objects.requireNonNull(localBrokerId, "localBrokerId");
    Objects.requireNonNull(node, "node");
    Objects.requireNonNull(engine, "engine");
    backpressure = backpressure == null ? BackpressureConfiguration.defaults() : backpressure;
    backpressure.validate();
  }

  public BrokerConfig.Network network() {
    return node.network();
  }

  public Path dataRootDirectory() {
    return node.dataDirectory();
  }

  public BrokerConfig.Processing processing() {
    return engine.processing();
  }

  public BrokerConfig.Snapshot snapshot() {
    return engine.snapshot();
  }

  public BrokerConfig.Consensus consensus() {
    return engine.consensus();
  }

  public static BrokerPartitionManagerConfig from(
      final BrokerConfig config, final long clusterKey, final boolean remoteReplication) {
    Objects.requireNonNull(config, "config");
    return new BrokerPartitionManagerConfig(
        clusterKey,
        config.brokerId(),
        config.node(),
        config.engine(),
        remoteReplication,
        config.backpressure());
  }
}
