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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.state.immutable.BrokerRegistrationState;
import io.taskite.topology.BrokerPartitionState;
import java.time.Instant;
import java.util.List;

public interface MutableBrokerRegistrationState extends BrokerRegistrationState {
  void registerBroker(
      String clusterId,
      String brokerId,
      String host,
      int port,
      int raftPort,
      String version,
      long renewalTimeoutMillis,
      Instant now,
      List<BrokerPartitionState> partitions);

  void registerBroker(
      String clusterId,
      String brokerId,
      String host,
      int port,
      int raftPort,
      String version,
      long renewalTimeoutMillis,
      Instant now,
      List<BrokerPartitionState> partitions,
      String incarnationId);

  void renewBroker(
      String clusterId, String brokerId, Instant now, List<BrokerPartitionState> partitions);

  void markBrokerOffline(String clusterId, String brokerId, Instant now);

  void markBrokerDraining(String clusterId, String brokerId, Instant now);

  void markBrokerOnline(String clusterId, String brokerId);

  void removeBroker(String clusterId, String brokerId);

  void excludeFromPlacement(String clusterId, String brokerId);

  void includeInPlacement(String clusterId, String brokerId);

  void deleteCluster(String clusterId);
}
