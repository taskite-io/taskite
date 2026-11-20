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
package io.taskite.placementdriver.broker.cluster;

import io.taskite.protocol.Protocol;
import java.util.regex.Pattern;

/** Shared validation for every command that creates or updates cluster sizing. */
public final class BrokerClusterInvariantValidator {
  private static final Pattern CLUSTER_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

  private BrokerClusterInvariantValidator() {}

  public static String validateBrokerClusterConfiguration(
      final BrokerClusterConfiguration configuration) {
    if (configuration == null || !CLUSTER_ID.matcher(configuration.clusterId()).matches()) {
      return "clusterId must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}";
    }
    if (configuration.clusterSize() <= 0
        || configuration.partitionSize() <= 0
        || configuration.partitionSize() > Protocol.MAXIMUM_PARTITIONS
        || configuration.replicationFactor() <= 0) {
      return "clusterSize, partitionSize and replicationFactor must be positive and partitionSize "
          + "must not exceed "
          + Protocol.MAXIMUM_PARTITIONS;
    }
    final int quorum = configuration.replicationFactor() / 2 + 1;
    if (configuration.replicationFactor() > configuration.clusterSize()
        || quorum > configuration.clusterSize()) {
      return "clusterSize must accommodate replicationFactor and its quorum";
    }
    return null;
  }
}
