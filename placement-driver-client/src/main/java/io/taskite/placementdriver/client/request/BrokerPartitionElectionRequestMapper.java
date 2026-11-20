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
package io.taskite.placementdriver.client.request;

import static io.taskite.placementdriver.protocol.BrokerPartitionElectionState.newBuilder;

import io.taskite.placementdriver.protocol.BrokerPartitionElectionState.Builder;
import io.taskite.topology.BrokerPartitionElectionState;

/** Maps Broker election observations onto the public PD request protocol. */
final class BrokerPartitionElectionRequestMapper {
  private BrokerPartitionElectionRequestMapper() {}

  static Builder toProto(final BrokerPartitionElectionState state) {
    return newBuilder()
        .setConfigurationIndex(state.configurationIndex())
        .setConfigurationDigest(state.configurationDigest())
        .setDesiredPriority(state.desiredPriority())
        .setTargetPriority(state.targetPriority())
        .setNodeState(state.nodeState().name())
        .setLeaderId(state.leaderId())
        .setServiceReady(state.serviceReady())
        .setObservedAtEpochMillis(state.observedAtEpochMillis());
  }
}
