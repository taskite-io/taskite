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
package io.taskite.placementdriver.state.immutable;

public interface ProcessingState {
  BrokerClusterState getBrokerClusterState();

  BrokerClusterConfigState getBrokerClusterConfigState();

  BrokerRegistrationState getBrokerRegistrationState();

  default WorkerRegistrationState getWorkerRegistrationState() {
    return WorkerRegistrationState.EMPTY;
  }

  default JobYieldState getJobYieldState() {
    throw new UnsupportedOperationException("Job yield state is unavailable");
  }

  default RegistrationDeadlineState getRegistrationDeadlineState() {
    return RegistrationDeadlineState.EMPTY;
  }

  CommandDistributionState getCommandDistributionState();

  TimerSubscriptionState getTimerSubscriptionState();

  JobSubscriptionState getJobSubscriptionState();

  JobSubscriptionMirrorState getJobSubscriptionMirrorState();

  PlacementDriverTopologyState getPlacementDriverTopologyState();

  BrokerPartitionTopologyState getBrokerPartitionTopologyState();

  default BrokerTopologyCoordinatorState getBrokerTopologyCoordinatorState() {
    throw new UnsupportedOperationException("Broker topology scheduling state is unavailable");
  }

  BrokerTopologyChangeState getBrokerTopologyChangeState();

  default TopologyMembershipChangeState getTopologyMembershipChangeState() {
    throw new UnsupportedOperationException("Topology membership change state is unavailable");
  }

  MemberChangeState getMemberChangeState();

  long getVersion();

  long getTerm();
}
