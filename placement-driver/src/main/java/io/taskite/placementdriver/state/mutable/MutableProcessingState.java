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

import io.taskite.placementdriver.PlacementDriverConfiguration;
import io.taskite.placementdriver.RegistrationPolicy;
import io.taskite.placementdriver.state.broker.DbBrokerClusterState;
import io.taskite.placementdriver.state.broker.DbBrokerRegistrationState;
import io.taskite.placementdriver.state.distribution.DbCommandDistributionState;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.job.DbJobSubscriptionMirrorState;
import io.taskite.placementdriver.state.job.DbJobSubscriptionState;
import io.taskite.placementdriver.state.job.DbJobYieldState;
import io.taskite.placementdriver.state.registration.DbRegistrationDeadlineState;
import io.taskite.placementdriver.state.timer.DbTimerSubscriptionState;
import io.taskite.placementdriver.state.topology.DbBrokerClusterConfigState;
import io.taskite.placementdriver.state.topology.DbBrokerPartitionTopologyState;
import io.taskite.placementdriver.state.topology.DbBrokerTopologyChangeState;
import io.taskite.placementdriver.state.topology.DbBrokerTopologyCoordinatorState;
import io.taskite.placementdriver.state.topology.DbMemberChangeState;
import io.taskite.placementdriver.state.topology.DbPlacementDriverTopologyState;
import io.taskite.placementdriver.state.topology.DbTopologyMembershipChangeState;
import io.taskite.placementdriver.state.worker.DbWorkerRegistrationState;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class MutableProcessingState implements ProcessingState {
  private final MutableBrokerClusterState brokerClusterState;
  private final MutableBrokerClusterConfigState brokerClusterConfigState;
  private final MutableBrokerRegistrationState brokerRegistrationState;
  private final MutableWorkerRegistrationState workerRegistrationState;
  private final MutableJobYieldState jobYieldState;
  private final MutableTimerSubscriptionState timerSubscriptionState;
  private final MutableJobSubscriptionState jobSubscriptionState;
  private final MutableRegistrationDeadlineState registrationDeadlineState;
  private final MutableCommandDistributionState commandDistributionState;
  private final MutableJobSubscriptionMirrorState jobSubscriptionMirrorState;
  private final MutablePlacementDriverTopologyState placementDriverTopologyState;
  private final MutableBrokerPartitionTopologyState brokerPartitionTopologyState;
  private final MutableBrokerTopologyCoordinatorState brokerTopologyCoordinatorState;
  private final MutableBrokerTopologyChangeState brokerTopologyChangeState;
  private final MutableTopologyMembershipChangeState topologyMembershipChangeState;
  private final MutableMemberChangeState memberChangeState;
  private final LongSupplier versionSupplier;
  private final LongSupplier termSupplier;

  public MutableProcessingState(final ProcessingStateStore storage) {
    this(storage, () -> 0, () -> 0, PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT);
  }

  public MutableProcessingState(
      final ProcessingStateStore storage,
      final LongSupplier versionSupplier,
      final LongSupplier termSupplier) {
    this(
        storage,
        versionSupplier,
        termSupplier,
        PlacementDriverConfiguration.DEFAULT_CHANGE_HISTORY_LIMIT);
  }

  public MutableProcessingState(
      final ProcessingStateStore storage,
      final LongSupplier versionSupplier,
      final LongSupplier termSupplier,
      final int changeHistoryLimit) {
    this(
        storage,
        versionSupplier,
        termSupplier,
        changeHistoryLimit,
        new RegistrationPolicy(
            PlacementDriverConfiguration.DEFAULT_BROKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_LEASE_TIMEOUT,
            PlacementDriverConfiguration.DEFAULT_WORKER_OFFLINE_RETENTION));
  }

  public MutableProcessingState(
      final ProcessingStateStore storage,
      final LongSupplier versionSupplier,
      final LongSupplier termSupplier,
      final int changeHistoryLimit,
      final RegistrationPolicy registrationPolicy) {
    final ProcessingStateStore nonNullStorage = Objects.requireNonNull(storage, "storage");
    this.versionSupplier = Objects.requireNonNull(versionSupplier, "versionSupplier");
    this.termSupplier = Objects.requireNonNull(termSupplier, "termSupplier");
    brokerClusterState = new DbBrokerClusterState(nonNullStorage);
    brokerTopologyCoordinatorState = new DbBrokerTopologyCoordinatorState(nonNullStorage);
    brokerClusterConfigState = new DbBrokerClusterConfigState(nonNullStorage);
    registrationDeadlineState = new DbRegistrationDeadlineState(nonNullStorage, registrationPolicy);
    brokerRegistrationState =
        new DbBrokerRegistrationState(nonNullStorage, registrationDeadlineState);
    workerRegistrationState =
        new DbWorkerRegistrationState(nonNullStorage, registrationDeadlineState);
    jobYieldState = new DbJobYieldState(nonNullStorage);
    timerSubscriptionState = new DbTimerSubscriptionState(nonNullStorage);
    jobSubscriptionState = new DbJobSubscriptionState(nonNullStorage);
    commandDistributionState = new DbCommandDistributionState(nonNullStorage);
    jobSubscriptionMirrorState = new DbJobSubscriptionMirrorState(nonNullStorage);
    placementDriverTopologyState = new DbPlacementDriverTopologyState(nonNullStorage);
    brokerPartitionTopologyState =
        new DbBrokerPartitionTopologyState(nonNullStorage, jobYieldState.synchronization());
    topologyMembershipChangeState = new DbTopologyMembershipChangeState(nonNullStorage);
    brokerTopologyChangeState =
        new DbBrokerTopologyChangeState(
            nonNullStorage, changeHistoryLimit, topologyMembershipChangeState);
    memberChangeState =
        new DbMemberChangeState(
            nonNullStorage,
            changeHistoryLimit,
            topologyMembershipChangeState,
            brokerTopologyChangeState);
  }

  @Override
  public MutableBrokerClusterState getBrokerClusterState() {
    return brokerClusterState;
  }

  @Override
  public MutableBrokerClusterConfigState getBrokerClusterConfigState() {
    return brokerClusterConfigState;
  }

  @Override
  public MutableBrokerRegistrationState getBrokerRegistrationState() {
    return brokerRegistrationState;
  }

  @Override
  public MutableWorkerRegistrationState getWorkerRegistrationState() {
    return workerRegistrationState;
  }

  @Override
  public MutableJobYieldState getJobYieldState() {
    return jobYieldState;
  }

  @Override
  public MutableTimerSubscriptionState getTimerSubscriptionState() {
    return timerSubscriptionState;
  }

  @Override
  public MutableJobSubscriptionState getJobSubscriptionState() {
    return jobSubscriptionState;
  }

  @Override
  public MutableRegistrationDeadlineState getRegistrationDeadlineState() {
    return registrationDeadlineState;
  }

  @Override
  public MutableCommandDistributionState getCommandDistributionState() {
    return commandDistributionState;
  }

  @Override
  public MutableJobSubscriptionMirrorState getJobSubscriptionMirrorState() {
    return jobSubscriptionMirrorState;
  }

  @Override
  public MutablePlacementDriverTopologyState getPlacementDriverTopologyState() {
    return placementDriverTopologyState;
  }

  @Override
  public MutableBrokerPartitionTopologyState getBrokerPartitionTopologyState() {
    return brokerPartitionTopologyState;
  }

  @Override
  public MutableBrokerTopologyCoordinatorState getBrokerTopologyCoordinatorState() {
    return brokerTopologyCoordinatorState;
  }

  @Override
  public MutableTopologyMembershipChangeState getTopologyMembershipChangeState() {
    return topologyMembershipChangeState;
  }

  @Override
  public MutableBrokerTopologyChangeState getBrokerTopologyChangeState() {
    return brokerTopologyChangeState;
  }

  @Override
  public MutableMemberChangeState getMemberChangeState() {
    return memberChangeState;
  }

  @Override
  public long getVersion() {
    return versionSupplier.getAsLong();
  }

  @Override
  public long getTerm() {
    return termSupplier.getAsLong();
  }
}
