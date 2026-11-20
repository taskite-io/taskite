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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.intent.placementdriver.WorkerIntent;
import io.taskite.streamplatform.api.state.EventAppliers;

public final class PlacementDriverEventAppliers {
  private PlacementDriverEventAppliers() {}

  public static EventAppliers create(final MutableProcessingState state) {
    return new EventAppliers()
        .register(JobSubscriptionIntent.CREATED, new JobSubscriptionCreatedApplier(state))
        .register(JobSubscriptionIntent.DELETED, new JobSubscriptionDeletedApplier(state))
        .register(TimerIntent.CREATED, new TimerCreatedApplier(state))
        .register(TimerIntent.ASSIGNED, new TimerAssignedApplier(state))
        .register(TimerIntent.CANCELED, new TimerCanceledApplier(state))
        .register(TimerIntent.TRIGGERED, new TimerTriggeredApplier(state))
        .register(TopologyIntent.QUERIED, new TopologyQueriedApplier())
        .register(TopologyIntent.CLUSTER_CONFIGURED, new BrokerClusterConfiguredApplier(state))
        .register(TopologyIntent.CLUSTER_CREATED, new BrokerClusterCreatedApplier(state))
        .register(TopologyIntent.CLUSTER_DELETED, new BrokerClusterDeletedApplier(state))
        .register(
            TopologyIntent.CLUSTER_CHANGE_CANCELLED, new BrokerClusterChangeCancelledApplier(state))
        .register(BrokerIntent.REGISTERED, new BrokerRegisteredApplier(state))
        .register(BrokerIntent.RENEWED, new BrokerRenewedApplier(state))
        .register(BrokerIntent.DEREGISTERED, new BrokerDeregisteredApplier(state))
        .register(BrokerIntent.MARKED_OFFLINE, new BrokerMarkedOfflineApplier(state))
        .register(BrokerIntent.DRAINED, new BrokerDrainedApplier(state))
        .register(WorkerIntent.REGISTERED, new WorkerRegisteredApplier(state))
        .register(WorkerIntent.RENEWED, new WorkerRenewedApplier(state))
        .register(WorkerIntent.DEREGISTERED, new WorkerDeregisteredApplier(state))
        .register(WorkerIntent.MARKED_OFFLINE, new WorkerMarkedOfflineApplier(state))
        .register(WorkerIntent.DELETED, new WorkerDeletedApplier(state))
        .register(JobYieldIntent.PROGRESS_UPDATED, new JobYieldProgressUpdatedApplier(state))
        .register(JobYieldIntent.ACKNOWLEDGED, new JobYieldAcknowledgedApplier(state))
        .register(
            JobYieldIntent.PARTITION_SYNC_UPDATED, new JobYieldPartitionSyncUpdatedApplier(state))
        .register(CommandDistributionIntent.STARTED, new CommandDistributionStartedApplier(state))
        .register(
            CommandDistributionIntent.ACKNOWLEDGED,
            new CommandDistributionAcknowledgedApplier(state))
        .register(CommandDistributionIntent.COMPLETED, new CommandDistributionCompletedApplier())
        .register(
            CommandDistributionIntent.DISPATCHED, new CommandDistributionDispatchedApplier(state))
        .register(CommandDistributionIntent.RETRIED, new CommandDistributionRetriedApplier(state))
        .register(CommandDistributionIntent.QUERIED, new CommandDistributionQueriedApplier())
        .register(CommandDistributionIntent.LISTED, new CommandDistributionListedApplier())
        .register(
            CommandDistributionIntent.REDISTRIBUTED,
            new CommandDistributionRedistributedApplier(state))
        .register(
            JobSubscriptionMirrorIntent.CREATED, new JobSubscriptionMirrorCreatedApplier(state))
        .register(
            JobSubscriptionMirrorIntent.DELETED, new JobSubscriptionMirrorDeletedApplier(state))
        .register(JobSubscriptionMirrorIntent.QUERIED, new JobSubscriptionMirrorQueriedApplier())
        .register(BrokerPartitionChangeIntent.SUBMITTED, new PartitionChangeSubmittedApplier(state))
        .register(BrokerPartitionChangeIntent.ADMITTED, new PartitionChangeAdmittedApplier(state))
        .register(BrokerPartitionChangeIntent.CANCELLED, new PartitionChangeCancelledApplier(state))
        .register(
            BrokerPartitionChangeIntent.STATUS_UPDATED,
            new PartitionChangeStatusUpdatedApplier(state))
        .register(
            BrokerPartitionChangeIntent.SUPERSEDING, new PartitionChangeSupersedingApplier(state))
        .register(BrokerPartitionChangeIntent.REPLACED, new PartitionChangeReplacedApplier(state))
        .register(
            BrokerPartitionChangeIntent.AUTHORIZED, new PartitionChangeAuthorizedApplier(state))
        .register(BrokerPartitionChangeIntent.BLOCKED, new PartitionChangeBlockedApplier(state))
        .register(
            BrokerPartitionChangeIntent.EXECUTION_UPDATED,
            new PartitionExecutionUpdatedApplier(state))
        .register(MemberChangeIntent.PLANNED, new MemberChangePlannedApplier(state))
        .register(MemberChangeIntent.ADVANCED, new MemberChangeAdvancedApplier(state))
        .register(MemberChangeIntent.CANCELLED, new MemberChangeCancelledApplier(state))
        .register(MemberChangeIntent.EXECUTION_UPDATED, new MemberExecutionUpdatedApplier(state))
        .register(MemberChangeIntent.JOINED, new MemberJoinedApplier(state))
        .register(MemberChangeIntent.LEFT, new MemberLeftApplier(state))
        .register(MemberChangeIntent.PROMOTED, new MemberPromotedApplier(state))
        .register(MemberChangeIntent.LEADER_TRANSFERRED, new MemberLeaderTransferredApplier(state))
        .register(BrokerPartitionChangeIntent.JOINED, new PartitionJoinedApplier(state))
        .register(BrokerPartitionChangeIntent.LEFT, new PartitionLeftApplier(state))
        .register(BrokerPartitionChangeIntent.CREATED, new PartitionCreatedApplier(state))
        .register(BrokerPartitionChangeIntent.PROMOTED, new PartitionPromotedApplier(state))
        .register(
            BrokerPartitionChangeIntent.PRIORITY_CHANGED,
            new PartitionPriorityChangedApplier(state))
        .register(
            BrokerPartitionChangeIntent.LEADER_TRANSFERRED,
            new PartitionLeaderTransferredApplier(state))
        .register(
            BrokerPartitionChangeIntent.LEADER_STEPPED_DOWN,
            new PartitionLeaderSteppedDownApplier(state))
        .register(BrokerPartitionChangeIntent.PEERS_RESET, new PartitionPeersResetApplier(state))
        .register(TopologyIntent.UPDATED, new PlacementDriverTopologyUpdatedApplier(state))
        .register(TopologyIntent.COORDINATED, new BrokerTopologyCoordinatedApplier(state));
  }
}
