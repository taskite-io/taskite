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
package io.taskite.placementdriver.processing.topology;

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;

public final class TopologyMembershipChangeProcessors {
  private TopologyMembershipChangeProcessors() {}

  public static void register(
      final TypedRecordProcessors processors, final TypedRecordProcessorContext context) {
    processors
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.JOIN,
            new PartitionJoinProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.LEAVE,
            new PartitionLeaveProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.CREATE,
            new PartitionCreateProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.PROMOTE,
            new PartitionPromoteProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.CHANGE_PRIORITY,
            new PartitionChangePriorityProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.TRANSFER_LEADER,
            new PartitionTransferLeaderProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.STEP_DOWN_LEADER,
            new PartitionStepDownLeaderProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.RESET_PEERS,
            new PartitionResetPeersProcessor(context))
        .onCommand(
            ValueType.MEMBER_CHANGE, MemberChangeIntent.JOIN, new MemberJoinProcessor(context))
        .onCommand(
            ValueType.MEMBER_CHANGE, MemberChangeIntent.LEAVE, new MemberLeaveProcessor(context))
        .onCommand(
            ValueType.MEMBER_CHANGE,
            MemberChangeIntent.PROMOTE,
            new MemberPromoteProcessor(context))
        .onCommand(
            ValueType.MEMBER_CHANGE,
            MemberChangeIntent.TRANSFER_LEADER,
            new MemberTransferLeaderProcessor(context));
  }
}
