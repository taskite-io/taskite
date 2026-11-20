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
package io.taskite.placementdriver.processing.broker.cluster;

import io.taskite.placementdriver.processing.PlacementDriverProcessorDependencies;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;

public final class BrokerClusterProcessors {
  private BrokerClusterProcessors() {}

  public static TypedRecordProcessors registerBrokerClusterProcessors(
      final TypedRecordProcessors processors,
      final TypedRecordProcessorContext context,
      final PlacementDriverProcessorDependencies dependencies) {
    return processors
        .onCommand(
            ValueType.TOPOLOGY,
            TopologyIntent.CONFIGURE_CLUSTER,
            new TopologyConfigureClusterProcessor(
                context, dependencies.clusterChangePlanningService()))
        .onCommand(
            ValueType.TOPOLOGY,
            TopologyIntent.CREATE_CLUSTER,
            new TopologyCreateClusterProcessor(context))
        .onCommand(
            ValueType.TOPOLOGY,
            TopologyIntent.DELETE_CLUSTER,
            new TopologyDeleteClusterProcessor(context))
        .onCommand(
            ValueType.TOPOLOGY,
            TopologyIntent.CANCEL_CLUSTER_CHANGE,
            new TopologyCancelClusterChangeProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.SUBMIT,
            new PartitionSubmitProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.ADMIT,
            new PartitionAdmitProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.UPDATE_STATUS,
            new PartitionUpdateStatusProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.SUPERSEDE,
            new PartitionSupersedeProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.REPLACE,
            new PartitionReplaceProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.AUTHORIZE,
            new PartitionAuthorizeProcessor(context))
        .onCommand(
            ValueType.PARTITION_CHANGE,
            BrokerPartitionChangeIntent.BLOCK,
            new PartitionBlockProcessor(context));
  }
}
