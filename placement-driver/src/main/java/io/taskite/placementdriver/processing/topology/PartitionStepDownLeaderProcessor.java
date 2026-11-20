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

import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;

public final class PartitionStepDownLeaderProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final TopologyPartitionChangeBehavior behavior;

  public PartitionStepDownLeaderProcessor(final TypedRecordProcessorContext context) {
    behavior = new TopologyPartitionChangeBehavior(context);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    behavior.complete(
        record,
        TopologyOperationType.STEP_DOWN_IF_NOT_PRIMARY,
        BrokerPartitionChangeIntent.LEADER_STEPPED_DOWN);
  }
}
