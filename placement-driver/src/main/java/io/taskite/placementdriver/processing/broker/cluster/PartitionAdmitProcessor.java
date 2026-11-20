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

import io.taskite.placementdriver.broker.cluster.BrokerMigrationBudget;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

/** The serialized processor rechecks admission against the committed reservation set. */
public final class PartitionAdmitProcessor implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String CHANGE_NOT_QUEUED =
      "CHANGE_NOT_QUEUED: Expected to admit topology change '%s' in cluster '%s' at operation index '%d' and generation '%d', but no matching owned QUEUED plan was found";
  private static final String BUDGET_EXHAUSTED =
      "MIGRATION_BUDGET_EXHAUSTED: Expected to admit topology change '%s' in cluster '%s' for partition '%d', but concurrent migrations have exhausted the migration budget";
  private final TypedRecordProcessorContext context;
  private final ProcessingState state;

  public PartitionAdmitProcessor(final TypedRecordProcessorContext context) {
    this.context = context;
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<PartitionChangeRecord> record) {
    final var value = record.getValue();
    final var plan =
        state
            .getBrokerTopologyChangeState()
            .changePlan(value.getClusterId(), value.getChangeId())
            .orElse(null);
    if (plan == null
        || plan.status() != BrokerPartitionChangeStatus.QUEUED
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan)
        || plan.nextOperationIndex() != value.getNextOperationIndex()
        || plan.execution().generation() != value.getExecutionGeneration()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              CHANGE_NOT_QUEUED.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getNextOperationIndex(),
                  value.getExecutionGeneration()));
      return;
    }
    if (!BrokerMigrationBudget.canAdmit(state, plan, BrokerMigrationBudget.DEFAULT)) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              BUDGET_EXHAUSTED.formatted(plan.changeId(), plan.clusterId(), plan.partitionId()));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.ADMITTED,
            BrokerPartitionChangeRecordMapper.fromPlan(
                BrokerMigrationBudget.admit(plan, context.getProcessingTime())));
  }
}
