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

import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.common.mapper.BrokerPartitionChangeRecordMapper;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

/** Persists the Broker's committed execution certificate before any ordinary dispatch. */
public final class PartitionAuthorizeProcessor
    implements TypedRecordProcessor<PartitionChangeRecord> {
  private final Writers writers;
  private static final String CHANGE_NOT_CURRENT =
      "CHANGE_NOT_CURRENT: Expected to authorize topology change '%s' in cluster '%s' at operation index '%d' with generation '%d' and authority index '%d', but the plan is missing, inactive, already authorized, owned by another plan, or the certificate is invalid";
  private final ProcessingState state;

  public PartitionAuthorizeProcessor(final TypedRecordProcessorContext context) {
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
        || plan.isTerminal()
        || plan.status() == BrokerPartitionChangeStatus.QUEUED
        || plan.status() == BrokerPartitionChangeStatus.SUPERSEDING
        || !state.getBrokerTopologyChangeState().ownsPendingPlan(plan)
        || plan.nextOperationIndex() != value.getNextOperationIndex()
        || plan.execution().generation() != 0
        || value.getExecutionGeneration() <= 0
        || value.getAuthorityIndex() <= 0) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              CHANGE_NOT_CURRENT.formatted(
                  value.getChangeId(),
                  value.getClusterId(),
                  value.getNextOperationIndex(),
                  value.getExecutionGeneration(),
                  value.getAuthorityIndex()));
      return;
    }
    final var certificate =
        BrokerPartitionChangeRecordMapper.toPlan(value, plan.status()).execution();
    // The command supplies only a certificate; it cannot replace operations or lineage.
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerPartitionChangeIntent.AUTHORIZED,
            BrokerPartitionChangeRecordMapper.fromPlan(
                plan.withExecution(
                    new ChangePlan.Execution(
                        certificate.generation(),
                        certificate.authorityIndex(),
                        plan.execution().predecessor(),
                        plan.execution().successor(),
                        plan.rootChangeId(),
                        PlanBlockedReason.NONE,
                        0))));
  }
}
