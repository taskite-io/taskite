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
package io.taskite.placementdriver.processing.broker.distribution;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class CommandDistributionCompleteProcessor
    implements TypedRecordProcessor<CommandDistributionRecord<?>> {
  private final Writers writers;
  private final ProcessingState state;

  public CommandDistributionCompleteProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRecord<?>> record) {
    final var request = record.getValue();
    if (request.getDistributionKey() <= 0 || request.getGeneration() < 0) {
      writers
          .rejection()
          .appendRejection(RejectionType.INVALID_ARGUMENT, "Invalid distribution identity");
      return;
    }
    final var distributions = state.getCommandDistributionState();
    final var existing = distributions.distribution(request.getDistributionKey());
    if (existing.isEmpty()) {
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, "Distribution does not exist");
      return;
    }
    final var distribution = existing.get();
    if (distribution.getGeneration() != request.getGeneration()
        || !distributions.isPlanComplete(request.getDistributionKey())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE, "Stale generation or unacknowledged targets");
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            distribution.getDistributionKey(), CommandDistributionIntent.COMPLETED, distribution);
    if (distribution.getValueType() == ValueType.JOB_SUBSCRIPTION
        && distribution.getValue() instanceof JobSubscriptionRecord subscription
        && state.getJobSubscriptionState().get(distribution.getDistributionKey()).isEmpty()
        && JobSubscriptionDistributionDetails.deletionKey(state, distribution).isEmpty()) {
      writers
          .state()
          .appendFollowUpEvent(
              distribution.getDistributionKey(),
              ValueType.JOB_SUBSCRIPTION,
              JobSubscriptionIntent.CREATED,
              subscription);
    } else if (distribution.getValueType() == ValueType.JOB_SUBSCRIPTION_DELETION
        && distribution.getValue() instanceof JobSubscriptionDeletionRecord deletion) {
      writers
          .state()
          .appendFollowUpEvent(
              deletion.getSubscriptionKey(),
              ValueType.JOB_SUBSCRIPTION_DELETION,
              JobSubscriptionIntent.DELETED,
              deletion);
    }
  }
}
