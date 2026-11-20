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
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class CommandDistributionQueryProcessor
    implements TypedRecordProcessor<CommandDistributionQueryRecord> {
  private final Writers writers;
  private static final String INVALID_KEY =
      "Expected to query a job subscription distribution with a positive distributionKey, but was '%d'";
  private static final String DISTRIBUTION_NOT_FOUND =
      "JOB_SUBSCRIPTION_DISTRIBUTION_NOT_FOUND: Expected to query job subscription distribution '%d', but no distribution was found";
  private final ProcessingState state;

  public CommandDistributionQueryProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionQueryRecord> record) {
    final long distributionKey = record.getValue().getDistributionKey();
    if (distributionKey <= 0) {
      final String reason = INVALID_KEY.formatted(distributionKey);
      writers.rejection().appendRejection(RejectionType.INVALID_ARGUMENT, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_ARGUMENT, reason);
      return;
    }
    final var distributionState = state.getCommandDistributionState();
    final var distribution = distributionState.distribution(distributionKey);
    if (distribution.isEmpty()) {
      final String reason = DISTRIBUTION_NOT_FOUND.formatted(distributionKey);
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.NOT_FOUND, reason);
      return;
    }

    final var query = JobSubscriptionDistributionDetails.view(state, distribution.get(), true);
    writers.state().appendFollowUpEvent(distributionKey, CommandDistributionIntent.QUERIED, query);
    writers
        .response()
        .writeEventOnCommand(distributionKey, CommandDistributionIntent.QUERIED, query, record);
  }
}
