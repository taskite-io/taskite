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

import io.taskite.placementdriver.state.immutable.CommandDistributionState;
import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.function.BooleanSupplier;

/** Validates the current attempt before scheduling its retry. */
public final class CommandDistributionRetryProcessor
    implements TypedRecordProcessor<CommandDistributionRecord<?>> {
  private final CommandDistributionState state;
  private final Writers writers;
  private final BooleanSupplier active;

  public CommandDistributionRetryProcessor(
      final TypedRecordProcessorContext context, final BooleanSupplier active) {
    state = context.getProcessingState(ProcessingState.class).getCommandDistributionState();
    writers = context.getWriters();
    this.active = active;
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRecord<?>> record) {
    if (!active.getAsBoolean()) {
      return;
    }
    final var command = record.getValue();
    final var target =
        new PendingTarget(
            command.getDistributionKey(), command.getClusterId(), command.getPartitionId());
    final var entry = state.scheduling().get(target).orElse(null);
    final var current = state.distribution(target.distributionKey()).orElse(null);
    if (entry == null
        || current == null
        || current.getGeneration() != command.getGeneration()
        || entry.generation() != command.getGeneration()
        || entry.attemptId() != command.getDispatchAttemptId()
        || entry.dueAt() != command.getNextAttemptAtEpochMillis()
        || !state.isPending(target.distributionKey(), target.clusterId(), target.partitionId())) {
      return;
    }
    final var retry =
        CommandDistributionDispatchProcessor.targetRecord(current, target)
            .setDispatchAttemptId(entry.attemptId())
            .setNextAttemptAtEpochMillis(record.getTimestamp() + 1000);
    writers
        .state()
        .appendFollowUpEvent(target.distributionKey(), CommandDistributionIntent.RETRIED, retry);
  }
}
