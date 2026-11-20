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

import io.taskite.placementdriver.state.immutable.CommandDistributionState.PendingTarget;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class CommandDistributionAcknowledgeProcessor
    implements TypedRecordProcessor<CommandDistributionRecord<?>> {
  private final Writers writers;
  private static final String INVALID_KEY =
      "Expected to acknowledge command distribution in cluster '%s' for partition '%d' with a positive distributionKey, but was '%d'";
  private static final String INVALID_PARTITION =
      "Expected to acknowledge command distribution '%d' in cluster '%s' with a positive partitionId, but was '%d'";
  private static final String MISSING_CLUSTER =
      "Expected to acknowledge command distribution '%d' for partition '%d', but clusterId is blank";
  private static final String DISTRIBUTION_NOT_FOUND =
      "Expected to acknowledge command distribution '%d' in cluster '%s' for partition '%d', but no distribution was found";
  private static final String STALE_GENERATION =
      "JOB_SUBSCRIPTION_DISTRIBUTION_GENERATION_CONFLICT: Expected to acknowledge command distribution '%d' in cluster '%s' for partition '%d' at generation '%d', but the current generation is '%d'";
  private static final String CLUSTER_NOT_TARGETED =
      "Expected to acknowledge command distribution '%d' in cluster '%s', but the distribution does not target this cluster";
  private static final String PARTITION_OUT_OF_RANGE =
      "Expected to acknowledge command distribution '%d' in cluster '%s' for partition '%d', but the target partition range is 1 through '%d'";
  private final ProcessingState state;

  public CommandDistributionAcknowledgeProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRecord<?>> record) {
    final CommandDistributionRecord<?> value = record.getValue();
    if (value.getDistributionKey() <= 0) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_KEY.formatted(
                  value.getClusterId(), value.getPartitionId(), value.getDistributionKey()));
      return;
    }
    if (value.getPartitionId() <= 0) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              INVALID_PARTITION.formatted(
                  value.getDistributionKey(), value.getClusterId(), value.getPartitionId()));
      return;
    }
    if (value.getClusterId().isBlank()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              MISSING_CLUSTER.formatted(value.getDistributionKey(), value.getPartitionId()));
      return;
    }
    final var distribution =
        state.getCommandDistributionState().distribution(value.getDistributionKey());
    if (distribution.isEmpty()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.NOT_FOUND,
              DISTRIBUTION_NOT_FOUND.formatted(
                  value.getDistributionKey(), value.getClusterId(), value.getPartitionId()));
      return;
    }
    if (distribution.get().getGeneration() != value.getGeneration()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              STALE_GENERATION.formatted(
                  value.getDistributionKey(),
                  value.getClusterId(),
                  value.getPartitionId(),
                  value.getGeneration(),
                  distribution.get().getGeneration()));
      return;
    }
    final var target =
        state.getCommandDistributionState().targets(value.getDistributionKey()).stream()
            .filter(candidate -> candidate.getClusterId().equals(value.getClusterId()))
            .findFirst();
    if (target.isEmpty()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              CLUSTER_NOT_TARGETED.formatted(value.getDistributionKey(), value.getClusterId()));
      return;
    }
    if (value.getPartitionId() > target.get().getExpectedPartitionCount()) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_ARGUMENT,
              PARTITION_OUT_OF_RANGE.formatted(
                  value.getDistributionKey(),
                  value.getClusterId(),
                  value.getPartitionId(),
                  target.get().getExpectedPartitionCount()));
      return;
    }
    if (!state
        .getCommandDistributionState()
        .isPending(value.getDistributionKey(), value.getClusterId(), value.getPartitionId())) {
      return;
    }
    if (value.getDispatchAttemptId() > 0
        && state
            .getCommandDistributionState()
            .scheduling()
            .get(
                new PendingTarget(
                    value.getDistributionKey(), value.getClusterId(), value.getPartitionId()))
            .filter(entry -> entry.attemptId() == value.getDispatchAttemptId())
            .isEmpty()) {
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            value.getDistributionKey(), CommandDistributionIntent.ACKNOWLEDGED, value);
    final var distributions = state.getCommandDistributionState();
    if (distributions.isPlanComplete(value.getDistributionKey())) {
      writers
          .command()
          .appendFollowUpCommand(
              value.getDistributionKey(), CommandDistributionIntent.COMPLETE, distribution.get());
    }
  }
}
