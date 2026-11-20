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
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRedistributeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class CommandDistributionRedistributeProcessor
    implements TypedRecordProcessor<CommandDistributionRedistributeRecord> {
  private final Writers writers;
  private static final String INVALID_REQUEST =
      "Expected to redistribute a job subscription with positive distributionKey and request timestamp and nonnegative expectedGeneration, but received key '%d', timestamp '%d' and generation '%d'";
  private static final String DISTRIBUTION_NOT_FOUND =
      "JOB_SUBSCRIPTION_DISTRIBUTION_NOT_FOUND: Expected to redistribute subscription with distribution key '%d', but no distribution was found";
  private static final String GENERATION_CONFLICT =
      "JOB_SUBSCRIPTION_DISTRIBUTION_GENERATION_CONFLICT: Expected to redistribute subscription with distribution key '%d' at generation '%d', but the current generation '%d' differs or cannot be incremented; refresh the subscription before retrying";
  private static final String UNSUPPORTED_RESOURCE =
      "Expected to redistribute job subscription '%d', but received valueType '%s'";
  private static final String RESOURCE_DELETED =
      "JOB_SUBSCRIPTION_DELETED: Expected to redistribute subscription with distribution key '%d', but it is deleted or being deleted";
  private static final String MISSING_TARGETS =
      "Expected to redistribute job subscription with distribution key '%d', but it has no persisted distribution targets";
  private static final String TARGET_NOT_FOUND =
      "TARGET_CLUSTER_NOT_FOUND: Expected to redistribute job subscription with distribution key '%d', but target cluster '%s' is missing or has no partitions";
  private final ProcessingState state;

  public CommandDistributionRedistributeProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRedistributeRecord> record) {
    final var request = record.getValue();
    if (request.getDistributionKey() <= 0
        || request.getExpectedGeneration() < 0
        || request.getRequestedAtEpochMillis() <= 0) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_REQUEST.formatted(
              request.getDistributionKey(),
              request.getRequestedAtEpochMillis(),
              request.getExpectedGeneration()));
      return;
    }
    final var existing =
        state.getCommandDistributionState().distribution(request.getDistributionKey());
    if (existing.isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          DISTRIBUTION_NOT_FOUND.formatted(request.getDistributionKey()));
      return;
    }
    final var distribution = existing.get();
    if (distribution.getGeneration() != request.getExpectedGeneration()
        || distribution.getGeneration() == Long.MAX_VALUE) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          GENERATION_CONFLICT.formatted(
              request.getDistributionKey(),
              request.getExpectedGeneration(),
              distribution.getGeneration()));
      return;
    }
    if (distribution.getValueType() != ValueType.JOB_SUBSCRIPTION
        || !(distribution.getValue() instanceof JobSubscriptionRecord)) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          UNSUPPORTED_RESOURCE.formatted(
              request.getDistributionKey(), distribution.getValueType()));
      return;
    }
    if (JobSubscriptionDistributionDetails.deletionKey(state, distribution).isPresent()) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          RESOURCE_DELETED.formatted(request.getDistributionKey()));
      return;
    }
    final var targets = state.getCommandDistributionState().targets(request.getDistributionKey());
    if (targets.isEmpty()) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          MISSING_TARGETS.formatted(request.getDistributionKey()));
      return;
    }
    for (final var target : targets) {
      final var config =
          state.getBrokerClusterConfigState().findClusterConfig(target.getClusterId());
      if (config.isEmpty() || config.get().partitionSize() <= 0) {
        reject(
            record,
            RejectionType.NOT_FOUND,
            TARGET_NOT_FOUND.formatted(request.getDistributionKey(), target.getClusterId()));
        return;
      }
    }
    distribution
        .setGeneration(distribution.getGeneration() + 1)
        .setLastRedistributedAt(request.getRequestedAtEpochMillis());
    writers
        .state()
        .appendFollowUpEvent(
            request.getDistributionKey(), CommandDistributionIntent.REDISTRIBUTED, distribution);
    writers
        .response()
        .writeEventOnCommand(
            request.getDistributionKey(),
            CommandDistributionIntent.REDISTRIBUTED,
            distribution,
            record);
  }

  private void reject(
      final TypedRecord<CommandDistributionRedistributeRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
