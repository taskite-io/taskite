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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRequestRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.LinkedHashSet;
import java.util.Set;

public final class CommandDistributionStartProcessor
    implements TypedRecordProcessor<CommandDistributionRequestRecord<?>> {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final Writers writers;
  private final ProcessingState state;
  private final KeyGenerator keys;

  public CommandDistributionStartProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keys = context.getKeyGenerator();
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionRequestRecord<?>> record) {
    final var request = record.getValue();
    final String clusterId;
    try {
      clusterId = validate(request);
    } catch (final IllegalArgumentException error) {
      reject(record, RejectionType.INVALID_ARGUMENT, error.getMessage());
      return;
    }
    final var distributions = state.getCommandDistributionState();
    final var duplicate =
        distributions.distributionKeyByDedupeKey(CommandDistributionDedupe.dedupeKey(request));
    if (duplicate.isPresent()) {
      final var existing = distributions.distribution(duplicate.get());
      if (existing.isEmpty()) {
        reject(record, RejectionType.PROCESSING_ERROR, "Missing deduplicated distribution");
        return;
      } else if (request.getValueType() == ValueType.JOB_SUBSCRIPTION
          && JobSubscriptionDistributionDetails.deletionKey(state, existing.get())
              .filter(distributions::isPlanComplete)
              .isPresent()) {
        // All partitions deleted the old subscription. A new STARTED event replaces its index.
      } else if (request.getValueType() != ValueType.JOB_SUBSCRIPTION
          && !CommandDistributionDedupe.sameCommandValue(request, existing.get())) {
        reject(
            record,
            RejectionType.ALREADY_EXISTS,
            "Subscription deletion conflicts with the existing distribution");
        return;
      } else {
        writers
            .response()
            .writeEventOnCommand(
                duplicate.get(), CommandDistributionIntent.STARTED, existing.get(), record);
        return;
      }
    }
    if (request.getValue() instanceof JobSubscriptionDeletionRecord deletion
        && distributions
            .distribution(deletion.getSubscriptionKey())
            .filter(target -> CommandDistributionDedupe.targetsSubscription(deletion, target))
            .isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          "Job subscription does not exist in this cluster and tenant");
      return;
    }
    final Set<String> targets = new LinkedHashSet<>();
    targets.add(clusterId);
    if (request.getValue() instanceof JobSubscriptionDeletionRecord deletion) {
      distributions
          .targets(deletion.getSubscriptionKey())
          .forEach(target -> targets.add(target.getClusterId()));
    } else {
      state
          .getJobSubscriptionMirrorState()
          .mirrorsFrom(clusterId)
          .forEach(mirror -> targets.add(mirror.getTargetClusterId()));
    }
    for (final String target : targets) {
      if (state.getBrokerClusterConfigState().findClusterConfig(target).isEmpty()) {
        reject(record, RejectionType.NOT_FOUND, "CLUSTER_NOT_REGISTERED: " + target);
        return;
      }
    }
    final long key = keys.nextKey();
    final var distribution =
        new CommandDistributionRecord<>()
            .setDistributionKey(key)
            .setClusterId(clusterId)
            .setValueType(request.getValueType())
            .setIntent(request.getIntent())
            .setValue(request.getValue());
    writers.state().appendFollowUpEvent(key, CommandDistributionIntent.STARTED, distribution);
    writers
        .response()
        .writeEventOnCommand(key, CommandDistributionIntent.STARTED, distribution, record);
  }

  private static String validate(final CommandDistributionRequestRecord<?> request) {
    if (request.getValueType() == ValueType.JOB_SUBSCRIPTION
        && request.getIntent() == JobSubscriptionIntent.CREATE.value()
        && request.getValue() instanceof JobSubscriptionRecord subscription) {
      if (subscription.getClusterId().isBlank() || subscription.getJobType().isBlank()) {
        throw new IllegalArgumentException("clusterId and jobType are required");
      }
      if (subscription.getRetries() < 1
          || subscription.getPriority() < 1
          || subscription.getPriority() > 100) {
        throw new IllegalArgumentException(
            "retries must be positive; priority must be between 1 and 100");
      }
      try {
        final var variables = JSON.readTree(subscription.getVariables());
        if (variables == null || !variables.isObject()) {
          throw new IllegalArgumentException("variables must be a JSON object");
        }
      } catch (final java.io.IOException error) {
        throw new IllegalArgumentException("variables must be a JSON object", error);
      }
      return subscription.getClusterId();
    }
    if (request.getValueType() == ValueType.JOB_SUBSCRIPTION_DELETION
        && request.getIntent() == JobSubscriptionIntent.DELETE.value()
        && request.getValue() instanceof JobSubscriptionDeletionRecord deletion
        && !deletion.getClusterId().isBlank()
        && deletion.getSubscriptionKey() > 0) {
      return deletion.getClusterId();
    }
    throw new IllegalArgumentException(
        "Expected a typed job subscription create or delete command");
  }

  private void reject(final TypedRecord<?> record, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
