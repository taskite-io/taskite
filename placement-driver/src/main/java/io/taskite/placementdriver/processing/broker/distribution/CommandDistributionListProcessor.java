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
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Comparator;
import java.util.stream.Stream;

public final class CommandDistributionListProcessor
    implements TypedRecordProcessor<CommandDistributionListRecord> {
  private final Writers writers;
  private static final String INVALID_KEY =
      "Expected to list job subscription distributions for cluster '%s' with a nonnegative distributionKey, but was '%d'";
  private final ProcessingState state;

  public CommandDistributionListProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<CommandDistributionListRecord> record) {
    final var query = record.getValue();
    if (query.getDistributionKey() < 0) {
      final String reason = INVALID_KEY.formatted(query.getClusterId(), query.getDistributionKey());
      writers.rejection().appendRejection(RejectionType.INVALID_ARGUMENT, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_ARGUMENT, reason);
      return;
    }
    final var resources =
        state.getCommandDistributionState().distributions().stream()
            .filter(
                resource ->
                    resource.getValueType() == ValueType.JOB_SUBSCRIPTION
                        && resource.getValue() instanceof JobSubscriptionRecord)
            .filter(
                resource ->
                    query.getDistributionKey() == 0
                        || resource.getDistributionKey() == query.getDistributionKey())
            .sorted(
                Comparator.comparingLong(
                        (CommandDistributionRecord<?> resource) -> resource.getDistributionKey())
                    .reversed())
            .map(resource -> JobSubscriptionDistributionDetails.view(state, resource, false))
            .filter(
                resource ->
                    query.getJobType().isEmpty()
                        || resource.getJobType().equals(query.getJobType()))
            .flatMap(
                resource ->
                    Stream.concat(
                            Stream.of(resource.getOriginClusterId()),
                            resource.getTargets().stream().map(target -> target.getClusterId()))
                        .distinct()
                        .map(
                            cluster ->
                                JobSubscriptionDistributionDetails.forCluster(resource, cluster)))
            .filter(
                resource ->
                    query.getClusterId().isEmpty()
                        || resource.getClusterId().equals(query.getClusterId()))
            .toList();
    query.setTotal(resources.size()).setSubscriptions(resources);
    writers.state().appendFollowUpEvent(record.getKey(), CommandDistributionIntent.LISTED, query);
    writers
        .response()
        .writeEventOnCommand(record.getKey(), CommandDistributionIntent.LISTED, query, record);
  }
}
