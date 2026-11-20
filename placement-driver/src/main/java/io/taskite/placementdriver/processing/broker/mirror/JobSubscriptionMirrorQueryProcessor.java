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
package io.taskite.placementdriver.processing.broker.mirror;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorQueryRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.List;

public final class JobSubscriptionMirrorQueryProcessor
    implements TypedRecordProcessor<JobSubscriptionMirrorQueryRecord> {
  private final Writers writers;
  private static final String MIRROR_NOT_FOUND =
      "JOB_SUBSCRIPTION_MIRROR_NOT_FOUND: Expected to query resource mirror with key '%d', but no mirror was found";
  private final ProcessingState state;

  public JobSubscriptionMirrorQueryProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<JobSubscriptionMirrorQueryRecord> record) {
    final JobSubscriptionMirrorQueryRecord query = record.getValue();
    if (query.getMirrorKey() > 0) {
      final var mirror = state.getJobSubscriptionMirrorState().mirror(query.getMirrorKey());
      if (mirror.isEmpty()) {
        final String reason = MIRROR_NOT_FOUND.formatted(query.getMirrorKey());
        writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
        writers.response().writeRejectionOnCommand(record, RejectionType.NOT_FOUND, reason);
        return;
      }
      query.setMirrors(List.of(mirror.get()));
    } else {
      query.setMirrors(
          state.getJobSubscriptionMirrorState().mirrors().stream()
              .filter(
                  mirror ->
                      query.getSourceClusterId().isBlank()
                          || mirror.getSourceClusterId().equals(query.getSourceClusterId()))
              .filter(
                  mirror ->
                      query.getTargetClusterId().isBlank()
                          || mirror.getTargetClusterId().equals(query.getTargetClusterId()))
              .toList());
    }
    writers
        .state()
        .appendFollowUpEvent(record.getKey(), JobSubscriptionMirrorIntent.QUERIED, query);
    writers
        .response()
        .writeEventOnCommand(record.getKey(), JobSubscriptionMirrorIntent.QUERIED, query, record);
  }
}
