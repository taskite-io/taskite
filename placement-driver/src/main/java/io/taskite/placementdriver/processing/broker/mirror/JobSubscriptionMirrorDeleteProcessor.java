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
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionMirrorIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class JobSubscriptionMirrorDeleteProcessor
    implements TypedRecordProcessor<JobSubscriptionMirrorRecord> {
  private final Writers writers;
  private static final String INVALID_KEY =
      "Expected to delete a resource mirror with a positive mirrorKey, but was '%d'";
  private static final String MIRROR_NOT_FOUND =
      "JOB_SUBSCRIPTION_MIRROR_NOT_FOUND: Expected to delete resource mirror with key '%d', but no mirror was found";
  private final ProcessingState state;

  public JobSubscriptionMirrorDeleteProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<JobSubscriptionMirrorRecord> record) {
    final long mirrorKey = record.getValue().getMirrorKey();
    if (mirrorKey <= 0) {
      final String reason = INVALID_KEY.formatted(mirrorKey);
      writers.rejection().appendRejection(RejectionType.INVALID_ARGUMENT, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_ARGUMENT, reason);
      return;
    }
    final var existing = state.getJobSubscriptionMirrorState().mirror(mirrorKey);
    if (existing.isEmpty()) {
      final String reason = MIRROR_NOT_FOUND.formatted(mirrorKey);
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.NOT_FOUND, reason);
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(mirrorKey, JobSubscriptionMirrorIntent.DELETED, existing.get());
    writers
        .response()
        .writeEventOnCommand(
            mirrorKey, JobSubscriptionMirrorIntent.DELETED, existing.get(), record);
  }
}
