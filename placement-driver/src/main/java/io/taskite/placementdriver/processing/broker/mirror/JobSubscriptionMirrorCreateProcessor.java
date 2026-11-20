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
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorCreationOutcome;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;

public final class JobSubscriptionMirrorCreateProcessor
    implements TypedRecordProcessor<JobSubscriptionMirrorRecord> {
  private final Writers writers;
  private static final String INVALID_CLUSTERS =
      "INVALID_JOB_SUBSCRIPTION_MIRROR: Expected to create a resource mirror between distinct nonblank clusters, but received source '%s' and target '%s'";
  private static final String SOURCE_NOT_FOUND =
      "SOURCE_CLUSTER_NOT_FOUND: Expected to create a resource mirror from cluster '%s' to cluster '%s', but the source cluster is not registered";
  private static final String TARGET_NOT_FOUND =
      "TARGET_CLUSTER_NOT_FOUND: Expected to create a resource mirror from cluster '%s' to cluster '%s', but the target cluster is not registered";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;

  public JobSubscriptionMirrorCreateProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
  }

  @Override
  public void processRecord(final TypedRecord<JobSubscriptionMirrorRecord> record) {
    final JobSubscriptionMirrorRecord requested = record.getValue();
    final String sourceClusterId = requested.getSourceClusterId();
    final String targetClusterId = requested.getTargetClusterId();
    if (sourceClusterId.isBlank()
        || targetClusterId.isBlank()
        || sourceClusterId.equals(targetClusterId)) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_CLUSTERS.formatted(sourceClusterId, targetClusterId));
      return;
    }
    if (state.getBrokerClusterConfigState().findClusterConfig(sourceClusterId).isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          SOURCE_NOT_FOUND.formatted(sourceClusterId, targetClusterId));
      return;
    }
    if (state.getBrokerClusterConfigState().findClusterConfig(targetClusterId).isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          TARGET_NOT_FOUND.formatted(sourceClusterId, targetClusterId));
      return;
    }
    final var existing =
        state.getJobSubscriptionMirrorState().mirror(sourceClusterId, targetClusterId);
    if (existing.isPresent()) {
      final JobSubscriptionMirrorRecord response =
          copy(existing.get()).setOutcome(JobSubscriptionMirrorCreationOutcome.EXISTS);
      writers
          .state()
          .appendFollowUpEvent(
              response.getMirrorKey(), JobSubscriptionMirrorIntent.CREATED, response);
      writers
          .response()
          .writeEventOnCommand(
              response.getMirrorKey(), JobSubscriptionMirrorIntent.CREATED, response, record);
      return;
    }
    final long mirrorKey = keyGenerator.nextKey();
    final JobSubscriptionMirrorRecord created =
        copy(requested)
            .setMirrorKey(mirrorKey)
            .setOutcome(JobSubscriptionMirrorCreationOutcome.CREATED);
    writers.state().appendFollowUpEvent(mirrorKey, JobSubscriptionMirrorIntent.CREATED, created);
    writers
        .response()
        .writeEventOnCommand(mirrorKey, JobSubscriptionMirrorIntent.CREATED, created, record);
  }

  private void reject(
      final TypedRecord<JobSubscriptionMirrorRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }

  private static JobSubscriptionMirrorRecord copy(final JobSubscriptionMirrorRecord value) {
    return new JobSubscriptionMirrorRecord()
        .setMirrorKey(value.getMirrorKey())
        .setSourceClusterId(value.getSourceClusterId())
        .setTargetClusterId(value.getTargetClusterId())
        .setCreatedAtEpochMillis(value.getCreatedAtEpochMillis())
        .setOutcome(value.getOutcome());
  }
}
