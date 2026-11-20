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
package io.taskite.engine.processing.job;

import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobYieldRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.broker.JobYieldIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.writer.Writers;

public final class WorkerSessionFenceProcessor implements TypedRecordProcessor<JobYieldRecord> {
  private static final String FEATURE_DISABLED =
      "Expected to fence Worker '%s' with lease token '%d' for tenant '%s' and yield id '%d', but"
          + " Worker job lease tokens are disabled";
  private static final String INVALID_YIELD_IDENTITY =
      "Expected to fence a worker session with positive yield id and lease token, non-blank tenant,"
          + " Worker and cluster ids of at most 256 characters, and non-negative partition id, but"
          + " received yield id '%d', lease token '%d', tenant '%s', Worker '%s', cluster '%s',"
          + " and partition '%d'";
  private static final String YIELD_IDENTITY_CONFLICT =
      "Expected to fence Worker '%s' with lease token '%d' for tenant '%s' in cluster '%s' partition"
          + " '%d' using yield id '%d', but that id belongs to Worker '%s' with lease token '%d'"
          + " for tenant '%s' in cluster '%s' partition '%d'";

  private final ProcessingState state;
  private final Writers writers;

  public WorkerSessionFenceProcessor(final ProcessingState state, final Writers writers) {
    this.state = state;
    this.writers = writers;
  }

  @Override
  public void processRecord(final TypedRecord<JobYieldRecord> command) {
    final var request = command.getValue();
    if (!state.workerJobLeaseTokenEnabled()) {
      reject(
          command,
          RejectionType.FEATURE_DISABLED,
          String.format(
              FEATURE_DISABLED,
              request.getWorkerId(),
              request.getWorkerLeaseToken(),
              request.getTenantId(),
              request.getYieldId()));
      return;
    }
    if (request.getYieldId() <= 0
        || request.getWorkerLeaseToken() <= 0
        || request.getWorkerId().isBlank()
        || request.getWorkerId().length() > 256
        || request.getTenantId().isBlank()
        || request.getTenantId().length() > 256
        || request.getClusterId().isBlank()
        || request.getClusterId().length() > 256
        || request.getPartitionId() < 0) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_YIELD_IDENTITY,
              request.getYieldId(),
              request.getWorkerLeaseToken(),
              request.getTenantId(),
              request.getWorkerId(),
              request.getClusterId(),
              request.getPartitionId()));
      return;
    }
    final var existing = state.getJobYieldState().get(request.getYieldId());
    if (existing != null) {
      if (!existing.sameIdentity(request)) {
        reject(
            command,
            RejectionType.ALREADY_EXISTS,
            String.format(
                YIELD_IDENTITY_CONFLICT,
                request.getWorkerId(),
                request.getWorkerLeaseToken(),
                request.getTenantId(),
                request.getClusterId(),
                request.getPartitionId(),
                request.getYieldId(),
                existing.getWorkerId(),
                existing.getWorkerLeaseToken(),
                existing.getTenantId(),
                existing.getClusterId(),
                existing.getPartitionId()));
        return;
      }
      writers
          .response()
          .writeEventOnCommand(command.getKey(), JobYieldIntent.SESSION_FENCED, existing, command);
      return;
    }
    writers.state().appendFollowUpEvent(command.getKey(), JobYieldIntent.SESSION_FENCED, request);
    writers
        .response()
        .writeEventOnCommand(command.getKey(), JobYieldIntent.SESSION_FENCED, request, command);
  }

  private void reject(
      final TypedRecord<JobYieldRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
