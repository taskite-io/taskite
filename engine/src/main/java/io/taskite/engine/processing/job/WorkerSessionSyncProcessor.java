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
import java.util.List;

/** The PD supplies ordered pages; a committed final page opens the activation barrier. */
public final class WorkerSessionSyncProcessor implements TypedRecordProcessor<JobYieldRecord> {
  private static final String FEATURE_DISABLED =
      "Expected to synchronize Worker sessions for cluster '%s' partition '%d' at cursor '%d', but"
          + " Worker job lease tokens are disabled";
  private static final String INVALID_SYNC_CURSOR =
      "Expected to synchronize Worker sessions for cluster '%s' partition '%d' with a cursor at"
          + " least '%d' and at most 100 sessions per page, but received cursor '%d' and '%d' sessions";
  private static final String INVALID_SESSION_IDENTITY =
      "Expected to synchronize a worker session with yield id in range (%d, %d], matching cluster"
          + " '%s' and partition '%d', positive lease token and valid ids of at most 256 characters,"
          + " but received yield id '%d', cluster '%s', partition '%d', lease token '%d', tenant"
          + " '%s', and Worker '%s'";
  private static final String SESSION_IDENTITY_CONFLICT =
      "Expected to synchronize yield id '%d' for Worker '%s' with lease token '%d' for tenant '%s'"
          + " in cluster '%s' partition '%d', but it already belongs to Worker '%s' with lease token"
          + " '%d' for tenant '%s' in cluster '%s' partition '%d'";
  private static final String INCOMPLETE_SYNC_PAGE =
      "Expected the worker session page for cluster '%s' partition '%d' to end at cursor '%d', but"
          + " its last yield id or previously synchronized cursor is '%d'";

  private final ProcessingState state;
  private final Writers writers;

  public WorkerSessionSyncProcessor(final ProcessingState state, final Writers writers) {
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
              request.getClusterId(),
              request.getPartitionId(),
              request.getSyncCursor()));
      return;
    }
    final long cursor = state.getJobYieldState().synchronizedCursor();
    if (request.getPreviousSyncCursor() != cursor) {
      final var current =
          request.copy().setSyncCursor(cursor).setSyncComplete(false).setSessions(List.of());
      writers
          .response()
          .writeEventOnCommand(command.getKey(), JobYieldIntent.SESSIONS_SYNCED, current, command);
      return;
    }
    if (request.getSyncCursor() < cursor || request.getSessions().size() > 100) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_SYNC_CURSOR,
              request.getClusterId(),
              request.getPartitionId(),
              cursor,
              request.getSyncCursor(),
              request.getSessions().size()));
      return;
    }
    long last = cursor;
    for (final var session : request.getSessions()) {
      if (session.getYieldId() <= last
          || session.getYieldId() > request.getSyncCursor()
          || !session.getClusterId().equals(request.getClusterId())
          || session.getPartitionId() != request.getPartitionId()
          || session.getWorkerLeaseToken() <= 0
          || session.getTenantId().isBlank()
          || session.getTenantId().length() > 256
          || session.getWorkerId().isBlank()
          || session.getWorkerId().length() > 256
          || session.getClusterId().length() > 256) {
        reject(
            command,
            RejectionType.INVALID_ARGUMENT,
            String.format(
                INVALID_SESSION_IDENTITY,
                last,
                request.getSyncCursor(),
                request.getClusterId(),
                request.getPartitionId(),
                session.getYieldId(),
                session.getClusterId(),
                session.getPartitionId(),
                session.getWorkerLeaseToken(),
                session.getTenantId(),
                session.getWorkerId()));
        return;
      }
      final var existing = state.getJobYieldState().get(session.getYieldId());
      if (existing != null && !existing.sameIdentity(session)) {
        reject(
            command,
            RejectionType.ALREADY_EXISTS,
            String.format(
                SESSION_IDENTITY_CONFLICT,
                session.getYieldId(),
                session.getWorkerId(),
                session.getWorkerLeaseToken(),
                session.getTenantId(),
                session.getClusterId(),
                session.getPartitionId(),
                existing.getWorkerId(),
                existing.getWorkerLeaseToken(),
                existing.getTenantId(),
                existing.getClusterId(),
                existing.getPartitionId()));
        return;
      }
      last = session.getYieldId();
    }
    if (last != request.getSyncCursor()) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INCOMPLETE_SYNC_PAGE,
              request.getClusterId(),
              request.getPartitionId(),
              request.getSyncCursor(),
              last));
      return;
    }
    writers.state().appendFollowUpEvent(command.getKey(), JobYieldIntent.SESSIONS_SYNCED, request);
    writers
        .response()
        .writeEventOnCommand(command.getKey(), JobYieldIntent.SESSIONS_SYNCED, request, command);
  }

  private void reject(
      final TypedRecord<JobYieldRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
