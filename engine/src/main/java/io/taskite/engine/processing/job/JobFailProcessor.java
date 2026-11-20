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

import io.taskite.engine.processing.common.WorkerJobLeaseCheck;
import io.taskite.engine.processing.variable.VariableBehavior;
import io.taskite.engine.state.immutable.IncidentState;
import io.taskite.engine.state.immutable.JobState;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.IncidentRecord;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.IncidentIntent;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.value.broker.ErrorType;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

public final class JobFailProcessor implements TypedRecordProcessor<JobRecord> {
  private static final String PROCESSING_TIME_REQUIRED =
      "Expected to construct job failure processor, but the processing time supplier was null";
  private static final String JOB_NOT_FOUND =
      "Expected to fail job with key '%d' for tenant '%s', but no such job was found";
  private static final String INVALID_WORKER_SESSION =
      "Expected to fail job with key '%d' for tenant '%s' using its active Worker session, but"
          + " requested Worker is '%s' with lease token '%d', stored Worker is '%s' with lease token"
          + " '%d', job state is '%s', and worker session recovery readiness is '%s'";
  private static final String INVALID_JOB_STATE =
      "Expected to fail job with key '%d' for tenant '%s' in state 'ACTIVATABLE' or 'ACTIVATED', but it is in state '%s'";
  private static final String INVALID_RETRIES =
      "Expected to fail job with key '%d' for tenant '%s' with non-negative retries, but received '%d'";
  private static final String INVALID_RETRY_BACKOFF =
      "Expected to fail job with key '%d' for tenant '%s' with non-negative retry backoff, but received '%d'";
  private static final String INVALID_FAILURE_REQUEST =
      "Expected to fail job with key '%d' for tenant '%s' with valid variables and a supported recurring time for"
          + " retry backoff '%d', but validation failed: %s";
  private static final String VARIABLES_REQUIRED =
      "Expected to fail job with key '%d' for tenant '%s' with a JSON object of variables, but variables were null";
  private static final String NO_RETRIES_LEFT =
      "Expected to retry job with key '%d' for tenant '%s' but no retries remain";

  private final ProcessingState state;
  private final Writers writers;
  private final KeyGenerator keys;
  private final LongSupplier processingTime;
  private final VariableBehavior variableBehavior;
  private final WorkerJobLeaseCheck leaseCheck;

  public JobFailProcessor(
      final ProcessingState state,
      final Writers writers,
      final KeyGenerator keys,
      final LongSupplier processingTime) {
    this.state = state;
    leaseCheck = new WorkerJobLeaseCheck(state);
    this.writers = writers;
    this.keys = keys;
    this.processingTime = Objects.requireNonNull(processingTime, PROCESSING_TIME_REQUIRED);
    variableBehavior = new VariableBehavior(state.getVariableState(), writers.state(), keys);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final var stored = state.getJobState().getJob(command.getKey());
    if (stored == null || !Objects.equals(stored.getTenantId(), command.getValue().getTenantId())) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          String.format(JOB_NOT_FOUND, command.getKey(), command.getValue().getTenantId()));
      return;
    }
    if (!leaseCheck.canFinish(command.getKey(), stored, command.getValue())) {
      reject(
          command,
          leaseCheck.ready()
              ? RejectionType.STALE_WORKER_SESSION
              : RejectionType.WORKER_SESSION_NOT_READY,
          String.format(
              INVALID_WORKER_SESSION,
              command.getKey(),
              command.getValue().getTenantId(),
              command.getValue().getWorkerId(),
              command.getValue().getWorkerLeaseToken(),
              stored.getWorkerId(),
              stored.getWorkerLeaseToken(),
              state.getJobState().getState(command.getKey()),
              leaseCheck.ready()));
      return;
    }
    final var status = state.getJobState().getState(command.getKey());
    if (status != JobState.State.ACTIVATABLE && status != JobState.State.ACTIVATED) {
      reject(
          command,
          RejectionType.INVALID_STATE,
          String.format(
              INVALID_JOB_STATE, command.getKey(), command.getValue().getTenantId(), status));
      return;
    }
    final var request = command.getValue();
    if (request.getRetries() < 0) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_RETRIES, command.getKey(), request.getTenantId(), request.getRetries()));
      return;
    }
    if (request.getRetryBackoff() < 0) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_RETRY_BACKOFF,
              command.getKey(),
              request.getTenantId(),
              request.getRetryBackoff()));
      return;
    }
    final JobRecord failed;
    final Map<String, Object> variables;
    try {
      variables =
          Objects.requireNonNull(
              request.getVariables(),
              () -> String.format(VARIABLES_REQUIRED, command.getKey(), request.getTenantId()));
      failed =
          stored
              .copy()
              .setRetries(request.getRetries())
              .setErrorMessage(request.getErrorMessage())
              .setRetryBackoff(request.getRetryBackoff())
              .setDeadline(0)
              .setRecurringTime(
                  request.getRetries() > 0 && request.getRetryBackoff() > 0
                      ? Math.addExact(processingTime.getAsLong(), request.getRetryBackoff())
                      : 0);
    } catch (final RuntimeException error) {
      reject(
          command,
          RejectionType.INVALID_ARGUMENT,
          String.format(
              INVALID_FAILURE_REQUEST,
              command.getKey(),
              request.getTenantId(),
              request.getRetryBackoff(),
              error.getMessage()));
      return;
    }
    writers.state().appendFollowUpEvent(command.getKey(), JobIntent.FAILED, failed);
    variableBehavior.merge(command.getKey(), failed.getTenantId(), variables);
    if (failed.getRetries() <= 0
        && state.getIncidentState().getJobIncidentKey(command.getKey())
            == IncidentState.MISSING_INCIDENT) {
      writers
          .state()
          .appendFollowUpEvent(
              keys.nextKey(),
              ValueType.INCIDENT,
              IncidentIntent.CREATED,
              new IncidentRecord()
                  .setTenantId(failed.getTenantId())
                  .setJobKey(command.getKey())
                  .setErrorType(ErrorType.JOB_NO_RETRIES)
                  .setErrorMessage(
                      failed.getErrorMessage() == null || failed.getErrorMessage().isBlank()
                          ? String.format(NO_RETRIES_LEFT, command.getKey(), failed.getTenantId())
                          : failed.getErrorMessage()));
    }
    writers.response().writeEventOnCommand(command.getKey(), JobIntent.FAILED, failed, command);
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
