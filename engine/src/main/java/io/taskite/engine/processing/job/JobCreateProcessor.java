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

import io.taskite.engine.processing.variable.VariableBehavior;
import io.taskite.engine.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.LinkedHashMap;
import java.util.Map;

/** Creates jobs only when a subscription matching their type and tenant exists. */
public final class JobCreateProcessor implements TypedRecordProcessor<JobRecord> {
  private final ProcessingState state;
  private final Writers writers;
  private final KeyGenerator keys;
  private final VariableBehavior variables;

  public JobCreateProcessor(
      final ProcessingState state, final Writers writers, final KeyGenerator keys) {
    this.state = state;
    this.writers = writers;
    this.keys = keys;
    variables = new VariableBehavior(state.getVariableState(), writers.state(), keys);
  }

  @Override
  public void processRecord(final TypedRecord<JobRecord> command) {
    final var request = command.getValue();
    final Map<String, Object> requestVariables;
    try {
      requestVariables = request.getVariables();
      if (request.getType() == null
          || request.getType().isBlank()
          || request.getTenantId() == null
          || request.getTenantId().isBlank()
          || request.getRetries() <= 0
          || request.getPriority() < 0
          || requestVariables == null) {
        throw new IllegalArgumentException(
            "Job type and tenant are required; retries must be positive, priority non-negative, variables a JSON object");
      }
    } catch (final RuntimeException error) {
      reject(command, RejectionType.INVALID_ARGUMENT, error.getMessage());
      return;
    }
    final long subscriptionKey =
        state
            .getJobSubscriptionState()
            .findSubscriptionKey(request.getType(), request.getTenantId());
    if (subscriptionKey == 0) {
      reject(
          command,
          RejectionType.NOT_FOUND,
          "No active job subscription for job type '%s' and tenant '%s'"
              .formatted(request.getType(), request.getTenantId()));
      return;
    }
    final Map<String, Object> mergedVariables;
    try {
      final var subscription = state.getJobSubscriptionState().get(subscriptionKey);
      mergedVariables =
          new LinkedHashMap<>(
              new JobRecord().setVariablesJson(subscription.getVariables()).getVariables());
      mergedVariables.putAll(requestVariables);
    } catch (final RuntimeException error) {
      reject(
          command, RejectionType.PROCESSING_ERROR, "Cannot read subscription template variables");
      return;
    }
    final var created =
        new JobRecord()
            .setJobType(request.getType())
            .setTenantId(request.getTenantId())
            .setSubscriptionKey(subscriptionKey)
            .setRetries(request.getRetries())
            .setPriority(request.getPriority())
            .setVariablesMap(mergedVariables);
    final long key = keys.nextKey();
    writers.state().appendFollowUpEvent(key, ValueType.JOB, JobIntent.CREATED, created);
    variables.merge(key, created.getTenantId(), mergedVariables);
    writers.response().writeEventOnCommand(key, JobIntent.CREATED, created, command);
  }

  private void reject(
      final TypedRecord<JobRecord> command, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(command, type, reason);
  }
}
