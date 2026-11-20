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
package io.taskite.placementdriver.processing.broker.timer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.ZoneId;
import java.util.HashSet;

/** Validates an independent subscription and emits its durable CREATED event. */
public final class TimerCreateProcessor implements TypedRecordProcessor<TimerSubscriptionRecord> {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private final ProcessingState state;
  private final Writers writers;
  private final KeyGenerator keys;
  private final DueDateTimerChecker checker;

  public TimerCreateProcessor(
      final TypedRecordProcessorContext context, final DueDateTimerChecker checker) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
    keys = context.getKeyGenerator();
    this.checker = checker;
  }

  @Override
  public void processRecord(final TypedRecord<TimerSubscriptionRecord> record) {
    final var timer = record.getValue();
    try {
      if (timer.getClusterId().isBlank()
          || timer.getName().isBlank()
          || timer.getJobTypes().isEmpty()
          || timer.getJobTypes().stream().anyMatch(String::isBlank)
          || new HashSet<>(timer.getJobTypes()).size() != timer.getJobTypes().size()) {
        throw new IllegalArgumentException(
            "clusterId, name and unique nonblank jobTypes are required");
      }
      final var json = JSON.readTree(timer.getVariables());
      if (json == null || !json.isObject()) {
        throw new IllegalArgumentException("variables must be a JSON object");
      }
    } catch (final Exception error) {
      reject(record, RejectionType.INVALID_ARGUMENT, error.getMessage());
      return;
    }
    if (state.getBrokerClusterConfigState().findClusterConfig(timer.getClusterId()).isEmpty()) {
      reject(record, RejectionType.NOT_FOUND, "CLUSTER_NOT_REGISTERED: " + timer.getClusterId());
      return;
    }
    for (final String jobType : timer.getJobTypes()) {
      final var subscription =
          state
              .getCommandDistributionState()
              .distributionKeyByDedupeKey(
                  CommandDistributionDedupe.subscriptionDedupeKey(
                      timer.getClusterId(), timer.getTenantId(), jobType))
              .flatMap(state.getJobSubscriptionState()::get)
              .filter(
                  value ->
                      timer.getClusterId().equals(value.getClusterId())
                          && timer.getTenantId().equals(value.getTenantId())
                          && jobType.equals(value.getJobType()));
      if (subscription.isEmpty()) {
        reject(record, RejectionType.NOT_FOUND, "JOB_SUBSCRIPTION_NOT_FOUND: " + jobType);
        return;
      }
    }
    try {
      final var timeZone = ZoneId.systemDefault();
      timer.setExpression(timer.getExpression().trim()).setTimeZone(timeZone.getId());
      final var schedule = TimerSchedule.parse(timer.getType(), timer.getExpression());
      final var first = schedule.first(record.getTimestamp(), timeZone);
      if (first.isEmpty()) {
        throw new IllegalArgumentException("Expression has no next execution time");
      }
      timer
          .setDueDate(first.getAsLong())
          .setRepetitions(schedule.repetitions())
          .setOccurrence(0)
          .setPartitionId(0);
    } catch (final RuntimeException error) {
      reject(record, RejectionType.INVALID_ARGUMENT, error.getMessage());
      return;
    }
    final long key = keys.nextKey();
    writers.state().appendFollowUpEvent(key, TimerIntent.CREATED, timer);
    writers.response().writeEventOnCommand(key, TimerIntent.CREATED, timer, record);
    writers.postCommit().append(() -> checker.scheduleTimer(timer.getDueDate()));
  }

  private void reject(final TypedRecord<?> record, final RejectionType type, final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
