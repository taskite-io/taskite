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

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

/** Cancels only an active subscription belonging to the requesting cluster and tenant. */
public final class TimerCancelProcessor implements TypedRecordProcessor<TimerSubscriptionRecord> {
  private final ProcessingState state;
  private final Writers writers;

  public TimerCancelProcessor(final TypedRecordProcessorContext context) {
    state = context.getProcessingState(ProcessingState.class);
    writers = context.getWriters();
  }

  @Override
  public void processRecord(final TypedRecord<TimerSubscriptionRecord> record) {
    final var timer = state.getTimerSubscriptionState().get(record.getKey()).orElse(null);
    final var request = record.getValue();
    if (timer == null
        || !timer.getClusterId().equals(request.getClusterId())
        || !timer.getTenantId().equals(request.getTenantId())) {
      final String reason = "Timer subscription does not exist in this cluster and tenant";
      writers.rejection().appendRejection(RejectionType.NOT_FOUND, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.NOT_FOUND, reason);
      return;
    }
    writers.state().appendFollowUpEvent(record.getKey(), TimerIntent.CANCELED, timer);
    writers.response().writeEventOnCommand(record.getKey(), TimerIntent.CANCELED, timer, record);
  }
}
