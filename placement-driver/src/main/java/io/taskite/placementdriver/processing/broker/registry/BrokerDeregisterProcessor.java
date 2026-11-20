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
package io.taskite.placementdriver.processing.broker.registry;

import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class BrokerDeregisterProcessor
    implements TypedRecordProcessor<BrokerRegistrationRecord> {
  private static final String STALE_INCARNATION =
      "STALE_BROKER_INCARNATION: Expected to deregister broker '%s' in cluster '%s' with incarnation '%s', but the registered incarnation is '%s'";
  private final Writers writers;
  private final ProcessingState state;

  public BrokerDeregisterProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<BrokerRegistrationRecord> record) {
    final var registration = record.getValue();
    final var existing =
        state
            .getBrokerRegistrationState()
            .registration(
                new BrokerRegistryKey(registration.getClusterId(), registration.getBrokerId()));
    if (existing.isPresent()
        && !registration.getIncarnationId().equals(existing.get().incarnationId())) {
      final String reason =
          STALE_INCARNATION.formatted(
              registration.getBrokerId(),
              registration.getClusterId(),
              registration.getIncarnationId(),
              existing.get().incarnationId());
      writers.rejection().appendRejection(RejectionType.INVALID_STATE, reason);
      writers.response().writeRejectionOnCommand(record, RejectionType.INVALID_STATE, reason);
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerIntent.DEREGISTERED,
            record.getValue().setEventTimestampEpochMillis(record.getTimestamp()));
    writers
        .response()
        .writeEventOnCommand(record.getKey(), BrokerIntent.DEREGISTERED, record.getValue(), record);
  }
}
