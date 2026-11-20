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
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class BrokerRenewProcessor implements TypedRecordProcessor<BrokerRegistrationRecord> {
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to renew broker '%s', but cluster '%s' is not registered";
  private static final String BROKER_NOT_FOUND =
      "NO_BROKER_REGISTERED: Expected to renew broker '%s' in cluster '%s', but no registration was found";
  private static final String BROKER_OFFLINE =
      "NO_ONLINE_BROKER: Expected to renew broker '%s' in cluster '%s', but its registration is OFFLINE";
  private static final String STALE_INCARNATION =
      "STALE_BROKER_INCARNATION: Expected to renew broker '%s' in cluster '%s' with incarnation '%s', but the registered incarnation is '%s'";
  private final Writers writers;
  private final ProcessingState state;

  public BrokerRenewProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<BrokerRegistrationRecord> record) {
    final BrokerRegistrationRecord registration = record.getValue();
    if (state
        .getBrokerClusterConfigState()
        .findClusterConfig(registration.getClusterId())
        .isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          CLUSTER_NOT_FOUND.formatted(registration.getBrokerId(), registration.getClusterId()));
      return;
    }
    final var existing =
        state
            .getBrokerRegistrationState()
            .registration(
                new BrokerRegistryKey(registration.getClusterId(), registration.getBrokerId()));
    if (existing.isEmpty()) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          BROKER_NOT_FOUND.formatted(registration.getBrokerId(), registration.getClusterId()));
      return;
    }
    if (existing.get().status() == BrokerStatus.OFFLINE) {
      reject(
          record,
          RejectionType.NOT_FOUND,
          BROKER_OFFLINE.formatted(registration.getBrokerId(), registration.getClusterId()));
      return;
    }
    if (!registration.getIncarnationId().equals(existing.get().incarnationId())) {
      reject(
          record,
          RejectionType.INVALID_STATE,
          STALE_INCARNATION.formatted(
              registration.getBrokerId(),
              registration.getClusterId(),
              registration.getIncarnationId(),
              existing.get().incarnationId()));
      return;
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerIntent.RENEWED,
            registration.setEventTimestampEpochMillis(record.getTimestamp()));
    writers
        .response()
        .writeEventOnCommand(record.getKey(), BrokerIntent.RENEWED, registration, record);
  }

  private void reject(
      final TypedRecord<BrokerRegistrationRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
