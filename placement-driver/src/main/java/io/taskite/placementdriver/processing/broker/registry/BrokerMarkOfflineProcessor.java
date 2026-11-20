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

import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.registry.RegistrationDeadlineDates;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.time.Duration;
import java.util.Objects;

public final class BrokerMarkOfflineProcessor
    implements TypedRecordProcessor<BrokerRegistrationRecord> {
  private static final String INVALID_TIMEOUT =
      "Expected broker lease timeout to be positive, but was '%s'";
  private static final String MISSING_TIMEOUT =
      "Expected to construct the broker offline processor, but leaseTimeout is null";
  private static final String SESSION_CHANGED =
      "Expected to mark broker '%s' offline in cluster '%s' with last renewal '%d', but its registration is missing, renewed, or no longer online (current status '%s', last renewal '%s')";
  private static final String LEASE_NOT_EXPIRED =
      "Expected to mark broker '%s' offline in cluster '%s', but its lease has not expired (deadline '%d', timestamp '%d')";
  private final Writers writers;
  private final ProcessingState state;
  private final Duration leaseTimeout;

  public BrokerMarkOfflineProcessor(
      final TypedRecordProcessorContext context, final Duration leaseTimeout) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    Objects.requireNonNull(leaseTimeout, MISSING_TIMEOUT);
    if (leaseTimeout.isZero() || leaseTimeout.isNegative()) {
      throw new IllegalArgumentException(INVALID_TIMEOUT.formatted(leaseTimeout));
    }
    this.leaseTimeout = leaseTimeout;
  }

  @Override
  public void processRecord(final TypedRecord<BrokerRegistrationRecord> record) {
    final BrokerRegistrationRecord requested = record.getValue();
    final long expectedLastRenewal = requested.getExpectedLastRenewalEpochMillis();
    if (expectedLastRenewal > 0) {
      final var current =
          state.getBrokerRegistrationState().registrations(requested.getClusterId()).stream()
              .filter(registration -> registration.brokerId().equals(requested.getBrokerId()))
              .findFirst()
              .orElse(null);
      if (current == null
          || current.status() != BrokerStatus.ONLINE
          || current.lastRenewal().toEpochMilli() != expectedLastRenewal) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                SESSION_CHANGED.formatted(
                    requested.getBrokerId(),
                    requested.getClusterId(),
                    expectedLastRenewal,
                    current == null ? null : current.status(),
                    current == null ? null : current.lastRenewal()));
        return;
      }
      if (record.getTimestamp() < RegistrationDeadlineDates.brokerLease(current, leaseTimeout)) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                LEASE_NOT_EXPIRED.formatted(
                    requested.getBrokerId(),
                    requested.getClusterId(),
                    RegistrationDeadlineDates.brokerLease(current, leaseTimeout),
                    record.getTimestamp()));
        return;
      }
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerIntent.MARKED_OFFLINE,
            requested.setEventTimestampEpochMillis(record.getTimestamp()));
  }
}
