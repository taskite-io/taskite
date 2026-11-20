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

import io.taskite.placementdriver.broker.cluster.TopologyOperationType;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;

public final class BrokerRegisterProcessor
    implements TypedRecordProcessor<BrokerRegistrationRecord> {
  private static final String INVALID_PORTS =
      "Expected to register broker '%s' in cluster '%s' with distinct business and Raft ports between 1 and 65535, but received business port '%d' and Raft port '%d'";
  private static final String CLUSTER_NOT_FOUND =
      "CLUSTER_NOT_REGISTERED: Expected to register broker '%s', but cluster '%s' is not registered";
  private static final String BROKER_ALREADY_REGISTERED =
      "BROKER_ID_ALREADY_REGISTERED: Expected to register broker '%s' in cluster '%s' with incarnation '%s', but a different startup is already registered (status '%s', incarnation '%s')";
  private static final String REMOVAL_FENCED =
      "BROKER_REMOVAL_FENCED: Expected to register broker '%s' in cluster '%s' with incarnation '%s', but this incarnation is excluded from learner membership after removal";
  private static final String REMOVAL_IN_PROGRESS =
      "BROKER_REMOVAL_IN_PROGRESS: Expected to register broker '%s' in cluster '%s', but a pending topology or member change is removing it";
  private final Writers writers;
  private final ProcessingState state;

  public BrokerRegisterProcessor(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  @Override
  public void processRecord(final TypedRecord<BrokerRegistrationRecord> record) {
    final BrokerRegistrationRecord registration = record.getValue();
    if (registration.getPort() <= 0
        || registration.getPort() > 65_535
        || registration.getRaftPort() <= 0
        || registration.getRaftPort() > 65_535
        || registration.getPort() == registration.getRaftPort()) {
      reject(
          record,
          RejectionType.INVALID_ARGUMENT,
          INVALID_PORTS.formatted(
              registration.getBrokerId(),
              registration.getClusterId(),
              registration.getPort(),
              registration.getRaftPort()));
      return;
    }
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
    final boolean sameStartup =
        existing.isPresent()
            && !registration.getIncarnationId().isBlank()
            && registration.getIncarnationId().equals(existing.get().incarnationId())
            && registration.getHost().equals(existing.get().host())
            && registration.getPort() == existing.get().port()
            && registration.getRaftPort() == existing.get().raftPort();
    if (existing.isPresent() && existing.get().status() != BrokerStatus.OFFLINE && !sameStartup) {
      reject(
          record,
          RejectionType.ALREADY_EXISTS,
          BROKER_ALREADY_REGISTERED.formatted(
              registration.getBrokerId(),
              registration.getClusterId(),
              registration.getIncarnationId(),
              existing.get().status(),
              existing.get().incarnationId()));
      return;
    }
    final var registrations = state.getBrokerRegistrationState();
    if (registrations.isVoterExcluded(registration.getClusterId(), registration.getBrokerId())) {
      final boolean removalInProgress =
          state
                  .getMemberChangeState()
                  .active(MemberChangeScope.BROKER, registration.getClusterId())
                  .filter(change -> change.getRemovedMembers().contains(registration.getBrokerId()))
                  .isPresent()
              || state
                  .getBrokerTopologyChangeState()
                  .activeClusterChange(registration.getClusterId())
                  .filter(change -> change.removedBrokers().contains(registration.getBrokerId()))
                  .isPresent()
              || state
                  .getBrokerTopologyChangeState()
                  .pendingChangePlans(registration.getClusterId())
                  .stream()
                  .anyMatch(
                      plan ->
                          plan.operations().stream()
                              .skip(plan.nextOperationIndex())
                              .anyMatch(
                                  operation ->
                                      operation.type() == TopologyOperationType.LEAVE
                                          && operation
                                              .targetBroker()
                                              .equals(registration.getBrokerId())));
      if (registrations.isLearnerExcluded(
          registration.getClusterId(),
          registration.getBrokerId(),
          registration.getIncarnationId())) {
        reject(
            record,
            RejectionType.INVALID_STATE,
            REMOVAL_FENCED.formatted(
                registration.getBrokerId(),
                registration.getClusterId(),
                registration.getIncarnationId()));
        return;
      }
      if (removalInProgress) {
        reject(
            record,
            RejectionType.INVALID_STATE,
            REMOVAL_IN_PROGRESS.formatted(registration.getBrokerId(), registration.getClusterId()));
        return;
      }
    }
    writers
        .state()
        .appendFollowUpEvent(
            record.getKey(),
            BrokerIntent.REGISTERED,
            registration.setEventTimestampEpochMillis(record.getTimestamp()));
    writers
        .response()
        .writeEventOnCommand(record.getKey(), BrokerIntent.REGISTERED, registration, record);
  }

  private void reject(
      final TypedRecord<BrokerRegistrationRecord> record,
      final RejectionType type,
      final String reason) {
    writers.rejection().appendRejection(type, reason);
    writers.response().writeRejectionOnCommand(record, type, reason);
  }
}
