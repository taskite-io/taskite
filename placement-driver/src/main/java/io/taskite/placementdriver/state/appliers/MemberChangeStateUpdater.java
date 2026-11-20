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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class MemberChangeStateUpdater {
  private final MutableProcessingState state;

  MemberChangeStateUpdater(final MutableProcessingState state) {
    this.state = state;
  }

  void update(final MemberChangeRecord value) {
    final boolean cancelled = value.getStatus() == MemberChangeStatus.CANCELLED;
    final MemberChangeRecord configuration =
        cancelled && !value.getSourceVoters().isEmpty()
            ? value
                .copy()
                .setTargetCount(value.getSourceVoters().size())
                .setTargetVoters(value.getSourceVoters())
                .setRemovedMembers(List.of())
            : value;
    if (configuration.getTargetCount() > 0) {
      state.getMemberChangeState().putConfiguration(configuration);
    }
    if (value.getScope() == MemberChangeScope.BROKER && configuration.getTargetCount() > 0) {
      final var previousConfig =
          state.getBrokerClusterConfigState().findClusterConfig(value.getResourceId());
      final var registrations = state.getBrokerRegistrationState();
      final var previousRegistrations = registrations.registrations(value.getResourceId());
      final Set<String> excluded =
          Stream.concat(value.getTargetVoters().stream(), value.getRemovedMembers().stream())
              .filter(memberId -> registrations.isVoterExcluded(value.getResourceId(), memberId))
              .collect(Collectors.toSet());
      state
          .getBrokerClusterConfigState()
          .findClusterConfig(value.getResourceId())
          .ifPresent(
              current ->
                  state
                      .getBrokerClusterConfigState()
                      .putClusterConfig(
                          value.getResourceId(),
                          new BrokerClusterConfig(
                              configuration.getTargetCount(),
                              current.partitionSize(),
                              current.replicationFactor(),
                              configuration.getTargetVoters(),
                              BrokerClusterConfig.MANAGED_ROSTER_EPOCH)));
      if (cancelled) {
        value
            .getRemovedMembers()
            .forEach(
                memberId -> {
                  state
                      .getBrokerRegistrationState()
                      .includeInPlacement(value.getResourceId(), memberId);
                  state
                      .getBrokerRegistrationState()
                      .markBrokerOnline(value.getResourceId(), memberId);
                });
      } else {
        value.getTargetVoters().stream()
            .filter(memberId -> !value.getSourceVoters().contains(memberId))
            .forEach(
                memberId ->
                    state
                        .getBrokerRegistrationState()
                        .includeInPlacement(value.getResourceId(), memberId));
        value
            .getRemovedMembers()
            .forEach(
                memberId -> {
                  state
                      .getBrokerRegistrationState()
                      .excludeFromPlacement(value.getResourceId(), memberId);
                  state
                      .getBrokerRegistrationState()
                      .markBrokerDraining(value.getResourceId(), memberId, Instant.EPOCH);
                });
      }
      if (!previousConfig.equals(
              state.getBrokerClusterConfigState().findClusterConfig(value.getResourceId()))
          || !previousRegistrations.equals(registrations.registrations(value.getResourceId()))
          || Stream.concat(value.getTargetVoters().stream(), value.getRemovedMembers().stream())
              .anyMatch(
                  memberId ->
                      excluded.contains(memberId)
                          != registrations.isVoterExcluded(value.getResourceId(), memberId))) {
        state.getBrokerTopologyCoordinatorState().markCluster(value.getResourceId());
      }
    }
    state.getMemberChangeState().put(value);
    if (value.isTerminal()) {
      state
          .getMemberChangeState()
          .active(value.getScope(), value.getResourceId())
          .filter(active -> active.getChangeId().equals(value.getChangeId()))
          .ifPresent(
              ignored ->
                  state
                      .getMemberChangeState()
                      .clearActive(value.getScope(), value.getResourceId()));
      if (value.getScope() == MemberChangeScope.BROKER) {
        BrokerRemovalFinalizer.finalizeRemovals(state, value.getResourceId());
      }
    } else {
      state.getMemberChangeState().setActive(value);
    }
  }
}
