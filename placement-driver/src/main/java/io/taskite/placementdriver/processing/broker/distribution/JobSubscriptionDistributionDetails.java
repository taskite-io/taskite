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
package io.taskite.placementdriver.processing.broker.distribution;

import io.taskite.placementdriver.broker.distribution.CommandDistributionDedupe;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord.Target;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import java.util.Optional;
import java.util.stream.IntStream;

final class JobSubscriptionDistributionDetails {
  private JobSubscriptionDistributionDetails() {}

  static Optional<Long> deletionKey(
      final ProcessingState state, final CommandDistributionRecord<?> record) {
    if (!(record.getValue() instanceof JobSubscriptionRecord resource)) {
      return Optional.empty();
    }
    return state
        .getCommandDistributionState()
        .distributionKeyByDedupeKey(
            CommandDistributionDedupe.deletionDedupeKey(
                new JobSubscriptionDeletionRecord()
                    .setClusterId(resource.getClusterId())
                    .setTenantId(resource.getTenantId())
                    .setSubscriptionKey(record.getDistributionKey())));
  }

  static CommandDistributionQueryRecord view(
      final ProcessingState state,
      final CommandDistributionRecord<?> record,
      final boolean includeContent) {
    final var distributions = state.getCommandDistributionState();
    final long key = record.getDistributionKey();
    final var deletion = deletionKey(state, record);
    final JobSubscriptionDistributionStatus status =
        deletion.isPresent()
            ? (distributions.isPlanComplete(deletion.get())
                ? JobSubscriptionDistributionStatus.DELETED
                : JobSubscriptionDistributionStatus.DELETING)
            : (distributions.isPlanComplete(key)
                ? JobSubscriptionDistributionStatus.COMPLETED
                : JobSubscriptionDistributionStatus.DISTRIBUTING);
    final var result =
        new CommandDistributionQueryRecord()
            .setDistributionKey(key)
            .setDeletionDistributionKey(deletion.orElse(0L))
            .setOriginClusterId(record.getClusterId())
            .setClusterId(record.getClusterId())
            .setCreationType(JobSubscriptionCreationType.SUBSCRIPTION)
            .setValueType(record.getValueType().name())
            .setStatus(status)
            .setGeneration(record.getGeneration())
            .setLastRedistributedAt(record.getLastRedistributedAt())
            .setTargets(
                distributions.targets(key).stream()
                    .map(
                        target -> {
                          final var pending =
                              distributions.pendingPartitions(key, target.getClusterId());
                          return new Target()
                              .setClusterId(target.getClusterId())
                              .setExpectedPartitions(target.getExpectedPartitionCount())
                              .setAcknowledgedPartitions(target.getAcknowledgedPartitionCount())
                              .setPendingPartitions(pending)
                              .setConfirmedPartitionIds(
                                  IntStream.rangeClosed(1, target.getExpectedPartitionCount())
                                      .filter(partition -> !pending.contains(partition))
                                      .boxed()
                                      .toList())
                              .setMirrorKey(
                                  state
                                      .getJobSubscriptionMirrorState()
                                      .mirror(record.getClusterId(), target.getClusterId())
                                      .map(mirror -> mirror.getMirrorKey())
                                      .orElse(0L))
                              .setStatus(
                                  target.isCompleted()
                                      ? JobSubscriptionDistributionStatus.COMPLETED
                                      : JobSubscriptionDistributionStatus.DISTRIBUTING);
                        })
                    .toList());
    final JobSubscriptionRecord resource;
    if (record.getValue() instanceof JobSubscriptionDeletionRecord removal) {
      result.setSubscriptionKey(removal.getSubscriptionKey());
      resource =
          distributions
              .distribution(removal.getSubscriptionKey())
              .map(CommandDistributionRecord::getValue)
              .filter(JobSubscriptionRecord.class::isInstance)
              .map(JobSubscriptionRecord.class::cast)
              .orElse(null);
      result.setTenantId(removal.getTenantId());
    } else {
      resource =
          record.getValue() instanceof JobSubscriptionRecord subscription ? subscription : null;
    }
    if (resource != null) {
      result
          .setName(resource.getName())
          .setJobType(resource.getJobType())
          .setTenantId(resource.getTenantId())
          .setSubscription(includeContent ? resource : null);
    }
    return result;
  }

  static CommandDistributionQueryRecord forCluster(
      final CommandDistributionQueryRecord resource, final String clusterId) {
    final var targets =
        resource.getTargets().stream()
            .filter(target -> target.getClusterId().equals(clusterId))
            .toList();
    final JobSubscriptionDistributionStatus status =
        resource.getStatus() == JobSubscriptionDistributionStatus.DELETING
                || resource.getStatus() == JobSubscriptionDistributionStatus.DELETED
            ? resource.getStatus()
            : resource.getStatus() == JobSubscriptionDistributionStatus.COMPLETED
                    && targets.stream()
                        .allMatch(
                            target ->
                                target.getStatus() == JobSubscriptionDistributionStatus.COMPLETED)
                    && !targets.isEmpty()
                ? JobSubscriptionDistributionStatus.COMPLETED
                : JobSubscriptionDistributionStatus.DISTRIBUTING;
    return new CommandDistributionQueryRecord()
        .setDistributionKey(resource.getDistributionKey())
        .setSubscriptionKey(resource.getSubscriptionKey())
        .setDeletionDistributionKey(resource.getDeletionDistributionKey())
        .setOriginClusterId(resource.getOriginClusterId())
        .setClusterId(clusterId)
        .setCreationType(
            clusterId.equals(resource.getOriginClusterId())
                ? JobSubscriptionCreationType.SUBSCRIPTION
                : JobSubscriptionCreationType.MIRROR)
        .setValueType(resource.getValueType())
        .setStatus(status)
        .setTargets(targets)
        .setName(resource.getName())
        .setJobType(resource.getJobType())
        .setTenantId(resource.getTenantId())
        .setSubscription(resource.getSubscription())
        .setGeneration(resource.getGeneration())
        .setLastRedistributedAt(resource.getLastRedistributedAt());
  }
}
