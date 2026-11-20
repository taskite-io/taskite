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
package io.taskite.placementdriver.broker.topology;

import io.taskite.placementdriver.runtime.CommittedPlacementDriverRecord;
import io.taskite.placementdriver.runtime.PlacementDriverCommittedRecordListener;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.ClusterConfigRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.intent.placementdriver.BrokerIntent;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.time.Instant;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerTopologyCommittedRecordListener
    implements PlacementDriverCommittedRecordListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(BrokerTopologyCommittedRecordListener.class);

  private final BrokerTopologyCalculator calculator;
  private final BrokerTopologyWatchPublisher publisher;

  public BrokerTopologyCommittedRecordListener(
      final BrokerTopologyCalculator calculator, final BrokerTopologyWatchPublisher publisher) {
    this.calculator = calculator;
    this.publisher = publisher;
  }

  @Override
  public void onCommittedRecord(final CommittedPlacementDriverRecord committed) {
    final LoggedEvent record = committed.record();
    if (record.metadata().getRecordType() == RecordType.EVENT
        && record.metadata().getIntent() == TopologyIntent.CLUSTER_DELETED
        && record.value() instanceof final ClusterConfigRecord value) {
      publisher.closeBrokerTopology(value.getClusterId());
      return;
    }
    topologyChangedClusterId(record)
        .ifPresent(
            clusterId -> {
              publishBrokerTopology(clusterId, Instant.ofEpochMilli(record.getTimestamp()));
            });
  }

  private void publishBrokerTopology(final String clusterId, final Instant updatedAt) {
    try {
      publisher.publishBrokerTopology(calculator.calculateBrokerTopology(clusterId, updatedAt));
    } catch (final RuntimeException error) {
      LOG.debug("broker topology is not available for {}", clusterId, error);
    }
  }

  private static Optional<String> topologyChangedClusterId(final LoggedEvent record) {
    if (record.metadata().getRecordType() != RecordType.EVENT) {
      return Optional.empty();
    }
    if (record.value() instanceof final PartitionChangeRecord change) {
      return Optional.of(change.getClusterId());
    }
    if (record.value() instanceof final MemberChangeRecord change
        && change.getScope() == MemberChangeScope.BROKER) {
      return Optional.of(change.getResourceId());
    }
    final var intent = record.metadata().getIntent();
    if (intent != TopologyIntent.CLUSTER_CONFIGURED
        && intent != TopologyIntent.CLUSTER_CREATED
        && !isBrokerLifecycleIntent(record)) {
      return Optional.empty();
    }
    if (record.value() instanceof final ClusterConfigRecord value) {
      return Optional.of(value.getClusterId());
    }
    if (record.value() instanceof final BrokerRegistrationRecord value) {
      return Optional.of(value.getClusterId());
    }
    return Optional.empty();
  }

  private static boolean isBrokerLifecycleIntent(final LoggedEvent record) {
    final var intent = record.metadata().getIntent();
    return switch (intent) {
      case BrokerIntent.REGISTERED,
              BrokerIntent.RENEWED,
              BrokerIntent.DEREGISTERED,
              BrokerIntent.MARKED_OFFLINE,
              BrokerIntent.DRAINED ->
          true;
      default -> false;
    };
  }
}
