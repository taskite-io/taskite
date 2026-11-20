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
package io.taskite.placementdriver.runtime;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public final class PlacementDriverTopologyCommittedRecordListener
    implements PlacementDriverCommittedRecordListener {
  private final UnaryOperator<MemberTopologyRecord> localView;
  private final PlacementDriverClusterTopologyWatchPublisher publisher;
  private final Consumer<MemberTopologyRecord> topologyObserver;

  public PlacementDriverTopologyCommittedRecordListener(
      final UnaryOperator<MemberTopologyRecord> localView,
      final PlacementDriverClusterTopologyWatchPublisher publisher) {
    this(localView, publisher, ignored -> {});
  }

  public PlacementDriverTopologyCommittedRecordListener(
      final UnaryOperator<MemberTopologyRecord> localView,
      final PlacementDriverClusterTopologyWatchPublisher publisher,
      final Consumer<MemberTopologyRecord> topologyObserver) {
    this.localView = localView;
    this.publisher = publisher;
    this.topologyObserver = topologyObserver;
  }

  @Override
  public void onCommittedRecord(final CommittedPlacementDriverRecord committed) {
    final var record = committed.record();
    if (record.metadata().getRecordType() == RecordType.EVENT
        && record.metadata().getIntent() == TopologyIntent.UPDATED
        && record.value() instanceof MemberTopologyRecord topology) {
      topologyObserver.accept(topology);
      publisher.publishPlacementDriverClusterTopology(localView.apply(topology));
    }
  }
}
