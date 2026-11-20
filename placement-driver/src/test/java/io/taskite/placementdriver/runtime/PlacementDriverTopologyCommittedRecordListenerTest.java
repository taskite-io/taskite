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

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class PlacementDriverTopologyCommittedRecordListenerTest {
  @Test
  void publishesOnlyCommittedTopologyEvent() {
    final AtomicInteger published = new AtomicInteger();
    final AtomicInteger observed = new AtomicInteger();
    final PlacementDriverClusterTopologyWatchPublisher publisher =
        new PlacementDriverClusterTopologyWatchPublisher();
    publisher.subscribeToPlacementDriverClusterTopology(ignored -> published.incrementAndGet());
    final PlacementDriverTopologyCommittedRecordListener listener =
        new PlacementDriverTopologyCommittedRecordListener(
            topology -> topology, publisher, ignored -> observed.incrementAndGet());
    final MemberTopologyRecord topology = new MemberTopologyRecord().setLeaderId("meta-0");

    listener.onCommittedRecord(
        new CommittedPlacementDriverRecord(
            LoggedEvent.ofRecord(
                1,
                new RecordMetadata()
                    .recordType(RecordType.COMMAND)
                    .valueType(ValueType.TOPOLOGY)
                    .intent(TopologyIntent.UPDATE),
                topology)));
    listener.onCommittedRecord(
        new CommittedPlacementDriverRecord(
            LoggedEvent.ofRecord(
                1,
                new RecordMetadata()
                    .recordType(RecordType.EVENT)
                    .valueType(ValueType.TOPOLOGY)
                    .intent(TopologyIntent.UPDATED),
                topology)));

    assertEquals(1, published.get());
    assertEquals(1, observed.get());
  }
}
