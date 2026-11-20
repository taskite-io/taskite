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

final class PlacementDriverCommittedRecordObserverTest {

  @Test
  void broadcastsAppliedRecordsAndClosesSubscriptionsIndependently() {
    final PlacementDriverCommittedRecordObserver observer =
        new PlacementDriverCommittedRecordObserver(Runnable::run);
    final AtomicInteger first = new AtomicInteger();
    final AtomicInteger second = new AtomicInteger();
    final PlacementDriverCommittedRecordSubscription firstSubscription =
        observer.subscribeToCommittedRecords(ignored -> first.incrementAndGet());
    observer.subscribeToCommittedRecords(ignored -> second.incrementAndGet());

    observer.onProcessed(event(1));
    firstSubscription.close();
    firstSubscription.close();
    observer.onProcessed(event(2));

    assertEquals(1, first.get());
    assertEquals(2, second.get());
  }

  @Test
  void closeStopsFurtherPublication() {
    final PlacementDriverCommittedRecordObserver observer =
        new PlacementDriverCommittedRecordObserver(Runnable::run);
    final AtomicInteger received = new AtomicInteger();
    observer.subscribeToCommittedRecords(ignored -> received.incrementAndGet());

    observer.onProcessed(event(1));
    observer.close();
    observer.close();
    observer.onProcessed(event(2));
    observer.publishCommittedRecord(new CommittedPlacementDriverRecord(event(3)));

    assertEquals(1, received.get());
  }

  private static LoggedEvent event(final long key) {
    return LoggedEvent.ofRecord(
        key,
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.TOPOLOGY)
            .intent(TopologyIntent.UPDATED),
        new MemberTopologyRecord());
  }
}
