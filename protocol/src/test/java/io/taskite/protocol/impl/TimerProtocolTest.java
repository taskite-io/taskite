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
package io.taskite.protocol.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.common.TimerType;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.common.TimerIntent;
import io.taskite.util.BufferUtil;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class TimerProtocolTest {
  @Test
  void readsExplicitSubscriptionSchedule() {
    final var timer = new TimerSubscriptionRecord();
    timer.wrap(
        BufferUtil.wrapString(
            """
        {"dueDate":1000,"type":"CYCLE","expression":"R3/PT0.5S","partitionId":2,
         "jobTypes":["charge","notify"],"timeZone":"UTC","occurrence":7,
         "repetitions":3,"clusterId":"cluster","tenantId":"tenant"}
        """));
    assertEquals(1000, timer.getDueDate());
    assertEquals(TimerType.CYCLE, timer.getType());
    assertEquals("R3/PT0.5S", timer.getExpression());
    assertEquals(List.of("charge", "notify"), timer.getJobTypes());
    assertEquals(7, timer.getOccurrence());
    assertEquals(3, timer.getRepetitions());
    assertEquals("cluster", timer.getClusterId());
    assertEquals("tenant", timer.getTenantId());
    assertEquals(2, timer.getPartitionId());
  }

  @Test
  void roundTripsAssignedPartitionAndSchedule() {
    final var source =
        new TimerSubscriptionRecord()
            .setDueDate(1000)
            .setType(TimerType.CRON)
            .setExpression("0 * * * * *")
            .setJobTypes(List.of("charge", "通知"))
            .setPartitionId(12)
            .setTenantId("租户")
            .setRepetitions(1);
    final var buffer = new UnsafeBuffer(new byte[source.getLength()]);
    source.write(buffer, 0);
    final var decoded = new TimerSubscriptionRecord();
    decoded.wrap(buffer);
    assertEquals(source.toJson(), decoded.toJson());
    assertEquals(12, decoded.getPartitionId());
    assertEquals(source.getJobTypes(), decoded.getJobTypes());
  }

  @Test
  void cancellationKeepsItsWireValueWithTheCorrectedName() {
    final var metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.TIMER)
            .intent(TimerIntent.CANCELED);
    final var buffer = new UnsafeBuffer(new byte[metadata.getLength()]);
    metadata.write(buffer, 0);
    final var decoded = new RecordMetadata();
    decoded.wrap(buffer, 0, buffer.capacity());
    assertSame(TimerIntent.CANCELED, decoded.getIntent());
    assertEquals(3, TimerIntent.CANCELED.value());
    assertEquals("CANCELED", decoded.getIntent().name());
  }
}
