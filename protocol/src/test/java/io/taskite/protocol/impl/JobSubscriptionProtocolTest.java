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

import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.impl.record.value.common.TimerSubscriptionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.util.BufferUtil;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class JobSubscriptionProtocolTest {
  @Test
  void roundTripsTypedSubscriptionScheduleAndTaskParameters() {
    final var value =
        new JobSubscriptionRecord()
            .setClusterId("cluster")
            .setTenantId("租户")
            .setName("nightly-charge")
            .setJobType("charge")
            .setVariables("{\"amount\":42}")
            .setPriority(90)
            .setRetries(7);
    final var source =
        new CommandDistributionRecord<JobSubscriptionRecord>()
            .setDistributionKey(42)
            .setClusterId("cluster")
            .setPartitionId(3)
            .setValueType(ValueType.JOB_SUBSCRIPTION)
            .setIntent(JobSubscriptionIntent.CREATE)
            .setValue(value);
    final var buffer = new UnsafeBuffer(new byte[source.getLength() + 8]);
    source.write(buffer, 8);
    final var restored = new CommandDistributionRecord<JobSubscriptionRecord>();
    restored.wrap(buffer, 8, source.getLength());
    assertEquals(value.toJson(), restored.getValue().toJson());
    assertEquals(3, restored.getPartitionId());
    assertEquals(7, restored.getValue().getRetries());
    assertEquals(90, restored.getValue().getPriority());
  }

  @Test
  void ordinarySubscriptionHasNoTimerAndUsesJobDefaults() {
    final var value = new JobSubscriptionRecord();
    assertEquals("{}", value.getVariables());
    assertEquals(3, value.getRetries());
    assertEquals(50, value.getPriority());
    org.junit.jupiter.api.Assertions.assertFalse(value.toJson().contains("dueDate"));
    org.junit.jupiter.api.Assertions.assertFalse(value.toJson().contains("intervalMillis"));
  }

  @Test
  void recoveredValuesDiscardRemovedBusinessRequestId() {
    for (final UnifiedRecordValue value :
        List.of(
            new JobRecord().setJobType("charge"),
            new JobSubscriptionRecord().setJobType("charge"),
            new TimerSubscriptionRecord().setJobTypes(List.of("charge")),
            new CommandDistributionQueryRecord().setJobType("charge"))) {
      final String current = value.toJson();
      value.wrap(BufferUtil.wrapString("{\"requestId\":\"retired\"," + current.substring(1)));
      assertEquals(current, value.toJson());
    }
  }
}
