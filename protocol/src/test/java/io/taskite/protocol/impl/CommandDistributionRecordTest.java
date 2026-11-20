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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.intent.placementdriver.JobSubscriptionIntent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class CommandDistributionRecordTest {
  @Test
  void commandDistributionIntentsPreservePairsAndDecodeDispatchCommand() {
    final var intents =
        List.of(
            CommandDistributionIntent.START,
            CommandDistributionIntent.STARTED,
            CommandDistributionIntent.DISPATCH,
            CommandDistributionIntent.DISPATCHED,
            CommandDistributionIntent.RETRY,
            CommandDistributionIntent.RETRIED,
            CommandDistributionIntent.ACKNOWLEDGE,
            CommandDistributionIntent.ACKNOWLEDGED,
            CommandDistributionIntent.COMPLETE,
            CommandDistributionIntent.COMPLETED,
            CommandDistributionIntent.REDISTRIBUTE,
            CommandDistributionIntent.REDISTRIBUTED,
            CommandDistributionIntent.QUERY,
            CommandDistributionIntent.QUERIED,
            CommandDistributionIntent.LIST,
            CommandDistributionIntent.LISTED);
    for (short value = 0; value < intents.size(); value++) {
      final var intent = intents.get(value);
      assertEquals(value, intent.value());
      assertEquals(intent, CommandDistributionIntent.from(value));
      assertEquals(intent, Intent.fromProtocolValue(ValueType.COMMAND_DISTRIBUTION, value));
      assertEquals(value % 2 == 1, intent.isEvent());
    }
    assertNull(CommandDistributionIntent.from((short) -1));
    assertNull(CommandDistributionIntent.from((short) 16));
  }

  @Test
  void resourceEnumsPreserveJsonNamesAndDecodeHistoricalQueryValues() throws Exception {
    for (final var status : JobSubscriptionDistributionStatus.values()) {
      for (final var creationType : JobSubscriptionCreationType.values()) {
        final var query =
            new CommandDistributionQueryRecord()
                .setStatus(status)
                .setCreationType(creationType)
                .setTargets(List.of(new CommandDistributionQueryRecord.Target().setStatus(status)));
        final String json = query.toJson();
        final var tree = new ObjectMapper().readTree(json);
        assertEquals(status.name(), tree.path("status").asText());
        assertEquals(creationType.name(), tree.path("creationType").asText());
        final var decoded = new CommandDistributionQueryRecord();
        decoded.wrap(new UnsafeBuffer(json.getBytes(StandardCharsets.UTF_8)));
        assertEquals(status, decoded.getStatus());
        assertEquals(creationType, decoded.getCreationType());
        assertEquals(status, decoded.getTargets().getFirst().getStatus());
      }
    }
    final var historical = new CommandDistributionQueryRecord();
    historical.wrap(
        new UnsafeBuffer(
            """
            {"status":"","creationType":"","targets":[{"clusterId":"demo","status":""}]}
            """
                .getBytes(StandardCharsets.UTF_8)));
    assertNull(historical.getStatus());
    assertNull(historical.getCreationType());
    assertNull(historical.getTargets().getFirst().getStatus());
    final var missing = new CommandDistributionQueryRecord();
    missing.wrap(new UnsafeBuffer("{}".getBytes(StandardCharsets.UTF_8)));
    assertNull(missing.getStatus());
    assertNull(missing.getCreationType());
    for (final String field : List.of("status", "creationType")) {
      assertThrows(
          RuntimeException.class,
          () ->
              new CommandDistributionQueryRecord()
                  .wrap(
                      new UnsafeBuffer(
                          ("{\"" + field + "\":\"INVALID\"}").getBytes(StandardCharsets.UTF_8))));
    }
  }

  @Test
  void roundTripsSubscriptionListAndFilters() {
    final var legacy =
        new UnsafeBuffer(
            """
            {"jobType":"work","distributionKey":101,"clusterId":"demo","total":1,
             "subscriptions":[{"distributionKey":101,"originClusterId":"demo","targets":[]}]}
            """
                .getBytes(StandardCharsets.UTF_8));
    final var decoded = new CommandDistributionListRecord();
    decoded.wrap(legacy);
    assertEquals("work", decoded.getJobType());
    assertEquals(101, decoded.getDistributionKey());
    assertEquals("demo", decoded.getSubscriptions().getFirst().getOriginClusterId());
    final String json = decoded.toJson();
    assertFalse(json.contains("\"bpmnId\""));
    assertFalse(json.contains("\"targetClusterId\""));
    assertFalse(json.contains("\"page\""));
    assertFalse(json.contains("\"pageSize\""));
    final var restored = new CommandDistributionListRecord();
    restored.wrap(new UnsafeBuffer(json.getBytes(StandardCharsets.UTF_8)));
    assertEquals("work", restored.getJobType());
  }

  @Test
  void roundTripsSchedulingIdentityAndResetsFieldsWhenReusingDecoder() {
    final var record =
        new CommandDistributionRecord<JobSubscriptionRecord>()
            .setDistributionKey(9007199254741052L)
            .setClusterId("demo")
            .setValueType(ValueType.JOB_SUBSCRIPTION)
            .setIntent(JobSubscriptionIntent.CREATE)
            .setValue(new JobSubscriptionRecord().setJobType("work").setVariables("{\"v\":1}"))
            .setGeneration(9007199254741053L)
            .setLastRedistributedAt(1793400000000L)
            .setDispatchAttemptId(9007199254741054L)
            .setNextAttemptAtEpochMillis(1793400015000L);
    final var buffer = new UnsafeBuffer(new byte[record.getLength() + 7]);
    record.write(buffer, 7);
    final var decoded = new CommandDistributionRecord<JobSubscriptionRecord>();
    decoded.wrap(buffer, 7, record.getLength());
    assertEquals(9007199254741053L, decoded.getGeneration());
    assertEquals(1793400000000L, decoded.getLastRedistributedAt());
    assertEquals("{\"v\":1}", decoded.getValue().getVariables());
    assertEquals(9007199254741054L, decoded.getDispatchAttemptId());
    assertEquals(1793400015000L, decoded.getNextAttemptAtEpochMillis());
    record
        .setGeneration(0)
        .setLastRedistributedAt(0)
        .setDispatchAttemptId(0)
        .setNextAttemptAtEpochMillis(0);
    record.write(buffer, 7);
    decoded.wrap(buffer, 7, record.getLength());
    assertEquals(0, decoded.getGeneration());
    assertEquals(0, decoded.getLastRedistributedAt());
    assertEquals(0, decoded.getDispatchAttemptId());
    assertEquals(0, decoded.getNextAttemptAtEpochMillis());
    assertEquals(9007199254741052L, decoded.getDistributionKey());
    assertEquals("{\"v\":1}", decoded.getValue().getVariables());
    assertThrows(
        IllegalArgumentException.class,
        () -> decoded.wrap(buffer, 7, record.getLength() - Long.BYTES));
  }
}
