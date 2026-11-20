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
package io.taskite.placementdriver.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RedistributeJobSubscriptionRequest;
import io.taskite.placementdriver.processing.PlacementDriverProcessors;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionListRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionQueryRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRedistributeRecord;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionCreationType;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class JobSubscriptionDistributionMapperTest {
  private final PlacementDriverRequestMapper mapper =
      new PlacementDriverRequestMapper(
          PlacementDriverProcessors.createEventRegistry(),
          Clock.fixed(Instant.ofEpochMilli(1793400000000L), ZoneOffset.UTC));

  @Test
  void mapsExactIdentifiersAndDeterministicRedistributeTimestamp() {
    final var request =
        mapper.toRedistributeJobSubscriptionRequest(
            "9007199254741052", new RedistributeJobSubscriptionRequest("9007199254741053"));
    final var value = (CommandDistributionRedistributeRecord) request.value();
    assertEquals(CommandDistributionIntent.REDISTRIBUTE, request.intent());
    assertEquals(9007199254741052L, value.getDistributionKey());
    assertEquals(9007199254741053L, value.getExpectedGeneration());
    assertEquals(1793400000000L, value.getRequestedAtEpochMillis());
    for (final String invalid : List.of("-1", "", "1.5", "9223372036854775808")) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              mapper.toRedistributeJobSubscriptionRequest(
                  "1", new RedistributeJobSubscriptionRequest(invalid)));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> mapper.toRedistributeJobSubscriptionRequest("1", null));
    final var list =
        (CommandDistributionListRecord)
            mapper
                .toJobSubscriptionDistributionListRequest(" order ", "9007199254741052", " demo ")
                .value();
    assertEquals("order", list.getJobType());
    assertEquals("demo", list.getClusterId());
    assertEquals(9007199254741052L, list.getDistributionKey());
    assertEquals(
        0,
        ((CommandDistributionListRecord)
                mapper.toJobSubscriptionDistributionListRequest(null, null, null).value())
            .getDistributionKey());
    assertThrows(
        IllegalArgumentException.class,
        () -> mapper.toJobSubscriptionDistributionListRequest(null, "0", null));
    assertThrows(
        IllegalArgumentException.class,
        () -> mapper.toJobSubscriptionDistributionListRequest(null, "9223372036854775808", null));
  }

  @Test
  void listResponseOmitsResourceContentAndDetailHasExplicitTargetPartitions() throws Exception {
    final var resource =
        new CommandDistributionQueryRecord()
            .setDistributionKey(9007199254741052L)
            .setOriginClusterId("demo")
            .setClusterId("backup")
            .setCreationType(JobSubscriptionCreationType.MIRROR)
            .setStatus(JobSubscriptionDistributionStatus.DISTRIBUTING)
            .setName("order")
            .setJobType("order")
            .setTargets(
                List.of(
                    new CommandDistributionQueryRecord.Target()
                        .setClusterId("backup")
                        .setStatus(JobSubscriptionDistributionStatus.DISTRIBUTING)
                        .setExpectedPartitions(2)
                        .setAcknowledgedPartitions(1)
                        .setPendingPartitions(List.of(2))
                        .setConfirmedPartitionIds(List.of(1))
                        .setMirrorKey(42)));
    final var list =
        new CommandDistributionListRecord().setTotal(1).setSubscriptions(List.of(resource));
    final var response = new PlacementDriverResponseMapper();
    final var json =
        new ObjectMapper()
            .valueToTree(
                response
                    .toJobSubscriptionDistributionListResult(
                        new PlacementDriverApiResponse<>(list, 1))
                    .body());
    assertEquals(false, json.path("subscriptions").get(0).has("resourceContent"));
    assertEquals(false, json.has("page"));
    assertEquals(false, json.has("pageSize"));
    assertEquals(false, json.path("subscriptions").get(0).has("originClusterId"));
    assertEquals("demo", json.path("subscriptions").get(0).path("sourceClusterId").asText());
    assertEquals("backup", json.path("subscriptions").get(0).path("clusterId").asText());
    assertEquals("MIRROR", json.path("subscriptions").get(0).path("creationType").asText());
    assertEquals("DISTRIBUTING", json.path("subscriptions").get(0).path("status").asText());
    assertEquals(
        "DISTRIBUTING",
        json.path("subscriptions").get(0).path("targets").get(0).path("status").asText());
    assertEquals(
        42, json.path("subscriptions").get(0).path("targets").get(0).path("mirrorKey").asLong());
    final var record =
        new CommandDistributionRecord<>()
            .setDistributionKey(9007199254741052L)
            .setGeneration(1)
            .setLastRedistributedAt(1793400000000L);
    assertEquals(
        202,
        response
            .toRedistributeJobSubscriptionResult(new PlacementDriverApiResponse<>(record, 1))
            .status());
    assertEquals(
        "DISTRIBUTING",
        new ObjectMapper()
            .valueToTree(
                response
                    .toRedistributeJobSubscriptionResult(
                        new PlacementDriverApiResponse<>(record, 1))
                    .body())
            .path("status")
            .asText());
  }
}
