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
package io.taskite.placementdriver;

import static io.taskite.protocol.record.BrokerPartitionChangeStatus.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeStatus;
import io.taskite.placementdriver.broker.cluster.BrokerPartitionChangeStatus;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.broker.topology.PartitionTopology;
import io.taskite.placementdriver.common.codec.ChangePlanRecordCodec;
import io.taskite.placementdriver.common.codec.ClusterChangeOperationCodec;
import io.taskite.placementdriver.common.codec.PartitionTopologyRecordCodec;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.protocol.record.ChangePlanRecordDecoder;
import io.taskite.protocol.record.MessageHeaderDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.topology.PartitionStatus;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class BrokerPartitionChangeCompatibilityTest {

  @Test
  void version13RecordsDefaultToMaturePartitionAndMigrationPlan() {
    final var legacyPlan =
        version13WithoutLastRootByte(
            ChangePlanRecordCodec.toBytes(
                new ChangePlan(
                    new ChangePlan.Definition(
                        new ChangePlan.Identity("demo", "initial", 1),
                        new ChangePlan.Versions(1, 2),
                        List.of(),
                        ChangePlan.Purpose.BOOTSTRAP),
                    new ChangePlan.Progress(0, BrokerPartitionChangeStatus.QUEUED),
                    new ChangePlan.Deadline(10, 100))));
    final var restored = ChangePlanRecordCodec.fromBytes(legacyPlan);
    assertEquals(ChangePlan.Purpose.MIGRATION, restored.purpose());
    assertEquals("initial", restored.changeId());
    assertEquals(100, restored.deadlineEpochMillis());
    final var topology =
        new PartitionTopology(
            new PartitionTopology.Identity("demo", 1),
            new PartitionTopology.Placement(3, List.of("broker-2"), "broker-2"),
            PartitionTopology.Membership.initial(List.of("broker-2"), List.of(), "broker-2"),
            PartitionStatus.ACTIVE,
            Map.of(),
            true);
    assertTrue(
        PartitionTopologyRecordCodec.toTopology(PartitionTopologyRecordCodec.toBytes(topology))
            .initializing());
    final var legacyTopology =
        PartitionTopologyRecordCodec.toTopology(
            version13WithoutLastRootByte(PartitionTopologyRecordCodec.toBytes(topology)));
    assertFalse(legacyTopology.initializing());
    assertEquals(topology.withInitializing(false), legacyTopology);
  }

  private static byte[] version13WithoutLastRootByte(final byte[] current) {
    final var header = new MessageHeaderDecoder().wrap(new UnsafeBuffer(current), 0);
    final int oldBlockLength = header.blockLength() - 1;
    final int fieldOffset = MessageHeaderEncoder.ENCODED_LENGTH + oldBlockLength;
    final var legacy = new byte[current.length - 1];
    System.arraycopy(current, 0, legacy, 0, fieldOffset);
    System.arraycopy(
        current, fieldOffset + 1, legacy, fieldOffset, current.length - fieldOffset - 1);
    new MessageHeaderEncoder()
        .wrap(new UnsafeBuffer(legacy), 0)
        .blockLength(oldBlockLength)
        .version(13);
    return legacy;
  }

  private static final byte[] LEGACY_CLUSTER_CHANGE_MANIFEST =
      """
      {
        "clusterId": "demo",
        "changeId": "change-1",
        "sourceTopologyVersion": 11,
        "targetClusterSize": 1,
        "targetPartitionSize": 1,
        "targetReplicationFactor": 1,
        "targetVoterRoster": ["broker-0"],
        "removedBrokers": [],
        "selectedBrokerIds": ["broker-0"],
        "expectedTopology": [],
        "planChangeIds": ["change-1:1"],
        "phase": "WAITING_READY",
        "startedAtEpochMillis": 100,
        "deadlineEpochMillis": 200
      }
      """
          .getBytes(StandardCharsets.UTF_8);

  @Test
  void brokerPartitionChangeIntentHasContinuousValuesAndExplicitEvents() {
    final BrokerPartitionChangeIntent[] intents = BrokerPartitionChangeIntent.values();
    final Set<BrokerPartitionChangeIntent> events =
        Set.of(
            BrokerPartitionChangeIntent.SUBMITTED,
            BrokerPartitionChangeIntent.CANCELLED,
            BrokerPartitionChangeIntent.STATUS_UPDATED,
            BrokerPartitionChangeIntent.SUPERSEDING,
            BrokerPartitionChangeIntent.REPLACED,
            BrokerPartitionChangeIntent.AUTHORIZED,
            BrokerPartitionChangeIntent.BLOCKED,
            BrokerPartitionChangeIntent.ADMITTED,
            BrokerPartitionChangeIntent.EXECUTION_UPDATED,
            BrokerPartitionChangeIntent.JOINED,
            BrokerPartitionChangeIntent.LEFT,
            BrokerPartitionChangeIntent.CREATED,
            BrokerPartitionChangeIntent.PROMOTED,
            BrokerPartitionChangeIntent.PRIORITY_CHANGED,
            BrokerPartitionChangeIntent.LEADER_TRANSFERRED,
            BrokerPartitionChangeIntent.LEADER_STEPPED_DOWN,
            BrokerPartitionChangeIntent.PEERS_RESET);

    for (short value = 0; value < intents.length; value++) {
      final BrokerPartitionChangeIntent intent = intents[value];
      assertEquals(value, intent.value());
      assertSame(intent, BrokerPartitionChangeIntent.from(value));
      assertSame(intent, Intent.fromProtocolValue(ValueType.PARTITION_CHANGE, value));
      assertEquals(events.contains(intent), intent.isEvent());
    }
    assertNull(BrokerPartitionChangeIntent.from((short) -1));
    assertNull(BrokerPartitionChangeIntent.from((short) intents.length));
  }

  @Test
  void brokerPartitionChangeStatusRetainsLegacySbeLayoutAndValues() {
    assertEquals(324, ChangePlanRecordDecoder.TEMPLATE_ID);
    assertEquals(5, ChangePlanRecordDecoder.statusId());
    assertEquals(24, ChangePlanRecordDecoder.statusEncodingOffset());
    assertEquals(1, ChangePlanRecordDecoder.statusEncodingLength());

    for (final BrokerPartitionChangeStatus status : BrokerPartitionChangeStatus.values()) {
      final var wireStatus = valueOf(status.name());
      assertEquals(status.ordinal(), wireStatus.value());
      assertEquals(
          status,
          ChangePlanRecordCodec.fromBytes(ChangePlanRecordCodec.toBytes(plan(status))).status());
    }
  }

  @Test
  void clusterChangeOperationCodecReadsAndWritesLegacyPhaseProperty() {
    final ClusterChangeOperation restored =
        ClusterChangeOperationCodec.fromBytes(LEGACY_CLUSTER_CHANGE_MANIFEST);

    assertEquals(BrokerClusterChangeStatus.WAITING_READY, restored.status());
    final String encoded =
        MsgPackConverter.convertToJson(ClusterChangeOperationCodec.toBytes(restored));
    assertTrue(encoded.contains("\"phase\":\"WAITING_READY\""));
    assertFalse(encoded.contains("\"status\""));
  }

  private static ChangePlan plan(final BrokerPartitionChangeStatus status) {
    return new ChangePlan(
        new ChangePlan.Definition(
            new ChangePlan.Identity("demo", "change-1", 1),
            new ChangePlan.Versions(11, 12),
            List.of()),
        new ChangePlan.Progress(0, status),
        new ChangePlan.Deadline(100, 200));
  }
}
