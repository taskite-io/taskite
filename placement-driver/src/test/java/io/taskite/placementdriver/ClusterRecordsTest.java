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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.common.codec.BrokerClusterConfigurationRecordCodec;
import io.taskite.placementdriver.common.codec.BrokerRegistrationRecordCodec;
import io.taskite.protocol.record.ClusterBrokerRecordDecoder;
import io.taskite.protocol.record.MessageHeaderEncoder;
import io.taskite.topology.BrokerPartitionState;
import io.taskite.topology.MemberRole;
import java.time.Instant;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class ClusterRecordsTest {
  @Test
  void brokerRecordRoundTripsThroughRegistration() {
    BrokerRegistration registration = registration();

    BrokerRegistration restored =
        BrokerRegistrationRecordCodec.toRegistration(
            BrokerRegistrationRecordCodec.toBytes(registration));

    assertEquals(registration, restored);
  }

  @Test
  void readsPreviousBrokerRecordVersionWithUnchangedLayout() {
    final BrokerRegistration registration = registration();
    final byte[] previous = BrokerRegistrationRecordCodec.toBytes(registration);
    new MessageHeaderEncoder().wrap(new UnsafeBuffer(previous), 0).version(10);

    assertEquals(registration, BrokerRegistrationRecordCodec.toRegistration(previous));
  }

  @Test
  void rejectsBrokerRecordVersionBeforeSupportedLayouts() {
    final byte[] previous = BrokerRegistrationRecordCodec.toBytes(registration());
    new MessageHeaderEncoder().wrap(new UnsafeBuffer(previous), 0).version(8);
    assertThrows(
        IllegalArgumentException.class,
        () -> BrokerRegistrationRecordCodec.toRegistration(previous));
  }

  @Test
  void rejectsFutureBrokerRecordVersion() {
    final byte[] future = BrokerRegistrationRecordCodec.toBytes(registration());
    new MessageHeaderEncoder()
        .wrap(new UnsafeBuffer(future), 0)
        .version(ClusterBrokerRecordDecoder.SCHEMA_VERSION + 1);

    assertThrows(
        IllegalArgumentException.class, () -> BrokerRegistrationRecordCodec.toRegistration(future));
  }

  @Test
  void rejectsBrokerRecordMissingRequiredFixedField() {
    final BrokerRegistration registration = registration();
    final byte[] current = BrokerRegistrationRecordCodec.toBytes(registration);
    final int headerLength = MessageHeaderEncoder.ENCODED_LENGTH;
    final int oldBlockLength = 20;
    final int addedFieldLength = Long.BYTES;
    final byte[] old = new byte[current.length - addedFieldLength];
    System.arraycopy(current, 0, old, 0, headerLength + oldBlockLength);
    System.arraycopy(
        current,
        headerLength + oldBlockLength + addedFieldLength,
        old,
        headerLength + oldBlockLength,
        current.length - headerLength - oldBlockLength - addedFieldLength);
    new MessageHeaderEncoder()
        .wrap(new UnsafeBuffer(old), 0)
        .blockLength(oldBlockLength)
        .version(3);

    assertThrows(
        IllegalArgumentException.class, () -> BrokerRegistrationRecordCodec.toRegistration(old));
  }

  @Test
  void topologyRecordRoundTrips() {
    BrokerClusterConfigurationRecordCodec record =
        new BrokerClusterConfigurationRecordCodec()
            .setClusterId("demo")
            .setClusterSize(3)
            .setPartitionSize(5)
            .setReplicationFactor(2);

    BrokerClusterConfigurationRecordCodec restored =
        new BrokerClusterConfigurationRecordCodec().wrap(record.toBytes());

    assertEquals("demo", restored.getClusterId());
    assertEquals(3, restored.getClusterSize());
    assertEquals(5, restored.getPartitionSize());
    assertEquals(2, restored.getReplicationFactor());
    // An un-migrated record carries no roster: epoch 0 and an empty roster (legacy fallback).
    assertEquals(0L, restored.getRosterEpoch());
    assertEquals(List.of(), restored.getVoterRoster());
  }

  @Test
  void topologyRecordRoundTripsVoterRoster() {
    BrokerClusterConfigurationRecordCodec record =
        new BrokerClusterConfigurationRecordCodec()
            .setClusterId("demo")
            .setClusterSize(3)
            .setPartitionSize(5)
            .setReplicationFactor(2)
            .setRosterEpoch(7)
            .setVoterRoster(List.of("broker-0", "broker-1", "broker-2"));

    BrokerClusterConfigurationRecordCodec restored =
        new BrokerClusterConfigurationRecordCodec().wrap(record.toBytes());

    assertEquals("demo", restored.getClusterId());
    assertEquals(3, restored.getClusterSize());
    assertEquals(5, restored.getPartitionSize());
    assertEquals(2, restored.getReplicationFactor());
    assertEquals(7L, restored.getRosterEpoch());
    // Roster order is significant (round-robin input), so equality is order-sensitive.
    assertEquals(List.of("broker-0", "broker-1", "broker-2"), restored.getVoterRoster());
  }

  private static BrokerRegistration registration() {
    return new BrokerRegistration(
        new BrokerRegistration.Identity("demo", "broker-0", "1.2.3", ""),
        new BrokerRegistration.Endpoint("127.0.0.1", 26501, 26601),
        new BrokerRegistration.Lease(
            Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-01T00:05:00Z"), 45_000),
        BrokerStatus.ONLINE,
        List.of(new BrokerPartitionState(0, MemberRole.LEADER, 7)));
  }
}
