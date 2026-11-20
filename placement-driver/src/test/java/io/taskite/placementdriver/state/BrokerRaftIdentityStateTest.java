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
package io.taskite.placementdriver.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.BrokerRegistrationRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.util.RaftMemberIdentity;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerRaftIdentityStateTest {
  @TempDir Path directory;

  @Test
  void updatedEndpointAndIncarnationSurvivePdRestart() throws Exception {
    final var key = new BrokerRegistryKey("demo", "broker-0");
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store, () -> 0, () -> 0, 50);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-0",
              "127.0.0.1",
              26501,
              26502,
              "1",
              1000,
              Instant.ofEpochMilli(1000),
              List.of(),
              "startup-a");
      state
          .getBrokerRegistrationState()
          .markBrokerOffline("demo", "broker-0", Instant.ofEpochMilli(2000));
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-0",
              "::1",
              36501,
              36502,
              "2",
              1000,
              Instant.ofEpochMilli(3000),
              List.of(),
              "startup-b");
      final var moved = state.getBrokerRegistrationState().registration(key).orElseThrow();
      assertEquals("demo", moved.clusterId());
      assertEquals("broker-0", moved.brokerId());
      assertEquals("::1", moved.host());
      assertEquals(36501, moved.port());
      assertEquals(36502, moved.raftPort());
      assertEquals("startup-b", moved.incarnationId());
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var state = new MutableProcessingState(store, () -> 0, () -> 0, 50);
      final var recovered = state.getBrokerRegistrationState().registration(key).orElseThrow();
      assertEquals("demo", recovered.clusterId());
      assertEquals("broker-0", recovered.brokerId());
      assertEquals("::1", recovered.host());
      assertEquals(36502, recovered.raftPort());
      assertEquals("startup-b", recovered.incarnationId());
      assertEquals(1, state.getRegistrationDeadlineState().findDue(Long.MAX_VALUE, 10).size());
    }
  }

  @Test
  void registrationsWithSameBrokerIdAreIsolatedByCluster() {
    final var key = new BrokerRegistryKey("demo", "broker-0");
    final String identity = RaftMemberIdentity.of("demo", "broker-0");
    try (var storage = new PartitionStorage(directory)) {
      final var state =
          new MutableProcessingState(new StateStoreProcessingStore(storage), () -> 0, () -> 0, 50);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-0",
              "127.0.0.1",
              26501,
              26502,
              "1",
              1000,
              Instant.EPOCH,
              List.of(),
              "startup-a");
      state.getBrokerRegistrationState().markBrokerOffline("demo", "broker-0", Instant.EPOCH);
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "demo",
              "broker-0",
              "::1",
              36501,
              36502,
              "2",
              1000,
              Instant.ofEpochMilli(1000),
              List.of(),
              "startup-b");
      state
          .getBrokerRegistrationState()
          .registerBroker(
              "other",
              "broker-0",
              "127.0.0.2",
              46501,
              46502,
              "2",
              1000,
              Instant.ofEpochMilli(1000),
              List.of(),
              "startup-c");
      final var demo = state.getBrokerRegistrationState().registration(key).orElseThrow();
      final var other =
          state
              .getBrokerRegistrationState()
              .registration(new BrokerRegistryKey("other", "broker-0"))
              .orElseThrow();
      assertEquals("::1", demo.host());
      assertEquals("127.0.0.2", other.host());
      assertEquals(identity, RaftMemberIdentity.of(demo.clusterId(), demo.brokerId()));
      Assertions.assertNotEquals(
          identity, RaftMemberIdentity.of(other.clusterId(), other.brokerId()));
    }
  }

  @Test
  void registrationEventRoundTripsExistingIdentityFieldsAndEndpoints() {
    final var original =
        new BrokerRegistrationRecord()
            .setClusterId("demo")
            .setBrokerId("broker-0")
            .setHost("127.0.0.1")
            .setPort(26501)
            .setRaftPort(26502)
            .setIncarnationId("startup-a");
    final var buffer = new ExpandableArrayBuffer();
    original.write(buffer, 0);
    final var restored = new BrokerRegistrationRecord();
    restored.wrap(buffer, 0, original.getLength());
    assertEquals("demo", restored.getClusterId());
    assertEquals("broker-0", restored.getBrokerId());
    assertEquals("127.0.0.1", restored.getHost());
    assertEquals(26501, restored.getPort());
    assertEquals("startup-a", restored.getIncarnationId());
    assertEquals(26502, restored.getRaftPort());
  }
}
