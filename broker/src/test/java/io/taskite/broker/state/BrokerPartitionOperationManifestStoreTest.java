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
package io.taskite.broker.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.broker.partition.BrokerPartitionOperation;
import io.taskite.broker.partition.BrokerPartitionOperationManifest;
import io.taskite.broker.partition.BrokerPartitionOperationStatus;
import io.taskite.protocol.record.TopologyOperationType;
import io.taskite.protocol.record.value.broker.BrokerPartitionMember;
import io.taskite.state.PartitionStorage;
import io.taskite.topology.RaftConfigurationReference;
import io.taskite.topology.RecoveryFence;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerPartitionOperationManifestStoreTest {
  @TempDir Path tempDir;

  @Test
  void operationResultSurvivesStoreRecreation() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final BrokerPartitionOperationManifest expected =
          new BrokerPartitionOperationManifest(
              new BrokerPartitionOperationManifest.Identity(1, "1:JOIN:broker-2:0"),
              new BrokerPartitionOperationManifest.Result(
                  BrokerPartitionOperationStatus.APPLIED, 42, ""),
              BrokerPartitionOperationManifest.Evidence.UNKNOWN);
      new BrokerPartitionOperationManifestStore(storage).put(expected);

      final BrokerPartitionOperationManifestStore reopened =
          new BrokerPartitionOperationManifestStore(storage);
      assertEquals(expected, reopened.find(expected.opId()).orElseThrow());
      assertEquals(expected, reopened.latest().orElseThrow());
    }
  }

  @Test
  void pinnedRequestSurvivesReopenAndRejectsChangedSource() {
    final Path path = tempDir.resolve("pinned");
    final var request =
        new BrokerPartitionOperation(
            new BrokerPartitionOperation.Partition("demo", 1),
            new BrokerPartitionOperation.Operation(
                "plan-a:join", TopologyOperationType.JOIN, "broker-1", 0),
            new BrokerPartitionOperation.Configuration(
                List.of(),
                new RaftConfigurationReference(7, "source-a"),
                new RecoveryFence(3, 0, "")));
    try (PartitionStorage storage = new PartitionStorage(path)) {
      new BrokerPartitionOperationManifestStore(storage).pinRequest(request);
    }
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final var store = new BrokerPartitionOperationManifestStore(storage);
      store.pinRequest(request);
      final var changed =
          new BrokerPartitionOperation(
              new BrokerPartitionOperation.Partition("demo", 1),
              new BrokerPartitionOperation.Operation(
                  "plan-a:join", TopologyOperationType.JOIN, "broker-1", 0),
              new BrokerPartitionOperation.Configuration(
                  List.of(),
                  new RaftConfigurationReference(8, "source-b"),
                  new RecoveryFence(3, 0, "")));
      final var error = assertThrows(IllegalStateException.class, () -> store.pinRequest(changed));
      assertTrue(error.getMessage().contains("OPERATION_REQUEST_CHANGED"));
    }
  }

  @Test
  void membershipStageAuthoritySurvivesReopenAndEndsWhenAnotherOperationStarts() {
    final Path path = tempDir.resolve("stages");
    final var request =
        new BrokerPartitionOperation(
            new BrokerPartitionOperation.Partition("demo", 1),
            new BrokerPartitionOperation.Operation(
                "promote-a", TopologyOperationType.PROMOTE_LEARNER, "a", 10),
            new BrokerPartitionOperation.Configuration(
                List.of(),
                new RaftConfigurationReference(7, "source"),
                new RecoveryFence(0, 0, "")));
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final var store = new BrokerPartitionOperationManifestStore(storage);
      store.authorizeConfigurations(request, List.of("source", "learners-removed", "target"));
      store.put(
          new BrokerPartitionOperationManifest(
              new BrokerPartitionOperationManifest.Identity(1, request.opId()),
              new BrokerPartitionOperationManifest.Result(
                  BrokerPartitionOperationStatus.IN_PROGRESS, 0, ""),
              BrokerPartitionOperationManifest.Evidence.UNKNOWN));
    }
    try (PartitionStorage storage = new PartitionStorage(path)) {
      final var store = new BrokerPartitionOperationManifestStore(storage);
      assertTrue(store.canResume(request, "learners-removed"));
      assertFalse(store.canResume(request, "unrelated"));
      store.put(
          new BrokerPartitionOperationManifest(
              new BrokerPartitionOperationManifest.Identity(1, "another-op"),
              new BrokerPartitionOperationManifest.Result(
                  BrokerPartitionOperationStatus.IN_PROGRESS, 0, ""),
              BrokerPartitionOperationManifest.Evidence.UNKNOWN));
      assertFalse(store.canResume(request, "learners-removed"));
    }
  }

  @Test
  void pinsEveryExecutionParameterAndMemberOrderAcrossReopen() {
    final var first = new BrokerPartitionMember("a", true, 100);
    final var second = new BrokerPartitionMember("b", false, 0);
    final var partition = new BrokerPartitionOperation.Partition("demo", 1);
    final var op =
        new BrokerPartitionOperation.Operation("pinned", TopologyOperationType.JOIN, "b", 100);
    final var config =
        new BrokerPartitionOperation.Configuration(
            List.of(first, second),
            new RaftConfigurationReference(7, "source"),
            new RecoveryFence(3, 4, "proof"));
    final var authority = new BrokerPartitionOperation.Authority(5, "owner");
    final var request = new BrokerPartitionOperation(partition, op, config, authority);
    final Path path = tempDir.resolve("all-fields");
    try (final var storage = new PartitionStorage(path)) {
      new BrokerPartitionOperationManifestStore(storage).pinRequest(request);
    }
    final var variants =
        List.of(
            new BrokerPartitionOperation(
                new BrokerPartitionOperation.Partition("other", 1), op, config, authority),
            new BrokerPartitionOperation(
                new BrokerPartitionOperation.Partition("demo", 2), op, config, authority),
            new BrokerPartitionOperation(
                partition,
                new BrokerPartitionOperation.Operation(
                    "pinned", TopologyOperationType.ADD, "b", 100),
                config,
                authority),
            new BrokerPartitionOperation(
                partition,
                new BrokerPartitionOperation.Operation(
                    "pinned", TopologyOperationType.JOIN, "a", 100),
                config,
                authority),
            new BrokerPartitionOperation(
                partition,
                new BrokerPartitionOperation.Operation(
                    "pinned", TopologyOperationType.JOIN, "b", 200),
                config,
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    List.of(second, first), config.source(), config.recoveryFence()),
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    config.members(),
                    new RaftConfigurationReference(8, "source"),
                    config.recoveryFence()),
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    config.members(),
                    new RaftConfigurationReference(7, "changed"),
                    config.recoveryFence()),
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    config.members(), config.source(), new RecoveryFence(2, 4, "proof")),
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    config.members(), config.source(), new RecoveryFence(3, 5, "proof")),
                authority),
            new BrokerPartitionOperation(
                partition,
                op,
                new BrokerPartitionOperation.Configuration(
                    config.members(), config.source(), new RecoveryFence(3, 4, "changed")),
                authority),
            new BrokerPartitionOperation(
                partition, op, config, new BrokerPartitionOperation.Authority(6, "owner")),
            new BrokerPartitionOperation(
                partition, op, config, new BrokerPartitionOperation.Authority(5, "changed")));
    try (final var storage = new PartitionStorage(path)) {
      final var store = new BrokerPartitionOperationManifestStore(storage);
      store.pinRequest(request);
      for (final var changed : variants) {
        final var error =
            assertThrows(IllegalStateException.class, () -> store.pinRequest(changed));
        assertTrue(error.getMessage().contains("OPERATION_REQUEST_CHANGED"));
      }
      store.pinRequest(request);
    }
  }

  @Test
  void closedStorageIsNotAccessedThroughNativeHandle() {
    final PartitionStorage storage = new PartitionStorage(tempDir.resolve("closed-runtime"));
    final BrokerPartitionOperationManifestStore store =
        new BrokerPartitionOperationManifestStore(storage);
    final BrokerPartitionOperationManifest manifest =
        new BrokerPartitionOperationManifest(
            new BrokerPartitionOperationManifest.Identity(1, "1:JOIN:broker-2:0"),
            new BrokerPartitionOperationManifest.Result(
                BrokerPartitionOperationStatus.APPLIED, 42, ""),
            BrokerPartitionOperationManifest.Evidence.UNKNOWN);
    store.put(manifest);

    storage.close();

    assertTrue(store.latest().isEmpty());
    assertThrows(IllegalStateException.class, () -> store.put(manifest));
  }
}
