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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.topology.MemberRole;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class PlacementDriverTopologyManifestTest {

  @Test
  void atomicallyRoundTripsManifest(@TempDir final Path directory) throws Exception {
    final PlacementDriverTopologyManifest manifest = manifest("meta-0");
    new PlacementDriverTopologyManifestWriter().write(directory, manifest);

    final PlacementDriverTopologyManifest restored =
        new PlacementDriverTopologyManifestReader().read(directory, "meta-0").orElseThrow();

    assertEquals(PlacementDriverTopologyManifest.Source.SHUTDOWN, restored.source());
    assertEquals(2, restored.members().size());
    assertEquals(12, restored.raftTerm());
    assertTrue(restored.checksum().value().matches("[a-f0-9]{64}"));
    assertTrue(!Files.exists(directory.resolve(".topology.tmp")));
  }

  @Test
  void rejectsCorruptChecksum(@TempDir final Path directory) throws Exception {
    new PlacementDriverTopologyManifestWriter().write(directory, manifest("meta-0"));
    final Path file = directory.resolve(".topology");
    final String corrupt = Files.readString(file).replace("\"raftTerm\" : 12", "\"raftTerm\" : 13");
    Files.writeString(file, corrupt);

    assertThrows(
        IllegalArgumentException.class,
        () -> new PlacementDriverTopologyManifestReader().read(directory, "meta-0"));
  }

  @Test
  void rejectsLocalIdentityMismatch(@TempDir final Path directory) {
    new PlacementDriverTopologyManifestWriter().write(directory, manifest("meta-0"));

    assertThrows(
        IllegalArgumentException.class,
        () -> new PlacementDriverTopologyManifestReader().read(directory, "meta-1"));
  }

  @Test
  void snapshotCheckpointWritesBootstrapManifest(@TempDir final Path directory) {
    final Path checkpoint = directory.resolve("checkpoint");
    final Path dataDirectory = directory.resolve("pd-data");
    try (PartitionStorage storage = new PartitionStorage(directory.resolve("runtime"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getPlacementDriverTopologyState()
          .updateTopology(
              new MemberTopologyRecord()
                  .setLeaderId("meta-0")
                  .setTerm(9)
                  .setConfigurationEpoch(8)
                  .setMembers(
                      List.of(
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("meta-0")
                              .setRole(MemberRole.LEADER)
                              .setHost("127.0.0.1")
                              .setHttpPort(19001)
                              .setGrpcPort(29001)
                              .setRaftPort(29101)
                              .setIncarnationId("meta-0-1")
                              .setEndpointEpoch(2))));
      storage.createCheckpoint(checkpoint);
    }

    new PlacementDriverTopologySnapshotListener(dataDirectory, "meta-0")
        .onSnapshotSaved(checkpoint, 55, 9);

    final PlacementDriverTopologyManifest restored =
        new PlacementDriverTopologyManifestReader().read(dataDirectory, "meta-0").orElseThrow();
    assertEquals(PlacementDriverTopologyManifest.Source.SNAPSHOT_SAVE, restored.source());
    assertEquals(55, restored.snapshotIndex());
    assertEquals(8, restored.configurationEpoch());
    assertEquals("meta-0", restored.members().getFirst().memberId());
  }

  @Test
  void snapshotLoadWithoutLocalMemberPreservesExistingManifest(@TempDir final Path directory) {
    final Path checkpoint = directory.resolve("checkpoint");
    final Path dataDirectory = directory.resolve("pd-data");
    new PlacementDriverTopologyManifestWriter().write(dataDirectory, manifest("meta-1"));
    try (PartitionStorage storage = new PartitionStorage(directory.resolve("runtime"))) {
      final MutableProcessingState state =
          new MutableProcessingState(new StateStoreProcessingStore(storage));
      state
          .getPlacementDriverTopologyState()
          .updateTopology(
              new MemberTopologyRecord()
                  .setLeaderId("meta-0")
                  .setTerm(9)
                  .setConfigurationEpoch(8)
                  .setMembers(
                      List.of(
                          new MemberTopologyRecord.MemberRecord()
                              .setMemberId("meta-0")
                              .setRole(MemberRole.LEADER)
                              .setHost("127.0.0.1")
                              .setHttpPort(19001)
                              .setGrpcPort(29001)
                              .setRaftPort(29101)
                              .setIncarnationId("meta-0-1")
                              .setEndpointEpoch(2))));
      storage.createCheckpoint(checkpoint);
    }

    new PlacementDriverTopologySnapshotListener(dataDirectory, "meta-1")
        .onSnapshotLoaded(checkpoint, 55, 9);

    final PlacementDriverTopologyManifest restored =
        new PlacementDriverTopologyManifestReader().read(dataDirectory, "meta-1").orElseThrow();
    assertEquals(PlacementDriverTopologyManifest.Source.SHUTDOWN, restored.source());
    assertEquals(7, restored.topologyVersion());
  }

  private static PlacementDriverTopologyManifest manifest(final String localMemberId) {
    return new PlacementDriverTopologyManifest(
        1,
        "placement-driver",
        1,
        localMemberId,
        PlacementDriverTopologyManifest.Source.SHUTDOWN,
        7,
        7,
        42,
        null,
        12,
        "meta-0",
        Instant.parse("2026-08-14T00:00:00Z"),
        List.of(
            new PlacementDriverTopologyManifest.Member(
                "meta-0",
                "127.0.0.1",
                19001,
                29001,
                29101,
                PlacementDriverTopologyManifest.Role.VOTER,
                "meta-0-1",
                1),
            new PlacementDriverTopologyManifest.Member(
                "meta-1",
                "127.0.0.1",
                19002,
                29002,
                29102,
                PlacementDriverTopologyManifest.Role.LEARNER,
                "meta-1-1",
                1)),
        null);
  }
}
