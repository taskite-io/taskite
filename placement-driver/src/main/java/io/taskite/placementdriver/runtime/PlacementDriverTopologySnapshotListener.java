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

import io.taskite.db.impl.DbConstants;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.snapshot.SnapshotLifecycleListener;
import io.taskite.topology.MemberRole;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes the node-local bootstrap manifest after a JRaft snapshot save/load. */
public final class PlacementDriverTopologySnapshotListener implements SnapshotLifecycleListener {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDriverTopologySnapshotListener.class);
  private static final String TOPOLOGY_KEY = "placement-driver";

  private final Path dataDirectory;
  private final String localMemberId;
  private final PlacementDriverTopologyManifestWriter writer;

  public PlacementDriverTopologySnapshotListener(
      final Path dataDirectory, final String localMemberId) {
    this(dataDirectory, localMemberId, new PlacementDriverTopologyManifestWriter());
  }

  PlacementDriverTopologySnapshotListener(
      final Path dataDirectory,
      final String localMemberId,
      final PlacementDriverTopologyManifestWriter writer) {
    this.dataDirectory = dataDirectory;
    this.localMemberId = localMemberId;
    this.writer = writer;
  }

  @Override
  public void onSnapshotSaved(
      final Path checkpointDirectory, final long snapshotIndex, final long snapshotTerm) {
    write(
        checkpointDirectory,
        PlacementDriverTopologyManifest.Source.SNAPSHOT_SAVE,
        snapshotIndex,
        snapshotTerm);
  }

  @Override
  public void onSnapshotLoaded(
      final Path checkpointDirectory, final long snapshotIndex, final long snapshotTerm) {
    write(
        checkpointDirectory,
        PlacementDriverTopologyManifest.Source.SNAPSHOT_LOAD,
        snapshotIndex,
        snapshotTerm);
  }

  private void write(
      final Path checkpointDirectory,
      final PlacementDriverTopologyManifest.Source source,
      final long snapshotIndex,
      final long snapshotTerm) {
    try {
      final MemberTopologyRecord topology = readTopology(checkpointDirectory);
      if (topology == null) {
        LOG.warn(
            "skip topology manifest for snapshot {} because committed topology is absent",
            snapshotIndex);
        return;
      }
      if (topology.getMembers().stream()
          .filter(PlacementDriverTopologySnapshotListener::isCommittedMember)
          .noneMatch(member -> localMemberId.equals(member.getMemberId()))) {
        LOG.warn(
            "skip {} topology manifest for snapshot {} because local member {} is absent",
            source,
            snapshotIndex,
            localMemberId);
        return;
      }
      writer.write(dataDirectory, manifest(topology, source, snapshotIndex, snapshotTerm));
    } catch (final RuntimeException error) {
      LOG.error(
          "failed to write placement-driver topology manifest for snapshot {}",
          snapshotIndex,
          error);
    }
  }

  private PlacementDriverTopologyManifest manifest(
      final MemberTopologyRecord topology,
      final PlacementDriverTopologyManifest.Source source,
      final long snapshotIndex,
      final long snapshotTerm) {
    final List<PlacementDriverTopologyManifest.Member> members =
        topology.getMembers().stream()
            .filter(PlacementDriverTopologySnapshotListener::isCommittedMember)
            .map(
                member ->
                    new PlacementDriverTopologyManifest.Member(
                        member.getMemberId(),
                        member.getHost(),
                        member.getHttpPort(),
                        member.getGrpcPort(),
                        member.getRaftPort(),
                        member.getRole() == MemberRole.LEARNER
                            ? PlacementDriverTopologyManifest.Role.LEARNER
                            : PlacementDriverTopologyManifest.Role.VOTER,
                        member.getIncarnationId().isBlank()
                            ? member.getMemberId() + "-0"
                            : member.getIncarnationId(),
                        member.getEndpointEpoch()))
            .toList();
    return new PlacementDriverTopologyManifest(
        PlacementDriverTopologyManifest.CURRENT_VERSION,
        PlacementDriverTopologyManifest.CLUSTER_ID,
        PlacementDriverTopologyManifest.PARTITION_ID,
        localMemberId,
        source,
        topology.getConfigurationEpoch(),
        topology.getConfigurationEpoch(),
        snapshotIndex,
        snapshotIndex,
        snapshotTerm,
        topology.getLeaderId(),
        Instant.now(),
        members,
        null);
  }

  private static boolean isCommittedMember(final MemberTopologyRecord.MemberRecord member) {
    return member.getRole().isAdmitted();
  }

  private static MemberTopologyRecord readTopology(final Path checkpointDirectory) {
    try (final DBOptions options =
            new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
        final ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> descriptors =
          List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
      final List<ColumnFamilyHandle> handles = new ArrayList<>();
      RocksDB db = null;
      try {
        db = RocksDB.openReadOnly(options, checkpointDirectory.toString(), descriptors, handles);
        final byte[] value = db.get(handles.getFirst(), topologyKey());
        if (value == null) {
          return null;
        }
        final MemberTopologyRecord topology = new MemberTopologyRecord();
        topology.wrap(new UnsafeBuffer(value), 0, value.length);
        return topology;
      } finally {
        handles.forEach(ColumnFamilyHandle::close);
        if (db != null) {
          db.close();
        }
      }
    } catch (final Exception error) {
      throw new IllegalStateException(
          "failed to read topology from checkpoint " + checkpointDirectory, error);
    }
  }

  private static byte[] topologyKey() {
    final byte[] text = TOPOLOGY_KEY.getBytes(StandardCharsets.UTF_8);
    final UnsafeBuffer buffer =
        new UnsafeBuffer(new byte[Long.BYTES + Integer.BYTES + text.length]);
    buffer.putLong(0, ColumnFamilies.PD_MEMBER_TOPOLOGY.getValue(), DbConstants.DB_BYTE_ORDER);
    buffer.putInt(Long.BYTES, text.length, DbConstants.DB_BYTE_ORDER);
    buffer.putBytes(Long.BYTES + Integer.BYTES, text);
    return buffer.byteArray();
  }
}
