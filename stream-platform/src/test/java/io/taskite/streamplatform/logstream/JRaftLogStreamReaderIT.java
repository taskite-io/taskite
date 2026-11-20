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
package io.taskite.streamplatform.logstream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.util.Endpoint;
import io.taskite.cluster.raft.RaftPartitionConfig;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.cluster.raft.RaftRole;
import io.taskite.cluster.raft.jraft.JRaftPartition;
import io.taskite.cluster.raft.jraft.runtime.JRaftServer;
import io.taskite.protocol.impl.record.value.broker.JobRecord;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.broker.JobIntent;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end proof of migration decision D1 across the module seam: a real single-node jraft
 * partition ({@link JRaftPartition}) is fed batch-carrying appends, then the production {@link
 * RaftLogStreamReader} — bound to jraft only via {@link JRaftPartition#openLogReader()} — expands
 * the committed jraft log into per-position {@link LoggedEvent}s, exactly as it does over the
 * in-house journal. This is the concrete evidence that the logstream layer can ride jraft with no
 * change: the same reader, the same position semantics, a different engine underneath.
 */
final class JRaftLogStreamReaderIT {

  @Test
  void raftLogStreamReaderExpandsJraftCommittedBatchesIntoPositions(@TempDir final Path baseDir)
      throws Exception {
    final int port = freePort();
    final RaftPeer self = new RaftPeer("b0", "127.0.0.1", port, true);
    final JRaftServer server = new JRaftServer(new Endpoint("127.0.0.1", port));
    final Path groupDir = baseDir.resolve("partition-0");
    Files.createDirectories(groupDir.resolve("log"));
    Files.createDirectories(groupDir.resolve("meta"));
    Files.createDirectories(groupDir.resolve("snapshot"));

    final RaftPartitionConfig config =
        RaftPartitionConfig.builder("logstream-jraft", 0, self.brokerId())
            .peers(List.of(self))
            .dataDirectory(baseDir)
            .electionTimeout(Duration.ofSeconds(1))
            .build();
    final JRaftPartition partition = JRaftPartition.builder(config, server, groupDir).build();

    try {
      partition.start();
      server.start();
      awaitLeader(partition);

      // Two batches, framed exactly like the Sequencer does: index-1 spans positions [1..3],
      // index-2 spans position [4]. Each record's key encodes its position, for read-back asserts.
      partition
          .append(1, 3, BatchPayloadCodec.encode(List.of(entry(1), entry(2), entry(3))))
          .join(10, TimeUnit.SECONDS);
      partition
          .append(4, 4, BatchPayloadCodec.encode(List.of(entry(4))))
          .join(10, TimeUnit.SECONDS);
      awaitCommittedPosition(partition, 4);

      // The production reader, bound to jraft ONLY through openLogReader(), must expand the two
      // committed batches into positions 1,2,3,4 in order — identical to the in-house behaviour.
      final RaftLogStreamReader reader = new RaftLogStreamReader(partition::openLogReader);
      reader.seekToNextEvent(-1); // from the start

      long expectedPosition = 1;
      while (reader.hasNext()) {
        assertEquals(expectedPosition, reader.getNextPosition());
        final LoggedEvent event = reader.next();
        assertEquals(expectedPosition, event.getPosition());
        assertEquals(expectedPosition, event.getKey());
        expectedPosition++;
      }
      assertEquals(5, reader.getNextPosition(), "must have expanded all four positions [1..4]");
      reader.close();

      // Resume mid-batch: after position 2, the next record must be position 3 (still in batch A).
      final RaftLogStreamReader resumed = new RaftLogStreamReader(partition::openLogReader);
      resumed.seekToNextEvent(2);
      assertTrue(resumed.hasNext());
      assertEquals(3, resumed.getNextPosition());
      assertEquals(3, resumed.next().getKey());
      assertEquals(4, resumed.next().getKey());
      assertFalse(resumed.hasNext());
      resumed.close();
    } finally {
      partition.close();
      server.close();
    }
  }

  private static void awaitLeader(final JRaftPartition partition) throws InterruptedException {
    for (int i = 0; i < 200; i++) {
      if (partition.role() == RaftRole.LEADER) {
        return;
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("single node did not become leader");
  }

  private static void awaitCommittedPosition(final JRaftPartition partition, final long target)
      throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (partition.committedPosition() >= target) {
        return;
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException(
        "committedPosition " + partition.committedPosition() + " never reached " + target);
  }

  /** A framed log entry whose key is {@code position}, so reads can be asserted by key. */
  private static byte[] entry(final long position) {
    final RecordMetadata metadata =
        new RecordMetadata()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.JOB)
            .intent(JobIntent.ACTIVATED);
    final JobRecord value = new JobRecord().setJobType("t").setWorker("w").setTenantId("x");
    final LogAppendEntry appendEntry = LogAppendEntry.of(position, metadata, value);
    final byte[] bytes = new byte[LogEntryDescriptor.framedLength(appendEntry)];
    LogEntryDescriptor.serialize(new UnsafeBuffer(bytes), 0, appendEntry, position, -1, 0);
    return bytes;
  }

  private static int freePort() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
