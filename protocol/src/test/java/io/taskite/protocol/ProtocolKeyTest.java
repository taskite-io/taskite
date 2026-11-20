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
package io.taskite.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import org.junit.jupiter.api.Test;

final class ProtocolKeyTest {
  @Test
  void everyClusterAndPartitionHasAUniquePositiveEntityKey() {
    final var keys = new HashSet<Long>();
    for (long cluster = 0; cluster < Protocol.MAXIMUM_CLUSTERS; cluster++) {
      for (int partition = 1; partition <= Protocol.MAXIMUM_PARTITIONS; partition++) {
        final long key = Protocol.encodeKey(cluster, partition, 1);
        assertEquals(
            (cluster << (Protocol.PARTITION_BITS + Protocol.KEY_BITS))
                + ((long) partition << Protocol.KEY_BITS)
                + 1,
            key);
        assertTrue(key > 0);
        assertTrue(keys.add(key));
        assertEquals(cluster, Protocol.decodeClusterKey(key));
        assertEquals(partition, Protocol.decodePartitionId(key));
        assertEquals(1, Protocol.decodeKeyInPartition(key));
      }
    }
    assertEquals(8176, keys.size());
    assertEquals(Long.MAX_VALUE, Protocol.encodeKey(15, 511, Protocol.MAXIMUM_SEQUENCE));
    assertEquals(1L << Protocol.KEY_BITS, Protocol.encodeKey(0, 1, 0));
    assertEquals(1125899906842625L, Protocol.encodeKey(0, 1, 1));
    assertEquals(0, Protocol.decodePartitionId(0));
  }

  @Test
  void invalidInputsAreRejectedWithoutTruncationOrCarry() {
    assertThrows(IllegalArgumentException.class, () -> Protocol.encodeKey(-1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.encodeKey(16, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.encodeKey(0, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.encodeKey(0, 512, 1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.encodeKey(0, 1, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> Protocol.encodeKey(0, 1, Protocol.MAXIMUM_SEQUENCE + 1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.decodeClusterKey(-1));
    assertThrows(IllegalArgumentException.class, () -> Protocol.decodePartitionId(-1));
  }

  @Test
  void replacingPrefixesPreservesTheOtherFields() {
    final long key = Protocol.encodeKey(7, 400, 123);
    assertEquals(Protocol.encodeKey(12, 400, 123), Protocol.encodeClusterKey(12, key));
    assertEquals(Protocol.encodeKey(7, 511, 123), Protocol.encodePartitionId(511, key));
  }

  @Test
  void placementDriverAndBrokersShareTheEncodingWithDisjointClusterPrefixes() {
    final long pdMaximum =
        Protocol.encodeKey(
            Protocol.PLACEMENT_DRIVER_CLUSTER_KEY,
            Protocol.MAXIMUM_PARTITIONS,
            Protocol.MAXIMUM_SEQUENCE);
    assertEquals(0, Protocol.decodeClusterKey(pdMaximum));
    assertEquals(511, Protocol.decodePartitionId(pdMaximum));
    assertEquals(Protocol.MAXIMUM_SEQUENCE, Protocol.decodeKeyInPartition(pdMaximum));
    for (long cluster = 1; cluster < Protocol.MAXIMUM_CLUSTERS; cluster++) {
      assertTrue(pdMaximum < Protocol.encodeKey(cluster, 1, 0));
    }
  }
}
