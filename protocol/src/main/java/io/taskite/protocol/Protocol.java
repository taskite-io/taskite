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

/**
 * Defines the 64-bit key encoding shared by PD, Broker, and their protocol consumers. Within one PD
 * allocation domain, a key identifies its owning cluster, partition, and partition-local sequence.
 *
 * <p>The layout is {@code [sign:1][cluster:4][partition:9][sequence:50]}. Bit 63 is always zero,
 * cluster bits occupy 62–59, partition bits occupy 58–50, and sequence bits occupy 49–0.
 *
 * <p>Cluster key {@code 0} is reserved for PD; Broker clusters receive keys {@code 1..15}. Actual
 * partition ids are {@code 1..511}, stored directly in the 9-bit field. Partition id {@code 0} is
 * reserved and is not assigned to a runtime partition. Encoding and decoding are identical for PD
 * and Broker.
 *
 * <p>Entity sequences start at {@code 1}; sequence {@code 0} is reserved for generator baselines
 * and routing placeholders. PD partition {@code 1} has prefix {@code 1L << 50}, and its first
 * entity key is {@code encodeKey(0, 1, 1) == 1125899906842625L}. This key decodes to cluster {@code
 * 0}, partition {@code 1}, sequence {@code 1}.
 */
public final class Protocol {
  private Protocol() {}

  /** Number of bits reserved for the cluster namespace. */
  public static final int CLUSTER_BITS = 4;

  /** Number of bits reserved for the actual partition id. */
  public static final int PARTITION_BITS = 9;

  /** Number of low bits reserved for the sequence within one cluster and partition. */
  public static final int KEY_BITS = 50;

  /** Fixed cluster namespace used by PD, including job subscription keys and lease tokens. */
  public static final long PLACEMENT_DRIVER_CLUSTER_KEY = 0;

  /** Total namespace capacity, including the reserved PD namespace: {@code 2^4 == 16}. */
  public static final int MAXIMUM_CLUSTERS = 1 << CLUSTER_BITS;

  /** Maximum partition count and maximum actual partition id: {@code 2^9 - 1 == 511}. */
  public static final int MAXIMUM_PARTITIONS = (1 << PARTITION_BITS) - 1;

  /** Largest sequence that fits without carrying into the partition prefix: {@code 2^50 - 1}. */
  public static final long MAXIMUM_SEQUENCE = (1L << KEY_BITS) - 1;

  private static final int CLUSTER_SHIFT = PARTITION_BITS + KEY_BITS;
  private static final String INVALID_CLUSTER = "clusterKey must be in [0, %d], got %d";
  private static final String INVALID_PARTITION = "partitionId must be in [1, %d], got %d";
  private static final String INVALID_SEQUENCE = "sequence must be in [0, %d], got %d";
  private static final String INVALID_KEY = "Key must be nonnegative, got %d";

  /**
   * Combines a cluster namespace, actual partition id, and sequence into a nonnegative key.
   *
   * <p>The formula is {@code (clusterKey << 59) | ((long) partitionId << 50) | sequence}. The
   * validated fields occupy disjoint bit ranges, so bitwise OR and addition produce the same
   * result. Partition {@code 1} contributes offset {@code 1L << 50}.
   *
   * @param clusterKey cluster namespace in {@code 0..15}
   * @param partitionId actual partition id in {@code 1..511}
   * @param sequence partition-local sequence in {@code 0..MAXIMUM_SEQUENCE}
   * @throws IllegalArgumentException if any input is outside its range
   */
  public static long encodeKey(final long clusterKey, final int partitionId, final long sequence) {
    validateClusterKey(clusterKey);
    validatePartitionId(partitionId);
    validateSequence(sequence, MAXIMUM_SEQUENCE);
    return (clusterKey << CLUSTER_SHIFT) | ((long) partitionId << KEY_BITS) | sequence;
  }

  /** Validates the shared cluster namespace range; Broker startup additionally rejects PD's 0. */
  public static void validateClusterKey(final long clusterKey) {
    if (clusterKey < 0 || clusterKey >= MAXIMUM_CLUSTERS) {
      throw new IllegalArgumentException(
          INVALID_CLUSTER.formatted(MAXIMUM_CLUSTERS - 1, clusterKey));
    }
  }

  /** Validates an actual, 1-based partition id without silently truncating it to 9 bits. */
  public static void validatePartitionId(final int partitionId) {
    if (partitionId < 1 || partitionId > MAXIMUM_PARTITIONS) {
      throw new IllegalArgumentException(
          INVALID_PARTITION.formatted(MAXIMUM_PARTITIONS, partitionId));
    }
  }

  /**
   * Extracts the cluster namespace from a nonnegative key, including PD namespace {@code 0}.
   *
   * @throws IllegalArgumentException if the key is negative
   */
  public static long decodeClusterKey(final long key) {
    validateKey(key);
    return key >>> CLUSTER_SHIFT;
  }

  /**
   * Replaces the cluster prefix of an already encoded key, preserving its partition and sequence.
   * Use {@link #encodeKey(long, int, long)} when starting from a raw sequence.
   */
  public static long encodeClusterKey(final long clusterKey, final long key) {
    return encodeKey(clusterKey, decodePartitionId(key), decodeKeyInPartition(key));
  }

  /**
   * Replaces the partition prefix of an already encoded key, preserving its cluster and sequence.
   * The supplied partition id is 1-based. Use {@link #encodeKey(long, int, long)} when starting
   * from a raw sequence.
   */
  public static long encodePartitionId(final int partitionId, final long key) {
    return encodeKey(decodeClusterKey(key), partitionId, decodeKeyInPartition(key));
  }

  /**
   * Extracts the actual partition id directly from the partition field. A result of {@code 0}
   * indicates that no runtime partition prefix is present.
   *
   * @throws IllegalArgumentException if the key is negative
   */
  public static int decodePartitionId(final long key) {
    validateKey(key);
    return (int) ((key >>> KEY_BITS) & MAXIMUM_PARTITIONS);
  }

  /**
   * Extracts the partition-local sequence, discarding the cluster and partition prefixes.
   *
   * @throws IllegalArgumentException if the key is negative
   */
  public static long decodeKeyInPartition(final long key) {
    validateKey(key);
    return key & MAXIMUM_SEQUENCE;
  }

  private static void validateKey(final long key) {
    if (key < 0) {
      throw new IllegalArgumentException(INVALID_KEY.formatted(key));
    }
  }

  private static void validateSequence(final long sequence, final long maximum) {
    if (sequence < 0 || sequence > maximum) {
      throw new IllegalArgumentException(INVALID_SEQUENCE.formatted(maximum, sequence));
    }
  }
}
