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
package io.taskite.streamplatform.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.db.ColumnFamily;
import io.taskite.db.DbKey;
import io.taskite.db.DbTransaction;
import io.taskite.db.DbValue;
import io.taskite.db.KeyValuePairVisitor;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.Protocol;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.streamplatform.NoopTransaction;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

/**
 * Verifies the persisted key generator does not reset or collide across commands and restarts,
 * replacing the old transient {@code new AtomicLong(1)} that produced duplicate keys; that keys are
 * namespaced by partition so different partitions never collide; and that a long-lived
 * per-partition instance reads the high-water straight from the transactional column family on
 * every {@link PersistedKeyGenerator#nextKey()}, so a snapshot install that overwrites the
 * persisted value out from under a surviving generator is picked up automatically (read-your-writes
 * + committed-read semantics of the transactional store).
 */
class PersistedKeyGeneratorTest {

  @Test
  void generatesMonotonicKeysAndPersistsHighWater() {
    final InMemoryStore store = new InMemoryStore();

    final PersistedKeyGenerator generator = new PersistedKeyGenerator(0, 1, store);
    assertEquals(Protocol.encodePartitionId(1, 1), generator.nextKey());
    assertEquals(Protocol.encodePartitionId(1, 2), generator.nextKey());
    assertEquals(Protocol.encodePartitionId(1, 3), generator.nextKey());

    // The persisted high-water mark is the last issued key (3), not the next key to issue.
    assertEquals(Protocol.encodePartitionId(1, 3), store.latestKeyHighWater());
  }

  @Test
  void continuesFromPersistedValueAfterRestart() {
    final InMemoryStore store = new InMemoryStore();

    final PersistedKeyGenerator before = new PersistedKeyGenerator(0, 1, store);
    before.nextKey(); // encodePartitionId(1, 1)
    before.nextKey(); // encodePartitionId(1, 2)

    // Simulate a restart: a brand-new generator over the same persisted store must not reset to 1.
    final PersistedKeyGenerator after = new PersistedKeyGenerator(0, 1, store);
    assertEquals(Protocol.encodePartitionId(1, 3), after.nextKey());
    assertEquals(Protocol.encodePartitionId(1, 4), after.nextKey());
  }

  @Test
  void namespacesKeysByPartition() {
    // Partition keys carry the partition id in the high bits, so a key alone identifies its owning
    // partition.
    final PersistedKeyGenerator partitionOne = new PersistedKeyGenerator(0, 1, new InMemoryStore());
    final long firstKey = partitionOne.nextKey();

    assertEquals(Protocol.encodePartitionId(1, 1), firstKey);
    assertEquals(1, Protocol.decodePartitionId(firstKey));
    assertEquals(1, Protocol.decodeKeyInPartition(firstKey));
  }

  @Test
  void picksUpOverwrittenHighWaterFromCommittedState() {
    // A single long-lived instance (as owned by the stream processor) reads the high-water on
    // every nextKey(), so a snapshot install that overwrites the persisted LATEST_KEY out from
    // under it is picked up automatically on the next call — the generator does not rely on a
    // stale in-memory counter that could collide after snapshot install. The snapshot-installed
    // value represents the last-issued key, so the next issued key must be one higher.
    final InMemoryStore store = new InMemoryStore();
    final PersistedKeyGenerator generator = new PersistedKeyGenerator(0, 1, store);
    assertEquals(Protocol.encodePartitionId(1, 1), generator.nextKey());

    // Snapshot install: the persisted high-water is overwritten out from under us (modelled here
    // by writing directly through the same shared in-memory column family).
    store.overwriteLatestKeyHighWater(Protocol.encodePartitionId(1, 500));

    assertEquals(Protocol.encodePartitionId(1, 501), generator.nextKey());
    assertEquals(Protocol.encodePartitionId(1, 502), generator.nextKey());
  }

  @Test
  void setKeyIfHigherRecoversHighWaterSoNextKeyDoesNotCollide() {
    // Models ZEP004 "Key Generation" recovery: a follower replays *:CREATED events (whose keys were
    // issued by the old leader) after its snapshot, then is promoted. Recovering the high-water
    // from
    // those event keys means the first nextKey() after promotion is strictly greater than every
    // replayed key — no collision with an already-used key.
    final InMemoryStore store = new InMemoryStore();
    final PersistedKeyGenerator generator = new PersistedKeyGenerator(0, 1, store);

    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 7));
    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 42));
    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 15)); // out of order, must not lower it

    assertEquals(Protocol.encodePartitionId(1, 42), store.latestKeyHighWater());
    assertEquals(Protocol.encodePartitionId(1, 43), generator.nextKey());
  }

  @Test
  void setKeyIfHigherNeverLowersHighWater() {
    // A key at or below the current high-water leaves it untouched, so a snapshot install that
    // carried a higher mark is never rolled back by replaying an older event.
    final InMemoryStore store = new InMemoryStore();
    final PersistedKeyGenerator generator = new PersistedKeyGenerator(0, 1, store);
    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 100));

    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 50));
    generator.setKeyIfHigher(Protocol.encodePartitionId(1, 100));

    assertEquals(Protocol.encodePartitionId(1, 100), store.latestKeyHighWater());
    assertEquals(Protocol.encodePartitionId(1, 101), generator.nextKey());
  }

  @Test
  void setKeyIfHigherIgnoresKeysFromOtherPartitions() {
    // Keys are namespaced by partition; a key minted by another partition lives in a disjoint
    // numbering space and must not disturb this partition's high-water.
    final InMemoryStore store = new InMemoryStore();
    final PersistedKeyGenerator generator = new PersistedKeyGenerator(0, 1, store);

    generator.setKeyIfHigher(Protocol.encodeKey(0, 2, 999));

    assertEquals(Protocol.encodePartitionId(1, 1), generator.nextKey());
  }

  @Test
  void clusterPrefixesAreDistinctAndForeignClusterCannotAdvanceHighWater() {
    final var first = new PersistedKeyGenerator(0, 9, new InMemoryStore());
    final var second = new PersistedKeyGenerator(15, 9, new InMemoryStore());
    assertEquals(Protocol.encodeKey(0, 9, 1), first.nextKey());
    assertEquals(Protocol.encodeKey(15, 9, 1), second.nextKey());
    first.setKeyIfHigher(Protocol.encodeKey(15, 9, 100));
    assertEquals(Protocol.encodeKey(0, 9, 2), first.nextKey());
  }

  @Test
  void sequenceExhaustionCannotCarryIntoThePrefix() {
    final var store = new InMemoryStore();
    final var generator = new PersistedKeyGenerator(15, 511, store);
    generator.setKeyIfHigher(Long.MAX_VALUE - 1);
    assertEquals(Long.MAX_VALUE, generator.nextKey());
    assertThrows(IllegalStateException.class, generator::nextKey);
    assertEquals(Long.MAX_VALUE, store.latestKeyHighWater());
  }

  @Test
  void rejectsMismatchedPersistedHighWater() {
    final var store = new InMemoryStore();
    final var generator = new PersistedKeyGenerator(3, 7, store);
    generator.nextKey();
    store.overwriteLatestKeyHighWater(Protocol.encodeKey(2, 7, 50));
    assertThrows(IllegalStateException.class, generator::nextKey);
  }

  /**
   * Minimal in-memory {@link ProcessingStateStore} that honours the typed column-family contract
   * just enough to exercise the key generator: it maps each logical column family to a shared
   * {@link InMemoryColumnFamily} whose entries are keyed by the serialised bytes of the key and
   * which stores/loads values through {@link DbValue#write}/{@link DbKey#wrap}.
   */
  private static final class InMemoryStore implements ProcessingStateStore {
    private final Map<ColumnFamilies, InMemoryColumnFamily<?, ?>> families = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <KeyType extends DbKey, ValueType extends DbValue>
        ColumnFamily<KeyType, ValueType> createColumnFamily(
            final ColumnFamilies columnFamily,
            final KeyType keyInstance,
            final ValueType valueInstance) {
      return (ColumnFamily<KeyType, ValueType>)
          families.computeIfAbsent(
              columnFamily, cf -> new InMemoryColumnFamily<>(keyInstance, valueInstance));
    }

    long latestKeyHighWater() {
      @SuppressWarnings("unchecked")
      final InMemoryColumnFamily<DbString, DbLong> keyCf =
          (InMemoryColumnFamily<DbString, DbLong>) families.get(ColumnFamilies.KEY);
      final DbString k = new DbString();
      k.wrapString("keygen/latestKey");
      final DbLong v = keyCf.get(k);
      return v == null ? 0L : v.getValue();
    }

    void overwriteLatestKeyHighWater(final long value) {
      @SuppressWarnings("unchecked")
      final InMemoryColumnFamily<DbString, DbLong> keyCf =
          (InMemoryColumnFamily<DbString, DbLong>) families.get(ColumnFamilies.KEY);
      final DbString k = new DbString();
      k.wrapString("keygen/latestKey");
      final DbLong v = new DbLong();
      v.wrapLong(value);
      keyCf.upsert(k, v);
    }

    @Override
    public long getLastProcessedPosition() {
      return 0;
    }

    @Override
    public void markAsProcessed(final long position) {}

    @Override
    public DbTransaction getCurrentTransaction() {
      return NoopTransaction.INSTANCE;
    }

    @Override
    public void close() {}
  }

  /**
   * A minimal Map-backed ColumnFamily. Keys and values are (de)serialised through the shared {@code
   * keyInstance}/{@code valueInstance} passed at construction time — exactly like the real
   * transactional CF reuses scratch instances — so tests exercise the same wrap/write contract.
   */
  private static final class InMemoryColumnFamily<K extends DbKey, V extends DbValue>
      implements ColumnFamily<K, V> {
    private final K keyInstance;
    private final V valueInstance;
    private final Map<BufferKey, byte[]> entries = new HashMap<>();

    InMemoryColumnFamily(final K keyInstance, final V valueInstance) {
      this.keyInstance = keyInstance;
      this.valueInstance = valueInstance;
    }

    @Override
    public void insert(final K key, final V value) {
      upsert(key, value);
    }

    @Override
    public void update(final K key, final V value) {
      upsert(key, value);
    }

    @Override
    public void upsert(final K key, final V value) {
      entries.put(BufferKey.of(key), serializeValue(value));
    }

    @Override
    public V get(final K key) {
      final byte[] bytes = entries.get(BufferKey.of(key));
      if (bytes == null) {
        return null;
      }
      wrapValue(valueInstance, bytes);
      return valueInstance;
    }

    @Override
    public V get(final K key, final Supplier<V> valueSupplier) {
      final byte[] bytes = entries.get(BufferKey.of(key));
      if (bytes == null) {
        return null;
      }
      final V v = valueSupplier.get();
      wrapValue(v, bytes);
      return v;
    }

    @Override
    public void forEach(final Consumer<V> consumer) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void forEach(final BiConsumer<K, V> consumer) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void whileTrue(final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void whileTrue(final K startAtKey, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void whileEqualPrefix(final DbKey keyPrefix, final BiConsumer<K, V> visitor) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void whileEqualPrefix(final DbKey keyPrefix, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void whileEqualPrefix(
        final DbKey keyPrefix, final K startAtKey, final KeyValuePairVisitor<K, V> visitor) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    @Override
    public void deleteExisting(final K key) {
      entries.remove(BufferKey.of(key));
    }

    @Override
    public void deleteIfExists(final K key) {
      entries.remove(BufferKey.of(key));
    }

    @Override
    public boolean exists(final K key) {
      return entries.containsKey(BufferKey.of(key));
    }

    @Override
    public boolean isEmpty() {
      return entries.isEmpty();
    }

    @Override
    public long count() {
      return entries.size();
    }

    @Override
    public long countEqualPrefix(final DbKey prefix) {
      throw new UnsupportedOperationException("not used by PersistedKeyGenerator");
    }

    private static byte[] serializeValue(final DbValue value) {
      final byte[] out = new byte[value.getLength()];
      final MutableDirectBuffer buf = new UnsafeBuffer(out);
      value.write(buf, 0);
      return out;
    }

    private static void wrapValue(final DbValue value, final byte[] bytes) {
      final UnsafeBuffer buf = new UnsafeBuffer(bytes);
      value.wrap(buf, 0, bytes.length);
    }
  }

  /** A value-object key over a DbKey's serialized bytes, suitable for use as a HashMap key. */
  private static final class BufferKey {
    private final byte[] bytes;
    private final int hashCode;

    private BufferKey(final byte[] bytes) {
      this.bytes = bytes;
      int h = 1;
      for (final byte b : bytes) {
        h = 31 * h + b;
      }
      hashCode = h;
    }

    static BufferKey of(final DbKey key) {
      final byte[] out = new byte[key.getLength()];
      final MutableDirectBuffer buf = new UnsafeBuffer(out);
      key.write(buf, 0);
      return new BufferKey(out);
    }

    @Override
    public boolean equals(final Object o) {
      if (!(o instanceof BufferKey other)) {
        return false;
      }
      if (bytes.length != other.bytes.length) {
        return false;
      }
      for (int i = 0; i < bytes.length; i++) {
        if (bytes[i] != other.bytes[i]) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
