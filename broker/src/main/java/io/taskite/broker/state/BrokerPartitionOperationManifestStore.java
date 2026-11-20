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

import io.taskite.broker.partition.BrokerPartitionOperation;
import io.taskite.broker.partition.BrokerPartitionOperationManifest;
import io.taskite.broker.partition.BrokerPartitionOperationStatus;
import io.taskite.db.impl.DbConstants;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.encoding.MsgPackConverter;
import io.taskite.state.PartitionStorage;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/** Durable per-partition operation dedupe and last-result state. */
public final class BrokerPartitionOperationManifestStore {
  private static final String LATEST_KEY = "__latest__";

  private final PartitionStorage storage;

  public BrokerPartitionOperationManifestStore(final PartitionStorage storage) {
    this.storage = storage;
  }

  public synchronized Optional<BrokerPartitionOperationManifest> find(final String opId) {
    return get(opId);
  }

  public synchronized Optional<BrokerPartitionOperationManifest> latest() {
    return get(LATEST_KEY);
  }

  /** Persisted before the first Raft mutation; retry authority belongs only to this pinned op. */
  public synchronized void authorizeConfigurations(
      final BrokerPartitionOperation request, final List<String> digests) {
    pinRequest(request);
    synchronized (storage) {
      try {
        final byte[] phaseKey = key("__configurations__:" + request.opId());
        if (storage.runtimeDb().get(storage.defaultColumnFamily(), phaseKey) == null) {
          storage
              .runtimeDb()
              .put(
                  storage.defaultColumnFamily(),
                  phaseKey,
                  MsgPackConverter.convertToMsgPack(digests));
          storage.runtimeDb().flushWal(true);
        }
      } catch (final RocksDBException error) {
        throw new IllegalStateException(
            "Cannot persist membership stages: " + request.opId(), error);
      }
    }
  }

  public synchronized boolean canResume(
      final BrokerPartitionOperation request, final String digest) {
    synchronized (storage) {
      final var last = latest();
      if (last.isEmpty()
          || !request.opId().equals(last.get().opId())
          || last.get().status() == BrokerPartitionOperationStatus.APPLIED) {
        return false;
      }
      try {
        final byte[] pinned =
            storage
                .runtimeDb()
                .get(storage.defaultColumnFamily(), key("__request__:" + request.opId()));
        final byte[] phases =
            storage
                .runtimeDb()
                .get(storage.defaultColumnFamily(), key("__configurations__:" + request.opId()));
        return Arrays.equals(pinned, encodeRequest(request))
            && phases != null
            && MsgPackConverter.convertToObject(List.class, phases).contains(digest);
      } catch (final RocksDBException error) {
        throw new IllegalStateException("Cannot read membership stages: " + request.opId(), error);
      }
    }
  }

  /** Pins the entire request before execution, including its source version and fencing proof. */
  public synchronized void pinRequest(final BrokerPartitionOperation request) {
    synchronized (storage) {
      final byte[] requestKey = key("__request__:" + request.opId());
      final byte[] encoded = encodeRequest(request);
      try {
        final byte[] existing = storage.runtimeDb().get(storage.defaultColumnFamily(), requestKey);
        if (existing != null && !Arrays.equals(existing, encoded)) {
          throw new IllegalStateException("OPERATION_REQUEST_CHANGED: " + request.opId());
        }
        if (existing == null) {
          storage.runtimeDb().put(storage.defaultColumnFamily(), requestKey, encoded);
          storage.runtimeDb().flushWal(true);
        }
      } catch (final RocksDBException error) {
        throw new IllegalStateException(
            "failed to pin topology operation " + request.opId(), error);
      }
    }
  }

  public synchronized void put(final BrokerPartitionOperationManifest manifest) {
    synchronized (storage) {
      if (storage.runtimeDb().isClosed()) {
        throw new IllegalStateException("partition storage is closed");
      }
      try (final var batch = new WriteBatch();
          final var options = new WriteOptions().setSync(true)) {
        final byte[] encoded = encode(manifest);
        batch.put(storage.defaultColumnFamily(), key(manifest.opId()), encoded);
        batch.put(storage.defaultColumnFamily(), key(LATEST_KEY), encoded);
        storage.runtimeDb().write(options, batch);
      } catch (final RocksDBException error) {
        throw new IllegalStateException(
            "failed to persist topology operation " + manifest.opId(), error);
      }
    }
  }

  private Optional<BrokerPartitionOperationManifest> get(final String manifestKey) {
    synchronized (storage) {
      if (storage.runtimeDb().isClosed()) {
        return Optional.empty();
      }
      try {
        final byte[] stored =
            storage.runtimeDb().get(storage.defaultColumnFamily(), key(manifestKey));
        return stored == null ? Optional.empty() : Optional.of(decode(stored));
      } catch (final RocksDBException error) {
        throw new IllegalStateException("failed to read topology operation " + manifestKey, error);
      }
    }
  }

  private static byte[] encodeRequest(final BrokerPartitionOperation request) {
    return MsgPackConverter.convertToMsgPack(request);
  }

  private static byte[] key(final String manifestKey) {
    final byte[] stringBytes = manifestKey.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.allocate(Long.BYTES + Integer.BYTES + stringBytes.length)
        .order(DbConstants.DB_BYTE_ORDER)
        .putLong(ColumnFamilies.BROKER_TOPOLOGY_OPERATIONS.getValue())
        .putInt(stringBytes.length)
        .put(stringBytes)
        .array();
  }

  private static byte[] encode(final BrokerPartitionOperationManifest manifest) {
    return MsgPackConverter.convertToMsgPack(manifest);
  }

  private static BrokerPartitionOperationManifest decode(final byte[] encoded) {
    return MsgPackConverter.convertToObject(BrokerPartitionOperationManifest.class, encoded);
  }
}
