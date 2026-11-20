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

import io.taskite.protocol.Protocol;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerKeyTransactionTest {
  @Test
  void rollbackAndRestartPreserveTheCommittedNamespace(@TempDir final Path directory)
      throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var generator = new PersistedKeyGenerator(7, 511, store);
      store.getCurrentTransaction();
      assertEquals(Protocol.encodeKey(7, 511, 1), generator.nextKey());
      assertEquals(Protocol.encodeKey(7, 511, 2), generator.nextKey());
      store.getCurrentTransaction().rollback();
      store.getCurrentTransaction();
      assertEquals(Protocol.encodeKey(7, 511, 1), generator.nextKey());
      store.getCurrentTransaction().commit();
      store.getCurrentTransaction();
      generator.setKeyIfHigher(Protocol.encodeKey(7, 511, 105));
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      final var recovered = new PersistedKeyGenerator(7, 511, store);
      store.getCurrentTransaction();
      assertEquals(Protocol.encodeKey(7, 511, 106), recovered.nextKey());
      store.getCurrentTransaction().rollback();
      assertThrows(
          IllegalStateException.class, () -> new PersistedKeyGenerator(6, 511, store).nextKey());
    }
  }

  @Test
  void emptyNamespaceSurvivesRestart(@TempDir final Path directory) throws Exception {
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      store.getCurrentTransaction();
      store.markAsProcessed(1);
      store.getCurrentTransaction().commit();
    }
    try (var storage = new PartitionStorage(directory)) {
      final var store = new StateStoreProcessingStore(storage);
      assertEquals(Protocol.encodeKey(0, 9, 1), new PersistedKeyGenerator(0, 9, store).nextKey());
    }
  }
}
