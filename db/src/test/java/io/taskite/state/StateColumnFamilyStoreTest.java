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
package io.taskite.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StateColumnFamilyStoreTest {
  @TempDir Path tempDir;

  @Test
  void writesReadsAndSnapshotsBusinessState() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      StateColumnFamilyStore store = new StateColumnFamilyStore(storage);
      store.put("a", "1");
      store.put("b", "2");

      assertEquals("1", store.get("a").orElse(null));
      assertTrue(store.exists("b"));
      assertEquals(Map.of("a", "1", "b", "2"), store.snapshot());
    }
  }

  @Test
  void restoreReplacesLiveState() {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      StateColumnFamilyStore store = new StateColumnFamilyStore(storage);
      store.put("a", "1");
      store.put("b", "2");

      store.restore(Map.of("c", "3"));

      assertEquals(Map.of("c", "3"), store.snapshot());
    }
  }

  @Test
  void batchCommitIsAtomic() throws Exception {
    try (PartitionStorage storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      StateColumnFamilyStore store = new StateColumnFamilyStore(storage);
      try (StateBatch batch = store.newBatch()) {
        batch.put("a", "1");
        batch.put("b", "2");
        batch.commit();
      }
      assertEquals(Map.of("a", "1", "b", "2"), store.snapshot());
    }
  }

  @Test
  void businessStateSurvivesReopen() {
    Path runtime = tempDir.resolve("runtime");

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      StateColumnFamilyStore store = new StateColumnFamilyStore(storage);
      store.put("a", "1");
    }

    try (PartitionStorage storage = new PartitionStorage(runtime)) {
      StateColumnFamilyStore reopened = new StateColumnFamilyStore(storage);
      assertEquals(Map.of("a", "1"), reopened.snapshot());
    }
  }
}
