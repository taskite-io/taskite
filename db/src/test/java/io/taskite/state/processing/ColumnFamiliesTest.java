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
package io.taskite.state.processing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.db.impl.DbConstants;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.PartitionStorage;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ColumnFamiliesTest {

  @TempDir Path directory;

  @Test
  void shouldAssignUniqueOrderedValuesAcrossReservedRanges() {
    // given
    final var families = List.of(ColumnFamilies.values());

    // when
    final var values = families.stream().map(ColumnFamilies::getValue).toList();

    // then
    assertEquals(values.size(), new HashSet<>(values).size());
    for (int index = 1; index < values.size(); index++) {
      assertTrue(values.get(index - 1) < values.get(index));
    }

    assertEquals(
        List.of(
            1, 2, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215,
            216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 231, 232, 233, 234,
            235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 500, 502, 522, 540, 541, 542, 543,
            544, 545, 546, 547, 548, 549, 560, 562),
        values);
    assertTrue(
        families.stream()
            .filter(
                family ->
                    family.name().startsWith("PD_")
                        || family == ColumnFamilies.BROKER_TOPOLOGY_OPERATIONS
                        || family == ColumnFamilies.BROKER_RECOVERY_EPOCHS)
            .allMatch(family -> family.getValue() >= 200 && family.getValue() < 500));
    assertTrue(
        families.stream()
            .filter(family -> family.getValue() >= 500)
            .allMatch(family -> family.getValue() < 1000));
  }

  @Test
  void shouldPersistExplicitValueAsLogicalColumnFamilyPrefix() throws Exception {
    // given
    try (final var storage = new PartitionStorage(directory.resolve("runtime"))) {
      final var store = new StateStoreProcessingStore(storage);
      final var family =
          store.createColumnFamily(ColumnFamilies.JOBS, new DbLong(), new DbString());
      final var key = new DbLong();
      key.wrapLong(42);
      final var value = new DbString();
      value.wrapString("job");

      // when
      family.upsert(key, value);
      store.getCurrentTransaction().commit();

      // then
      try (final var iterator = storage.runtimeDb().newIterator(storage.defaultColumnFamily())) {
        iterator.seekToFirst();
        assertTrue(iterator.isValid());
        assertEquals(
            ColumnFamilies.JOBS.getValue(),
            new UnsafeBuffer(iterator.key()).getLong(0, DbConstants.DB_BYTE_ORDER));
      }
    }
  }
}
