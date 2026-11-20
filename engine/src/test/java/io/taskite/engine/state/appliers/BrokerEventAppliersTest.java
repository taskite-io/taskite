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
package io.taskite.engine.state.appliers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.taskite.engine.state.mutable.MutableProcessingState;
import io.taskite.protocol.record.intent.broker.JobIntent;
import io.taskite.protocol.record.intent.broker.VariableIntent;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BrokerEventAppliersTest {

  @TempDir Path tempDir;

  @Test
  void registersEveryJobAndVariableEvent() throws Exception {
    try (final var storage = new PartitionStorage(tempDir.resolve("runtime"))) {
      final var state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final var appliers = BrokerEventAppliers.create(state);

      assertTrue(
          Arrays.stream(JobIntent.values())
              .filter(JobIntent::isEvent)
              .allMatch(intent -> appliers.hasApplierFor(intent, 1)));
      assertTrue(
          Arrays.stream(VariableIntent.values())
              .allMatch(intent -> appliers.hasApplierFor(intent, 1)));
    }
  }
}
