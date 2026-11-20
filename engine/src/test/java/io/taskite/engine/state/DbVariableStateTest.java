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
package io.taskite.engine.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.taskite.db.DbInconsistentException;
import io.taskite.engine.state.variable.DbVariableState;
import io.taskite.protocol.impl.record.value.broker.VariableRecord;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class DbVariableStateTest {

  @TempDir Path tempDir;

  private PartitionStorage storage;
  private StateStoreProcessingStore store;
  private DbVariableState state;

  @BeforeEach
  void setUp() {
    storage = new PartitionStorage(tempDir.resolve("runtime"));
    store = new StateStoreProcessingStore(storage);
    state = new DbVariableState(store);
    store.getCurrentTransaction();
  }

  @AfterEach
  void tearDown() throws Exception {
    store.getCurrentTransaction().rollback();
    storage.close();
  }

  @Test
  void separatesCreateAndUpdatePreconditions() {
    state.createVariable(11, variable(1, "amount", 1));

    assertThrows(
        DbInconsistentException.class, () -> state.createVariable(12, variable(1, "amount", 2)));

    state.updateVariable(11, variable(1, "amount", 2));
    assertEquals(2, state.getVariableLocal(1, "amount").value());

    assertThrows(
        DbInconsistentException.class, () -> state.updateVariable(13, variable(1, "missing", 1)));
  }

  @Test
  void isolatesVariablesByJobAndProjectsRequestedNames() {
    state.createVariable(11, variable(1, "root", 1));
    state.createVariable(12, variable(1, "amount", 10));
    state.createVariable(13, variable(2, "amount", 20));
    state.createVariable(14, variable(2, "local", List.of("x", "y")));
    assertEquals(Map.of("amount", 20, "local", List.of("x", "y")), state.getVariables(2));
    assertEquals(Map.of("amount", 20), state.getVariables(2, List.of("root", "amount")));
    assertEquals(Map.of("root", 1, "amount", 10), state.getVariables(1));
  }

  @Test
  void removesOnlyTheFinishedJobsVariables() {
    state.createVariable(10, variable(1, "root", 1));
    state.createVariable(11, variable(2, "result", 42));
    state.removeScope(2);
    assertEquals(Map.of(), state.getVariables(2));
    assertNull(state.getVariableLocal(2, "result"));
    assertEquals(Map.of("root", 1), state.getVariables(1));
  }

  @Test
  void rollsBackVariableAndScopeChangesTogether() throws Exception {
    state.createVariable(10, variable(1, "amount", 1));
    store.getCurrentTransaction().commit();

    store.getCurrentTransaction();
    state.updateVariable(10, variable(1, "amount", 2));
    state.createVariable(11, variable(2, "local", true));
    state.removeScope(2);
    store.getCurrentTransaction().rollback();

    assertEquals(1, state.getVariableLocal(1, "amount").value());
    assertEquals(Map.of(), state.getVariables(2));
    assertNull(state.getVariableLocal(2, "local"));
  }

  private static VariableRecord variable(
      final long scopeKey, final String name, final Object value) {
    return new VariableRecord().setJobKey(scopeKey).setName(name).setValue(value);
  }
}
