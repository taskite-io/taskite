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
package io.taskite.engine.state.variable;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.*;
import io.taskite.engine.state.mutable.MutableVariableState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.broker.VariableRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public final class DbVariableState implements MutableVariableState {
  private final DbLong jobKey = new DbLong();
  private final DbString variableName = new DbString();
  private final DbCompositeKey<DbLong, DbString> jobKeyAndVariableName =
      new DbCompositeKey<>(jobKey, variableName);
  private final VariableRecordValue value = new VariableRecordValue();
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, VariableRecordValue>
      variablesColumnFamilies;

  public DbVariableState(final ProcessingStateStore storage) {
    variablesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.VARIABLES, jobKeyAndVariableName, new VariableRecordValue());
  }

  private void wrapKey(final long key, final String name) {
    jobKey.wrapLong(key);
    variableName.wrapString(name);
  }

  @Override
  public Variable getVariableLocal(final long key, final String name) {
    wrapKey(key, name);
    final var stored = variablesColumnFamilies.get(jobKeyAndVariableName);
    return stored == null ? null : new Variable(stored.getKey(), stored.getValue());
  }

  @Override
  public Object getVariable(final long key, final String name) {
    final var found = getVariableLocal(key, name);
    return found == null ? null : found.value();
  }

  @Override
  public Map<String, Object> getVariables(final long key) {
    final Map<String, Object> result = new LinkedHashMap<>();
    jobKey.wrapLong(key);
    variablesColumnFamilies.whileEqualPrefix(
        jobKey,
        (scopeKey, value) -> {
          result.put(scopeKey.second().toString(), value.getValue());
        });
    return result;
  }

  @Override
  public Map<String, Object> getVariables(final long id, final Collection<String> names) {
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final var name : names) {
      final var found = getVariableLocal(id, name);
      if (found != null) {
        result.put(name, found.value());
      }
    }
    return result;
  }

  @Override
  public void createVariable(final long variableKey, final VariableRecord record) {
    wrapKey(record.getJobKey(), record.getName());
    value.set(variableKey, record.getValue());
    variablesColumnFamilies.insert(jobKeyAndVariableName, value);
  }

  @Override
  public void updateVariable(final long variableKey, final VariableRecord record) {
    wrapKey(record.getJobKey(), record.getName());
    value.set(variableKey, record.getValue());
    variablesColumnFamilies.update(jobKeyAndVariableName, value);
  }

  @Override
  public void removeScope(final long key) {
    jobKey.wrapLong(key);
    variablesColumnFamilies.whileEqualPrefix(
        jobKey,
        (scopeKey, ignored) -> {
          variablesColumnFamilies.deleteExisting(scopeKey);
        });
  }
}
