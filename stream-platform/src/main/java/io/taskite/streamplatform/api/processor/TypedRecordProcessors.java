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
package io.taskite.streamplatform.api.processor;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class TypedRecordProcessors {
  public static final int DEFAULT_RECORD_VERSION = 1;

  private final Table<ValueType, Intent, NavigableMap<Integer, TypedRecordProcessor<?>>>
      commandProcessors = HashBasedTable.create();
  private final List<StreamProcessorLifecycleAware> lifecycleListeners = new ArrayList<>();

  private TypedRecordProcessors() {}

  public static TypedRecordProcessors processors() {
    return new TypedRecordProcessors();
  }

  public TypedRecordProcessors withLifecycleListener(final StreamProcessorLifecycleAware listener) {
    lifecycleListeners.add(Objects.requireNonNull(listener, "listener"));
    return this;
  }

  public List<StreamProcessorLifecycleAware> lifecycleListeners() {
    return List.copyOf(lifecycleListeners);
  }

  /**
   * Registers a processor for {@code (valueType, intent)} at the default record version. Equivalent
   * to {@link #onCommand(ValueType, Intent, int, TypedRecordProcessor)} with {@link
   * #DEFAULT_RECORD_VERSION}.
   */
  public TypedRecordProcessors onCommand(
      final ValueType valueType, final Intent intent, final TypedRecordProcessor<?> processor) {
    return onCommand(valueType, intent, DEFAULT_RECORD_VERSION, processor);
  }

  /**
   * Registers a processor for {@code (valueType, intent)} at a specific record version, so multiple
   * versions of the same command's logic can coexist across a rolling upgrade (ZEP004 "Record
   * Versioning").
   */
  public TypedRecordProcessors onCommand(
      final ValueType valueType,
      final Intent intent,
      final int version,
      final TypedRecordProcessor<?> processor) {
    Objects.requireNonNull(valueType, "valueType");
    Objects.requireNonNull(intent, "intent");
    Objects.requireNonNull(processor, "processor");
    if (intent.isEvent()) {
      throw new IllegalArgumentException("Only command intents can be registered");
    }
    if (version < 1) {
      throw new IllegalArgumentException("Version must be greater than 0");
    }

    NavigableMap<Integer, TypedRecordProcessor<?>> byVersion =
        commandProcessors.get(valueType, intent);
    if (byVersion == null) {
      byVersion = new TreeMap<>();
      commandProcessors.put(valueType, intent, byVersion);
    }
    if (byVersion.putIfAbsent(version, processor) != null) {
      throw new IllegalArgumentException(
          "Command processor already registered for "
              + valueType
              + '/'
              + intent
              + " version "
              + version);
    }
    return this;
  }

  public TypedRecordProcessor<?> get(
      final ValueType valueType, final Intent intent, final int version) {
    final NavigableMap<Integer, TypedRecordProcessor<?>> byVersion =
        commandProcessors.get(valueType, intent);
    return byVersion == null ? null : byVersion.get(version);
  }

  public int getLatestVersion(final ValueType valueType, final Intent intent) {
    final NavigableMap<Integer, TypedRecordProcessor<?>> byVersion =
        commandProcessors.get(valueType, intent);
    return byVersion == null ? -1 : byVersion.lastKey();
  }
}
