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
package io.taskite.streamplatform.api.state;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.taskite.protocol.record.RecordValue;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.intent.Intent;
import java.util.Comparator;
import java.util.Objects;

public final class EventAppliers {
  public static final int DEFAULT_EVENT_VERSION = 1;

  public static final TypedEventApplier<Intent, RecordValue> NOOP_EVENT_APPLIER =
      (key, value) -> {};

  private final Table<Intent, Integer, TypedEventApplier> mapping = HashBasedTable.create();

  public <I extends Intent> EventAppliers register(
      final I intent, final TypedEventApplier<I, ?> applier) {
    return register(intent, DEFAULT_EVENT_VERSION, applier);
  }

  public <I extends Intent> EventAppliers register(
      final I intent, final int version, final TypedEventApplier<I, ?> applier) {
    Objects.requireNonNull(intent, "intent");
    Objects.requireNonNull(applier, "applier");
    if (version < 1) {
      throw new IllegalArgumentException("Version must be greater than 0");
    }
    if (!intent.isEvent()) {
      throw new IllegalArgumentException("Only event intents can be registered");
    }

    final var previous = mapping.put(intent, version, applier);
    if (previous != null) {
      throw new IllegalArgumentException(
          "Applier already registered for intent " + intent + " and version " + version);
    }

    return this;
  }

  public boolean hasApplierFor(final Intent intent, final int version) {
    return mapping.contains(intent, version);
  }

  public int getLatestVersion(final Intent intent) {
    return mapping.row(intent).keySet().stream().max(Comparator.naturalOrder()).orElse(-1);
  }

  public void applyState(
      final long key,
      final Intent intent,
      final UnifiedRecordValue value,
      final int recordVersion) {
    final var applier = mapping.get(intent, recordVersion);
    if (applier == null) {
      throw new IllegalStateException(
          "No event applier registered for intent "
              + intent
              + " and version "
              + recordVersion
              + ", latest version is "
              + getLatestVersion(intent));
    }

    @SuppressWarnings("unchecked")
    final TypedEventApplier<Intent, RecordValue> typedApplier =
        (TypedEventApplier<Intent, RecordValue>) applier;
    typedApplier.applyState(key, value);
  }
}
