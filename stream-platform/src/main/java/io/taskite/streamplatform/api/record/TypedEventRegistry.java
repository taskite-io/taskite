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
package io.taskite.streamplatform.api.record;

import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/** Immutable registry for constructing and decoding typed stream record values. */
public final class TypedEventRegistry {
  private final Map<ValueType, Supplier<? extends UnifiedRecordValue>> defaults;
  private final Map<RegistrationKey, Supplier<? extends UnifiedRecordValue>> exact;
  private final Supplier<? extends UnifiedRecordValue> fallback;

  private TypedEventRegistry(final Builder builder) {
    defaults = Collections.unmodifiableMap(new EnumMap<>(builder.defaults));
    exact = Collections.unmodifiableMap(new HashMap<>(builder.exact));
    fallback = builder.fallback;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static TypedEventRegistry unifiedRecordValue() {
    return builder().fallback(UnifiedRecordValue::new).build();
  }

  public UnifiedRecordValue create(final ValueType valueType, final Intent intent) {
    final Supplier<? extends UnifiedRecordValue> exactFactory =
        exact.get(new RegistrationKey(valueType, intent));
    if (exactFactory != null) {
      return exactFactory.get();
    }
    final Supplier<? extends UnifiedRecordValue> defaultFactory = defaults.get(valueType);
    return (defaultFactory == null ? fallback : defaultFactory).get();
  }

  public UnifiedRecordValue read(final LoggedEvent event, final RecordMetadata metadata) {
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(metadata, "metadata");
    if (metadata.getRecordType() == RecordType.COMMAND_REJECTION) {
      return new EmptyRecordValue();
    }
    final UnifiedRecordValue value = create(metadata.getValueType(), metadata.getIntent());
    value.wrap(event.valueBuffer(), event.valueOffset(), event.valueLength());
    return value;
  }

  public static final class Builder {
    private final EnumMap<ValueType, Supplier<? extends UnifiedRecordValue>> defaults =
        new EnumMap<>(ValueType.class);
    private final Map<RegistrationKey, Supplier<? extends UnifiedRecordValue>> exact =
        new HashMap<>();
    private Supplier<? extends UnifiedRecordValue> fallback = UnifiedRecordValue::new;

    public Builder fallback(final Supplier<? extends UnifiedRecordValue> fallback) {
      this.fallback = Objects.requireNonNull(fallback, "fallback");
      return this;
    }

    public Builder register(
        final ValueType valueType, final Supplier<? extends UnifiedRecordValue> factory) {
      Objects.requireNonNull(valueType, "valueType");
      Objects.requireNonNull(factory, "factory");
      if (defaults.putIfAbsent(valueType, factory) != null) {
        throw new IllegalArgumentException("Record value already registered for " + valueType);
      }
      return this;
    }

    public Builder register(
        final ValueType valueType,
        final Intent intent,
        final Supplier<? extends UnifiedRecordValue> factory) {
      Objects.requireNonNull(valueType, "valueType");
      Objects.requireNonNull(intent, "intent");
      Objects.requireNonNull(factory, "factory");
      final RegistrationKey key = new RegistrationKey(valueType, intent);
      if (exact.putIfAbsent(key, factory) != null) {
        throw new IllegalArgumentException(
            "Record value already registered for " + valueType + '/' + intent);
      }
      return this;
    }

    public TypedEventRegistry build() {
      return new TypedEventRegistry(this);
    }
  }

  private record RegistrationKey(ValueType valueType, Intent intent) {}

  private static final class EmptyRecordValue extends UnifiedRecordValue {
    @Override
    public int getLength() {
      return 0;
    }

    @Override
    public void write(final MutableDirectBuffer buffer, final int offset) {}

    @Override
    public void wrap(final DirectBuffer buffer, final int offset, final int length) {}

    @Override
    public String toJson() {
      return "{}";
    }
  }
}
