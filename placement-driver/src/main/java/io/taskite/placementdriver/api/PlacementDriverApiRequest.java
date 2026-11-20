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
package io.taskite.placementdriver.api;

import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Objects;
import java.util.function.Supplier;
import org.agrona.concurrent.UnsafeBuffer;

final class PlacementDriverApiRequest<T extends UnifiedRecordValue> {
  private final ValueType valueType;
  private final Intent intent;
  private final UnifiedRecordValue value;
  private final Supplier<T> responseFactory;
  private final long key;

  PlacementDriverApiRequest(
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue value,
      final Supplier<T> responseFactory) {
    this(0, valueType, intent, value, responseFactory);
  }

  PlacementDriverApiRequest(
      final long key,
      final ValueType valueType,
      final Intent intent,
      final UnifiedRecordValue value,
      final Supplier<T> responseFactory) {
    this.key = key;
    this.valueType = Objects.requireNonNull(valueType, "valueType");
    this.intent = Objects.requireNonNull(intent, "intent");
    this.value = Objects.requireNonNull(value, "value");
    this.responseFactory = Objects.requireNonNull(responseFactory, "responseFactory");
  }

  long key() {
    return key;
  }

  ValueType valueType() {
    return valueType;
  }

  Intent intent() {
    return intent;
  }

  UnifiedRecordValue value() {
    return value;
  }

  UnifiedRecordValue getRequestWriter() {
    return value;
  }

  ValueType getValueType() {
    return valueType;
  }

  Intent getIntent() {
    return intent;
  }

  T mapResponse(final LoggedEvent record) {
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[record.value().getLength()]);
    record.value().write(buffer, 0);
    final T response = responseFactory.get();
    response.wrap(buffer, 0, buffer.capacity());
    return response;
  }
}
