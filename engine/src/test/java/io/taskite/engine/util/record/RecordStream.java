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
package io.taskite.engine.util.record;

import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.RecordValue;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Repeatable queries: unrestricted reads are snapshots; explicitly limited reads await records. */
public class RecordStream<T extends RecordValue, S extends RecordStream<T, S>>
    implements Iterable<Record<T>> {
  protected final RecordingExporter exporter;
  protected final Function<RecordingExporter.ReadContext, Stream<Record<T>>> source;
  protected final Duration timeout;
  protected final boolean bounded;

  RecordStream(
      final RecordingExporter exporter,
      final Function<RecordingExporter.ReadContext, Stream<Record<T>>> source,
      final Duration timeout,
      final boolean bounded) {
    this.exporter = exporter;
    this.source = source;
    this.timeout = timeout;
    this.bounded = bounded;
  }

  protected S create(
      final Function<RecordingExporter.ReadContext, Stream<Record<T>>> next,
      final Duration wait,
      final boolean bounded) {
    @SuppressWarnings("unchecked")
    final S recordStream = (S) new RecordStream<>(exporter, next, wait, bounded);
    return recordStream;
  }

  public S filter(final Predicate<Record<T>> predicate) {
    Objects.requireNonNull(predicate, "predicate");
    return create(
        context ->
            source
                .apply(context)
                .filter(
                    record -> {
                      final boolean matches = predicate.test(record);
                      if (context != null) {
                        exporter.checkRead(context);
                      }
                      return matches;
                    }),
        timeout,
        bounded);
  }

  public S valueFilter(final Predicate<T> predicate) {
    Objects.requireNonNull(predicate, "predicate");
    return filter(record -> predicate.test(record.getValue()));
  }

  public S withIntent(final Intent intent) {
    return filter(record -> record.getIntent() == intent);
  }

  public S withIntents(final Intent... intents) {
    final var selected = List.copyOf(Arrays.asList(intents));
    return filter(record -> selected.contains(record.getIntent()));
  }

  public S withKey(final long key) {
    return filter(record -> record.getKey() == key);
  }

  public S withPosition(final long position) {
    return filter(record -> record.getPosition() == position);
  }

  public S withRequestId(final String requestId) {
    return filter(
        record ->
            record instanceof final TypedRecord<?> typed
                && Objects.equals(typed.getRequestId(), requestId));
  }

  public S withSourceEventPosition(final long position) {
    return filter(
        record ->
            record instanceof final TypedRecordImpl typed
                && typed.getSourceEventPosition() == position);
  }

  public S withTimestamp(final long timestamp) {
    return filter(record -> record.getTimestamp() == timestamp);
  }

  public S withPartitionId(final int partitionId) {
    return filter(record -> record.getPartitionId() == partitionId);
  }

  public S withValueType(final ValueType type) {
    return filter(record -> record.getValueType() == type);
  }

  public S withRecordType(final RecordType type) {
    return filter(record -> record.getRecordType() == type);
  }

  public S onlyCommands() {
    return withRecordType(RecordType.COMMAND);
  }

  public S onlyEvents() {
    return withRecordType(RecordType.EVENT);
  }

  public S onlyCommandRejections() {
    return withRecordType(RecordType.COMMAND_REJECTION);
  }

  public S withRejectionType(final RejectionType type) {
    return filter(record -> record.getRejectionType() == type);
  }

  public S withRejectionReason(final String reason) {
    return filter(record -> Objects.equals(record.getRejectionReason(), reason));
  }

  public S limit(final long count) {
    if (count < 0) {
      throw new IllegalArgumentException("limit must not be negative");
    }
    return create(context -> source.apply(context).limit(count), timeout, true);
  }

  /** Consumes through the first matching record, including it, before any downstream filters. */
  public S limit(final Predicate<Record<T>> stop) {
    Objects.requireNonNull(stop, "stop");
    return create(
        context -> {
          final var upstream = source.apply(context);
          final var iterator = upstream.iterator();
          return StreamSupport.stream(
                  new Spliterators.AbstractSpliterator<Record<T>>(
                      Long.MAX_VALUE, Spliterator.ORDERED) {
                    private boolean finished;

                    @Override
                    public boolean tryAdvance(final Consumer<? super Record<T>> action) {
                      if (finished || !iterator.hasNext()) {
                        return false;
                      }
                      final var record = iterator.next();
                      finished = stop.test(record);
                      if (context != null) {
                        exporter.checkRead(context);
                      }
                      action.accept(record);
                      return true;
                    }
                  },
                  false)
              .onClose(upstream::close);
        },
        timeout,
        true);
  }

  public S withTimeout(final Duration wait) {
    RecordingExporter.validateTimeout(wait);
    return create(source, wait, bounded);
  }

  /** Fresh snapshot unless limit(count/predicate) selected a live, bounded query. */
  public Stream<Record<T>> stream() {
    return bounded ? liveStream(timeout) : source.apply(null);
  }

  private Stream<Record<T>> liveStream(final Duration wait) {
    final var context = exporter.readContext(wait);
    return source
        .apply(context)
        .peek(
            record -> {
              exporter.checkRead(context);
              context.matched++;
            });
  }

  public List<Record<T>> asList() {
    try (final var records = stream()) {
      return records.toList();
    }
  }

  public long count() {
    try (final var records = source.apply(null)) {
      return records.count();
    }
  }

  public boolean exists() {
    try (final var records = source.apply(null)) {
      return records.findAny().isPresent();
    }
  }

  public Record<T> getFirst() {
    try (final var records = liveStream(timeout)) {
      return records
          .findFirst()
          .orElseThrow(() -> new AssertionError("Expected a matching record, found 0"));
    }
  }

  /** Consumes bounded queries to their boundary; otherwise returns the current last match. */
  public Record<T> getLast() {
    if (bounded) {
      return asList().getLast();
    }
    final var first = getFirst();
    final var snapshot = asList();
    return snapshot.isEmpty() ? first : snapshot.getLast();
  }

  public List<Record<T>> awaitRecords(final int minimumCount, final Duration wait) {
    if (minimumCount < 1) {
      throw new IllegalArgumentException("minimumCount must be positive");
    }
    try (final var records = liveStream(wait)) {
      final var iterator = records.iterator();
      final var result = new ArrayList<Record<T>>();
      while (result.size() < minimumCount && iterator.hasNext()) {
        result.add(iterator.next());
      }
      if (result.size() < minimumCount) {
        throw new AssertionError(
            "Expected at least " + minimumCount + " matching records, found " + result.size());
      }
      return List.copyOf(result);
    }
  }

  @Override
  public Iterator<Record<T>> iterator() {
    return stream().iterator();
  }
}
