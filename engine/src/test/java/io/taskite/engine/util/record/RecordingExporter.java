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

import io.taskite.protocol.impl.record.value.broker.*;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Per-engine recording with static queries for the current test scope. Instance streams and
 * snapshots always retain their explicit owner, including across nested scopes and executor tasks.
 */
public final class RecordingExporter implements AutoCloseable {
  private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);
  private static final ThreadLocal<Scope> CURRENT = new ThreadLocal<>();
  private final TypedEventRegistry registry;
  private final List<LoggedEvent> entries = new ArrayList<>();
  private boolean closed;
  private Throwable failure;
  private long generation;

  public RecordingExporter(final TypedEventRegistry registry) {
    this.registry = Objects.requireNonNull(registry, "registry");
  }

  /**
   * Binds an exporter on this thread until the scope closes. EngineRule binds automatically;
   * executor tasks can explicitly bind the exporter they were given.
   */
  public static Scope bind(final RecordingExporter exporter) {
    final var scope = new Scope(Objects.requireNonNull(exporter, "exporter"), currentScope());
    CURRENT.set(scope);
    return scope;
  }

  private static Scope currentScope() {
    var scope = CURRENT.get();
    while (scope != null && !scope.active) {
      scope = scope.previous;
    }
    if (scope == null) {
      CURRENT.remove();
    } else {
      CURRENT.set(scope);
    }
    return scope;
  }

  private static RecordingExporter current() {
    final var scope = currentScope();
    if (scope == null) {
      throw new IllegalStateException(
          "No RecordingExporter bound to this thread; start an EngineRule or bind an exporter");
    }
    return scope.exporter;
  }

  public static List<Record<?>> getRecords() {
    return current().snapshot();
  }

  public static void reset() {
    current().clear();
  }

  public static RecordStream<RecordValue, ?> records() {
    return current().recordStream();
  }

  public static <T extends RecordValue> RecordStream<T, ?> records(
      final ValueType type, final Class<T> valueClass) {
    return current().recordStream(type, valueClass);
  }

  public static JobRecordStream jobRecords() {
    return current().jobRecordStream();
  }

  public static JobRecordStream jobRecords(final Intent intent) {
    return jobRecords().withIntent(intent);
  }

  public static VariableRecordStream variableRecords() {
    return current().variableRecordStream();
  }

  public static VariableRecordStream variableRecords(final Intent intent) {
    return variableRecords().withIntent(intent);
  }

  public static IncidentRecordStream incidentRecords() {
    return current().incidentRecordStream();
  }

  public static IncidentRecordStream incidentRecords(final Intent intent) {
    return incidentRecords().withIntent(intent);
  }

  public static RecordStream<JobBatchRecord, ?> jobBatchRecords() {
    return current().jobBatchRecordStream();
  }

  public static RecordStream<JobBatchRecord, ?> jobBatchRecords(final Intent intent) {
    return jobBatchRecords().withIntent(intent);
  }

  public static RecordStream<VariableDocumentRecord, ?> variableDocumentRecords() {
    return current().variableDocumentRecordStream();
  }

  public static RecordStream<VariableDocumentRecord, ?> variableDocumentRecords(
      final Intent intent) {
    return variableDocumentRecords().withIntent(intent);
  }

  public static RecordStream<CommandDistributionRecord, ?> commandDistributionRecords() {
    return current().commandDistributionRecordStream();
  }

  public static RecordStream<CommandDistributionRecord, ?> commandDistributionRecords(
      final Intent intent) {
    return commandDistributionRecords().withIntent(intent);
  }

  /** Called with byte-backed, committed records in log order after processing has succeeded. */
  public synchronized void export(final List<LoggedEvent> batch) {
    if (closed) {
      return;
    }
    for (final var event : batch) {
      if (event.getPosition() <= 0
          || !entries.isEmpty() && event.getPosition() <= entries.getLast().getPosition()) {
        throw new IllegalArgumentException("Export requires committed records in position order");
      }
      entries.add(event);
    }
    notifyAll();
  }

  /** Decodes fresh values on every read, so a caller cannot mutate another stream's records. */
  public List<Record<?>> snapshot() {
    final List<LoggedEvent> copy;
    synchronized (this) {
      copy = List.copyOf(entries);
    }
    return copy.stream().map(this::decode).toList();
  }

  private Record<?> decode(final LoggedEvent event) {
    final var metadata = event.readMetadata(new RecordMetadata());
    final var record = new TypedRecordImpl(1);
    record.wrap(event, metadata, registry.read(event, metadata));
    return record;
  }

  /** Clears this engine's visible records without rewinding its state or export cursor. */
  public synchronized void clear() {
    entries.clear();
    generation++;
    notifyAll();
  }

  public RecordStream<RecordValue, ?> recordStream() {
    return new RecordStream<>(
        this,
        context -> read(context).map(RecordingExporter::<RecordValue>cast),
        DEFAULT_TIMEOUT,
        false);
  }

  public <T extends RecordValue> RecordStream<T, ?> recordStream(
      final ValueType type, final Class<T> valueClass) {
    return new RecordStream<>(this, source(type, valueClass), DEFAULT_TIMEOUT, false);
  }

  private <T extends RecordValue> Function<ReadContext, Stream<Record<T>>> source(
      final ValueType type, final Class<T> valueClass) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(valueClass, "valueClass");
    // Native rejection values are empty, so typed streams must not cast them to a business value.
    return context ->
        read(context)
            .filter(
                record -> record.getValueType() == type && valueClass.isInstance(record.getValue()))
            .map(RecordingExporter::<T>cast);
  }

  @SuppressWarnings("unchecked")
  private static <T extends RecordValue> Record<T> cast(final Record<?> record) {
    return (Record<T>) record;
  }

  public JobRecordStream jobRecordStream() {
    return new JobRecordStream(
        this, source(ValueType.JOB, JobRecord.class), DEFAULT_TIMEOUT, false);
  }

  public VariableRecordStream variableRecordStream() {
    return new VariableRecordStream(
        this, source(ValueType.VARIABLE, VariableRecord.class), DEFAULT_TIMEOUT, false);
  }

  public IncidentRecordStream incidentRecordStream() {
    return new IncidentRecordStream(
        this, source(ValueType.INCIDENT, IncidentRecord.class), DEFAULT_TIMEOUT, false);
  }

  public RecordStream<JobBatchRecord, ?> jobBatchRecordStream() {
    return recordStream(ValueType.JOB_BATCH, JobBatchRecord.class);
  }

  public RecordStream<VariableDocumentRecord, ?> variableDocumentRecordStream() {
    return recordStream(ValueType.VARIABLE_DOCUMENT, VariableDocumentRecord.class);
  }

  public RecordStream<CommandDistributionRecord, ?> commandDistributionRecordStream() {
    return recordStream(ValueType.COMMAND_DISTRIBUTION, CommandDistributionRecord.class);
  }

  synchronized ReadContext readContext(final Duration timeout) {
    validateTimeout(timeout);
    return new ReadContext(timeout, generation);
  }

  /**
   * A null context selects a finite snapshot; a live context owns one cursor and total deadline.
   */
  private Stream<Record<?>> read(final ReadContext context) {
    if (context == null) {
      return snapshot().stream();
    }
    return StreamSupport.stream(
        new Spliterators.AbstractSpliterator<Record<?>>(Long.MAX_VALUE, Spliterator.ORDERED) {
          @Override
          public boolean tryAdvance(final Consumer<? super Record<?>> action) {
            // Only entry lookup/waiting holds the monitor. Decoding and query callbacks do not.
            action.accept(decode(awaitEntry(context)));
            return true;
          }
        },
        false);
  }

  private synchronized LoggedEvent awaitEntry(final ReadContext context) {
    long remaining = context.remainingNanos();
    while (context.cursor >= entries.size()
        && context.generation == generation
        && failure == null
        && !closed
        && remaining > 0) {
      try {
        wait(remaining / 1_000_000, (int) (remaining % 1_000_000));
      } catch (final InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted awaiting engine records", error);
      }
      remaining = context.remainingNanos();
    }
    checkRead(context);
    return entries.get(context.cursor++);
  }

  synchronized void checkRead(final ReadContext context) {
    if (Thread.currentThread().isInterrupted()) {
      throw new IllegalStateException("Interrupted awaiting engine records");
    }
    if (failure != null) {
      throw new IllegalStateException("Test stream processor failed", failure);
    }
    if (context.generation != generation) {
      throw new IllegalStateException("RecordingExporter reset while awaiting records");
    }
    if (closed) {
      throw new IllegalStateException("Test engine closed while awaiting records");
    }
    if (context.remainingNanos() <= 0) {
      throw new AssertionError(
          "Timed out awaiting matching engine records within "
              + context.timeout
              + ", found "
              + context.matched
              + "; scanned="
              + context.cursor
              + "; exported="
              + entries.size());
    }
  }

  static void validateTimeout(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    timeout.toNanos();
  }

  public synchronized void onFailure(final Throwable error) {
    failure = Objects.requireNonNull(error, "error");
    notifyAll();
  }

  @Override
  public synchronized void close() {
    closed = true;
    notifyAll();
  }

  /**
   * A thread's query binding, independent of the exporter's records and lifecycle. Closing from
   * another thread also invalidates this binding for its original thread.
   */
  public static final class Scope implements AutoCloseable {
    private final RecordingExporter exporter;
    private final Scope previous;
    private volatile boolean active = true;

    private Scope(final RecordingExporter exporter, final Scope previous) {
      this.exporter = exporter;
      this.previous = previous;
    }

    @Override
    public void close() {
      active = false;
      currentScope();
    }
  }

  /** Mutable state belongs to one terminal query, never to the reusable RecordStream. */
  static final class ReadContext {
    long matched;
    private final Duration timeout;
    private final long started = System.nanoTime();
    private final long generation;
    private int cursor;

    private ReadContext(final Duration timeout, final long generation) {
      this.timeout = timeout;
      this.generation = generation;
    }

    private long remainingNanos() {
      return timeout.toNanos() - (System.nanoTime() - started);
    }
  }
}
