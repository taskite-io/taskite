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
package io.taskite.engine.util.runtime;

import io.taskite.engine.Engine;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.protocol.Protocol;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionDeletionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.JobSubscriptionRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.RecordMetadata;
import io.taskite.protocol.record.RecordType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import io.taskite.protocol.record.intent.placementdriver.CommandDistributionIntent;
import io.taskite.scheduler.ActorScheduler;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.metrics.ProcessingMetrics;
import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.api.record.TypedEventRegistry;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LoggedEvent;
import io.taskite.streamplatform.processing.StreamProcessor;
import io.taskite.streamplatform.processing.StreamProcessorContext;
import io.taskite.streamplatform.processing.StreamProcessorMode;
import io.taskite.streamplatform.record.TypedRecordImpl;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Isolated storage and the production single-partition Processor/Applier chain. */
public final class EngineTestRuntime implements AutoCloseable {
  private static final String CLUSTER_ID = "taskite-engine-test";
  private final TypedEventRegistry registry = Engine.eventRegistry();
  private final RecordingExporter recordingExporter = new RecordingExporter(registry);
  private final AtomicLong consumedPosition = new AtomicLong();
  private final AtomicLong subscriptionKeys = new AtomicLong();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final Map<Long, String> subscriptionTenants = new ConcurrentHashMap<>();
  private volatile Lifecycle lifecycle = Lifecycle.NEW;
  private long exportedPosition;
  private Path directory;
  private PartitionStorage storage;
  private ActorScheduler scheduler;
  private StreamProcessor processor;
  private InMemoryJournal journal;
  private final boolean workerJobLeaseTokenEnabled;

  private EngineTestRuntime(final boolean workerJobLeaseTokenEnabled) {
    this.workerJobLeaseTokenEnabled = workerJobLeaseTokenEnabled;
  }

  public static EngineTestRuntime create() {
    return create(false);
  }

  public static EngineTestRuntime create(final boolean workerJobLeaseTokenEnabled) {
    return new EngineTestRuntime(workerJobLeaseTokenEnabled);
  }

  public synchronized void start() {
    if (lifecycle == Lifecycle.RUNNING) {
      return;
    }
    if (lifecycle == Lifecycle.CLOSED) {
      throw new IllegalStateException("A closed test runtime cannot be restarted");
    }
    try {
      directory = Files.createTempDirectory("taskite-engine-test-");
      storage = new PartitionStorage(directory.resolve("runtime"));
      final var state = new StateStoreProcessingStore(storage);
      journal = new InMemoryJournal();
      processor =
          new StreamProcessor(
              new StreamProcessorContext(
                  new StreamProcessorContext.Configuration(
                      new StreamProcessorContext.Identity(
                          0,
                          1,
                          CLUSTER_ID + "-" + UUID.randomUUID(),
                          StreamProcessorMode.PROCESSING),
                      new StreamProcessorContext.Batching(1, 0, Duration.ZERO, 16 * 1024 * 1024)),
                  new StreamProcessorContext.Processing(
                      registry,
                      Engine::createProcessors,
                      (partitionId, store, keys, writers) ->
                          Engine.createProcessorContext(
                              partitionId, store, keys, writers, workerJobLeaseTokenEnabled)),
                  new StreamProcessorContext.State(
                      state, journal::committedPosition, ignored -> {}),
                  new StreamProcessorContext.Log(journal, journal),
                  new StreamProcessorContext.Observers(
                      (requestId, error) -> failEngine(error),
                      ignored -> {},
                      new CommittedRecordObserver() {
                        @Override
                        public void onProcessed(final LoggedEvent event) {}

                        @Override
                        public void onFailure(final LoggedEvent event, final Throwable error) {
                          failEngine(error);
                        }
                      },
                      (partitionId, context) ->
                          new ProcessingMetrics() {
                            @Override
                            public void onRecordPositions(
                                final long processed, final long committed) {
                              if (processed > exportedPosition) {
                                recordingExporter.export(
                                    journal.between(exportedPosition, processed));
                                exportedPosition = processed;
                              }
                              consumedPosition.accumulateAndGet(Math.max(0, processed), Math::max);
                            }
                          })));
      journal.onAppend(processor::onRecordAvailable);
      scheduler =
          ActorScheduler.newActorScheduler()
              .setCpuBoundActorThreadCount(1)
              .setIoBoundActorThreadCount(1)
              .build();
      scheduler.start();
      scheduler.submitActor(processor).get(10, TimeUnit.SECONDS);
      final long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (!processor.isRecovered()) {
        checkFailure();
        if (System.nanoTime() >= deadline) {
          throw new TimeoutException("Test stream processor did not recover");
        }
        TimeUnit.MILLISECONDS.sleep(5);
      }
      checkFailure();
      lifecycle = Lifecycle.RUNNING;
    } catch (final Exception error) {
      try {
        close();
      } catch (final RuntimeException closeError) {
        error.addSuppressed(closeError);
      }
      if (error instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException("Cannot start Taskite test runtime", error);
    }
  }

  @Override
  public synchronized void close() {
    if (lifecycle == Lifecycle.CLOSED) {
      return;
    }
    lifecycle = Lifecycle.CLOSED;
    recordingExporter.close();
    final List<Exception> errors = new ArrayList<>();
    closeResource(processor, errors);
    closeResource(scheduler, errors);
    closeResource(storage, errors);
    closeResource(journal, errors);
    if (directory != null) {
      try (final var paths = Files.walk(directory)) {
        for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      } catch (final IOException error) {
        errors.add(error);
      }
    }
    if (!errors.isEmpty()) {
      final var error =
          new IllegalStateException("Cannot close Taskite test runtime", errors.getFirst());
      errors.stream().skip(1).forEach(error::addSuppressed);
      throw error;
    }
  }

  public boolean isClosed() {
    return lifecycle == Lifecycle.CLOSED;
  }

  public Path getDataDirectory() {
    return directory;
  }

  public RecordingExporter getRecordingExporter() {
    return recordingExporter;
  }

  public long getLastProcessedPosition() {
    return consumedPosition.get();
  }

  public void waitForIdleState(final Duration timeout)
      throws InterruptedException, TimeoutException {
    requireRunning();
    final long deadline = System.nanoTime() + positiveTimeout(timeout);
    long idleSince = -1;
    long previousPosition = -1;
    while (System.nanoTime() < deadline) {
      requireRunning();
      checkFailure();
      final long committed = journal.committedPosition();
      final long now = System.nanoTime();
      if (consumedPosition.get() >= committed) {
        if (idleSince < 0 || committed != previousPosition) {
          idleSince = now;
        }
        if (now - idleSince >= Duration.ofMillis(30).toNanos()) {
          return;
        }
      } else {
        idleSince = -1;
      }
      previousPosition = committed;
      TimeUnit.MILLISECONDS.sleep(5);
    }
    throw new TimeoutException(
        "Engine did not become idle: consumed="
            + consumedPosition.get()
            + ", committed="
            + journal.committedPosition());
  }

  public boolean hasReachedEnd() {
    requireRunning();
    checkFailure();
    return consumedPosition.get() >= journal.committedPosition();
  }

  public void awaitProcessingOf(final Record<?> record, final Duration timeout) {
    requireRunning();
    if (record.getPartitionId() != 1 || record.getPosition() <= 0) {
      throw new IllegalArgumentException("Expected a committed record from test partition 1");
    }
    final long started = System.nanoTime();
    final long maximumWait = positiveTimeout(timeout);
    while (consumedPosition.get() < record.getPosition()) {
      requireRunning();
      checkFailure();
      if (System.nanoTime() - started >= maximumWait) {
        throw new AssertionError(
            "Record at position " + record.getPosition() + " was not processed");
      }
      try {
        TimeUnit.MILLISECONDS.sleep(5);
      } catch (final InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted awaiting record processing", error);
      }
    }
    checkFailure();
  }

  /** Returns the committed native command without waiting for its processing outcome. */
  public Record<?> writeCommand(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    requireRunning();
    checkFailure();
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(value, "value");
    if (intent == null
        || intent == Intent.UNKNOWN
        || intent.isEvent()
        || Intent.fromProtocolValue(type, intent.value()) != intent) {
      throw new IllegalArgumentException("Expected a command intent matching " + type);
    }
    if (!registry.create(type, intent).getClass().isInstance(value)) {
      throw new IllegalArgumentException("Record value does not match " + type + "." + intent);
    }
    final var metadata =
        new RecordMetadata()
            .recordType(RecordType.COMMAND)
            .valueType(type)
            .intent(intent)
            .requestId(UUID.randomUUID().toString())
            .requestStreamId(CLUSTER_ID);
    final long position = journal.append(LogAppendEntry.of(key, metadata, value)).join();
    final var event = journal.between(position - 1, position).getFirst();
    final var record = new TypedRecordImpl(1);
    record.wrap(event, metadata.copy(), registry.read(event, metadata));
    return record;
  }

  /** Follows request identity across admitted follow-up commands, including cancellation. */
  public Record<?> executeCommand(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    final var command = writeCommand(key, type, intent, value);
    return recordingExporter
        .recordStream()
        .withRequestId(((TypedRecord<?>) command).getRequestId())
        .filter(
            record ->
                record.getRecordType() == RecordType.EVENT
                    || record.getRecordType() == RecordType.COMMAND_REJECTION)
        .getFirst();
  }

  public Record<CommandDistributionRecord> createSubscription(
      final String name, final String jobType, final String tenantId) {
    final var subscription =
        new JobSubscriptionRecord()
            .setClusterId(CLUSTER_ID)
            .setTenantId(tenantId)
            .setName(name)
            .setJobType(jobType);
    final long key = nextSubscriptionKey();
    final var result = distribute(key, ValueType.JOB_SUBSCRIPTION, subscription);
    subscriptionTenants.put(key, tenantId);
    return result;
  }

  public Record<CommandDistributionRecord> deleteSubscription(
      final long key, final String tenantId) {
    requireRunning();
    if (!Objects.requireNonNull(tenantId, "tenantId").equals(subscriptionTenants.get(key))) {
      throw new IllegalArgumentException("Subscription not found for tenant");
    }
    final var deletion =
        new JobSubscriptionDeletionRecord()
            .setClusterId(CLUSTER_ID)
            .setTenantId(tenantId)
            .setSubscriptionKey(key);
    final var result =
        distribute(nextSubscriptionKey(), ValueType.JOB_SUBSCRIPTION_DELETION, deletion);
    subscriptionTenants.remove(key, tenantId);
    return result;
  }

  private Record<CommandDistributionRecord> distribute(
      final long key, final ValueType type, final UnifiedRecordValue value) {
    final var distribution =
        new CommandDistributionRecord<>()
            .setDistributionKey(key)
            .setClusterId(CLUSTER_ID)
            .setPartitionId(1)
            .setValueType(type)
            .setValue(value);
    final var result =
        executeCommand(
            key, ValueType.COMMAND_DISTRIBUTION, CommandDistributionIntent.START, distribution);
    if (result.getRecordType() == RecordType.COMMAND_REJECTION) {
      throw new AssertionError("Distribution rejected: " + result.getRejectionReason());
    }
    return recordingExporter
        .commandDistributionRecordStream()
        .withIntent(CommandDistributionIntent.ACKNOWLEDGED)
        .withRequestId(((TypedRecord<?>) result).getRequestId())
        .getFirst();
  }

  private long nextSubscriptionKey() {
    // Control-plane subscription keys do not overlap partition-1 entity keys.
    final long key = subscriptionKeys.incrementAndGet();
    if (key <= 0 || key >= (1L << Protocol.KEY_BITS)) {
      throw new IllegalStateException("Test resource key space exhausted");
    }
    return key;
  }

  private static long positiveTimeout(final Duration timeout) {
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    return timeout.toNanos();
  }

  private void failEngine(final Throwable error) {
    failure.compareAndSet(null, error);
    recordingExporter.onFailure(error);
  }

  private void checkFailure() {
    final Throwable error = failure.get();
    if (error != null) {
      throw new IllegalStateException("Test stream processor failed", error);
    }
  }

  private void requireRunning() {
    if (lifecycle != Lifecycle.RUNNING) {
      throw new IllegalStateException("Start the test runtime before use");
    }
  }

  private static void closeResource(final AutoCloseable resource, final List<Exception> errors) {
    if (resource != null) {
      try {
        resource.close();
      } catch (final Exception error) {
        errors.add(error);
      }
    }
  }

  private enum Lifecycle {
    NEW,
    RUNNING,
    CLOSED
  }
}
