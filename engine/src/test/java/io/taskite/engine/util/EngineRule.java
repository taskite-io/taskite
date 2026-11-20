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
package io.taskite.engine.util;

import io.taskite.engine.util.client.*;
import io.taskite.engine.util.record.RecordLogger;
import io.taskite.engine.util.record.RecordingExporter;
import io.taskite.engine.util.runtime.EngineTestRuntime;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.record.Record;
import io.taskite.protocol.record.UnifiedRecordValue;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.Intent;
import java.time.Duration;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.api.extension.TestWatcher;

/** Single-partition engine fixture for manual use or JUnit 5 {@code @RegisterExtension}. */
public final class EngineRule
    implements BeforeEachCallback, AfterEachCallback, TestWatcher, AutoCloseable, CommandWriter {
  private static final Duration WAIT = Duration.ofSeconds(5);
  private final boolean workerJobLeaseTokenEnabled;
  private EngineTestRuntime engine;
  private RecordingExporter.Scope recordingScope;
  private boolean running;
  private boolean closed;

  private EngineRule(final boolean workerJobLeaseTokenEnabled) {
    this.workerJobLeaseTokenEnabled = workerJobLeaseTokenEnabled;
    engine = EngineTestRuntime.create(workerJobLeaseTokenEnabled);
  }

  public static EngineRule singlePartition() {
    return new EngineRule(false);
  }

  public static EngineRule singlePartitionWithWorkerLeases() {
    return new EngineRule(true);
  }

  public synchronized void start() {
    if (running) {
      return;
    }
    if (closed) {
      engine = EngineTestRuntime.create(workerJobLeaseTokenEnabled);
      closed = false;
    }
    try {
      engine.start();
      recordingScope = RecordingExporter.bind(engine.getRecordingExporter());
      running = true;
    } catch (final RuntimeException error) {
      closed = true;
      try {
        engine.close();
      } catch (final RuntimeException closeError) {
        error.addSuppressed(closeError);
      }
      throw error;
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    running = false;
    closed = true;
    try {
      engine.close();
    } finally {
      if (recordingScope != null) {
        recordingScope.close();
        recordingScope = null;
      }
    }
  }

  public void stop() {
    close();
  }

  @Override
  public synchronized void beforeEach(final ExtensionContext context) {
    if (running) {
      throw new IllegalStateException("An EngineRule cannot be shared by concurrent tests");
    }
    start();
    // Store cleanup also covers failures in another beforeEach callback.
    context.getStore(namespace()).put("engine", (CloseableResource) this::close);
    engine.createSubscription("work", "work", "<default>");
    engine.getRecordingExporter().clear();
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    if (context.getStore(namespace()).remove("engine") != null) {
      close();
    }
  }

  @Override
  public void testFailed(final ExtensionContext context, final Throwable error) {
    try {
      new RecordLogger(engine.getRecordingExporter().snapshot()).log(context.getDisplayName());
    } finally {
      stop();
    }
  }

  private ExtensionContext.Namespace namespace() {
    return ExtensionContext.Namespace.create(EngineRule.class, this);
  }

  public EngineTestRuntime getEngine() {
    requireRunning();
    return engine;
  }

  @Override
  public RecordingExporter getRecordingExporter() {
    return engine.getRecordingExporter();
  }

  @Override
  public Record<?> writeCommand(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    return getEngine().writeCommand(key, type, intent, value);
  }

  @Override
  public Record<CommandDistributionRecord> createSubscription(
      final String name, final String jobType, final String tenantId) {
    return getEngine().createSubscription(name, jobType, tenantId);
  }

  @Override
  public Record<CommandDistributionRecord> deleteSubscription(
      final long key, final String tenantId) {
    return getEngine().deleteSubscription(key, tenantId);
  }

  public long getLastProcessedPosition() {
    return getEngine().getLastProcessedPosition();
  }

  public boolean hasReachedEnd() {
    return getEngine().hasReachedEnd();
  }

  public void waitForIdleState(final Duration timeout) {
    try {
      getEngine().waitForIdleState(timeout);
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted awaiting idle engine", error);
    } catch (final java.util.concurrent.TimeoutException error) {
      throw new AssertionError("Engine did not become idle", error);
    }
  }

  public void awaitProcessingOf(final Record<?> record) {
    getEngine().awaitProcessingOf(record, WAIT);
  }

  /** Awaits the first committed event or rejection in this native command's processing chain. */
  public Record<?> executeCommand(
      final long key, final ValueType type, final Intent intent, final UnifiedRecordValue value) {
    return getEngine().executeCommand(key, type, intent, value);
  }

  public JobActivationClient jobs() {
    return new JobActivationClient(this);
  }

  public JobClient job() {
    return new JobClient(this);
  }

  public VariableClient variables() {
    return new VariableClient(this);
  }

  public IncidentClient incident() {
    return new IncidentClient(this);
  }

  private void requireRunning() {
    if (!running) {
      throw new IllegalStateException("Start EngineRule before use");
    }
  }
}
