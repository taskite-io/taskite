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
package io.taskite.placementdriver.runtime;

import io.taskite.streamplatform.api.record.CommittedRecordObserver;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PlacementDriverCommittedRecordObserver
    implements CommittedRecordObserver, AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDriverCommittedRecordObserver.class);

  private final Executor dispatcher;
  private final CopyOnWriteArrayList<PlacementDriverCommittedRecordListener> listeners =
      new CopyOnWriteArrayList<>();
  private final AtomicBoolean closed = new AtomicBoolean();

  public PlacementDriverCommittedRecordObserver(final Executor dispatcher) {
    this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
  }

  public PlacementDriverCommittedRecordSubscription subscribeToCommittedRecords(
      final PlacementDriverCommittedRecordListener listener) {
    Objects.requireNonNull(listener, "listener");
    if (closed.get()) {
      throw new IllegalStateException("committed record observer is closed");
    }
    listeners.add(listener);
    final AtomicBoolean subscribed = new AtomicBoolean(true);
    return () -> {
      if (subscribed.compareAndSet(true, false)) {
        listeners.remove(listener);
      }
    };
  }

  @Override
  public void onProcessed(final LoggedEvent record) {
    if (record != null && !closed.get()) {
      dispatcher.execute(() -> publishCommittedRecord(new CommittedPlacementDriverRecord(record)));
    }
  }

  @Override
  public void onFailure(final LoggedEvent record, final Throwable error) {
    if (record != null) {
      LOG.warn("placement-driver failed to apply committed record {}", record.key(), error);
    }
  }

  public void publishCommittedRecord(final CommittedPlacementDriverRecord record) {
    if (closed.get()) {
      return;
    }
    for (final PlacementDriverCommittedRecordListener listener : listeners) {
      try {
        listener.onCommittedRecord(record);
      } catch (final RuntimeException error) {
        LOG.warn("placement-driver committed record listener failed", error);
      }
    }
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      listeners.clear();
    }
  }
}
