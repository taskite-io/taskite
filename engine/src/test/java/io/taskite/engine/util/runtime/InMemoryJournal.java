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

import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.command.RecordAppender;
import io.taskite.streamplatform.logstream.LogAppendEntry;
import io.taskite.streamplatform.logstream.LogStreamReader;
import io.taskite.streamplatform.logstream.LoggedEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/** Atomic committed batches using the production log encoding and application positions. */
final class InMemoryJournal implements LogStreamReader, RecordAppender {
  private final List<LoggedEvent> entries = new ArrayList<>();
  private long nextPosition = 1;
  private boolean closed;
  private Runnable onAppend = () -> {};

  synchronized void onAppend(final Runnable callback) {
    onAppend = callback;
  }

  synchronized long committedPosition() {
    return entries.size();
  }

  synchronized List<LoggedEvent> between(final long afterPosition, final long throughPosition) {
    return List.copyOf(
        entries.subList(
            (int) Math.min(afterPosition, entries.size()),
            (int) Math.min(throughPosition, entries.size())));
  }

  @Override
  public ActorFuture<Long> append(final List<LogAppendEntry> batch, final long sourcePosition) {
    final long committed;
    final Runnable wakeUp;
    synchronized (this) {
      if (closed) {
        return CompletableActorFuture.completedExceptionally(
            new IllegalStateException("Test journal is closed"));
      }
      final long firstPosition = entries.size() + 1L;
      final long timestamp = System.currentTimeMillis();
      final List<LoggedEvent> encoded = new ArrayList<>(batch.size());
      for (int index = 0; index < batch.size(); index++) {
        encoded.add(
            LoggedEvent.of(batch.get(index), firstPosition + index, sourcePosition, timestamp));
      }
      entries.addAll(encoded);
      committed = entries.size();
      wakeUp = onAppend;
    }
    wakeUp.run();
    return CompletableActorFuture.completed(committed);
  }

  @Override
  public synchronized void seekToNextEvent(final long position) {
    nextPosition = Math.max(1, position + 1);
  }

  @Override
  public synchronized void seek(final long position) {
    nextPosition = Math.max(1, position);
  }

  @Override
  public synchronized long getNextPosition() {
    return nextPosition;
  }

  @Override
  public synchronized boolean hasNext() {
    return !closed && nextPosition <= entries.size();
  }

  @Override
  public synchronized LoggedEvent next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No committed record at " + nextPosition);
    }
    return entries.get((int) nextPosition++ - 1);
  }

  @Override
  public synchronized void close() {
    closed = true;
    onAppend = () -> {};
  }
}
