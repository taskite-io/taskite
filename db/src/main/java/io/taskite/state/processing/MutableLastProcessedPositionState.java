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
package io.taskite.state.processing;

/**
 * Mutable view of the last processed source position. {@link #markAsProcessed(long)} writes the
 * position into the currently open transaction; the stream processor owns that transaction, so the
 * position and the command's state changes commit together atomically.
 */
public interface MutableLastProcessedPositionState extends LastProcessedPositionState {

  /** Records {@code position} as the last processed one, as a write into the open transaction. */
  void markAsProcessed(long position);
}
