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
 * Read view of the last processed source position. Command processing writes the command position;
 * replay writes an event/rejection's source position.
 */
public interface LastProcessedPositionState {

  /** The last successfully processed source position, or 0 if none. */
  long getLastProcessedPosition();
}
