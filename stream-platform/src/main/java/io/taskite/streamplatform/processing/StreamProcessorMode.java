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
package io.taskite.streamplatform.processing;

/**
 * The fixed execution role of a {@link StreamProcessor}. A processor is created for one mode and
 * replaced on leadership change, so its command-processing behavior cannot change during its
 * lifetime.
 *
 * <ul>
 *   <li>{@link #PROCESSING} — leader: replay committed events to recover state, then process
 *       commands and write their follow-up records.
 *   <li>{@link #REPLAY} — follower: replay committed events forever; commands are never processed.
 * </ul>
 */
public enum StreamProcessorMode {
  PROCESSING,
  REPLAY
}
