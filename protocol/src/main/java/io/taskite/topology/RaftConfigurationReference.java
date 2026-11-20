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
package io.taskite.topology;

/** Identity of an applied Raft configuration; the index is not a topology version. */
public record RaftConfigurationReference(long index, String digest) {
  public static final RaftConfigurationReference UNKNOWN = new RaftConfigurationReference(0, "");

  public RaftConfigurationReference {
    digest = digest == null ? "" : digest;
  }
}
