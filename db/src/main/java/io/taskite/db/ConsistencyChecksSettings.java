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
package io.taskite.db;

/**
 * Toggles the optional consistency checks the transaction layer performs on writes. Both default to
 * off in production for throughput; tests and the fork's {@code TaskiteTransactionDb} enable
 * preconditions to catch programming errors early.
 *
 * @param enablePreconditions verify insert/update/deleteExisting key preconditions
 * @param enableForeignKeyChecks verify referenced foreign keys exist before a write
 */
public record ConsistencyChecksSettings(
    boolean enablePreconditions, boolean enableForeignKeyChecks) {
  private static final boolean DEFAULT_ENABLE_PRECONDITIONS = false;
  private static final boolean DEFAULT_ENABLE_FOREIGN_KEY_CHECKS = false;

  public ConsistencyChecksSettings() {
    this(DEFAULT_ENABLE_PRECONDITIONS, DEFAULT_ENABLE_FOREIGN_KEY_CHECKS);
  }
}
