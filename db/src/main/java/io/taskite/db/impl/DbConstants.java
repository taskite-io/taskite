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
package io.taskite.db.impl;

import java.nio.ByteOrder;

public final class DbConstants {
  private DbConstants() {}

  /**
   * Byte order for primitive keys/values written into RocksDB buffers. BIG_ENDIAN preserves
   * ascending numeric ordering in RocksDB's lexicographic byte comparison, which matters for keys
   * that start with a number (e.g. a long) and rely on ordered iteration.
   */
  public static final ByteOrder DB_BYTE_ORDER = ByteOrder.BIG_ENDIAN;
}
