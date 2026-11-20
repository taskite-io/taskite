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

import io.taskite.db.impl.DbForeignKey;
import java.util.Collection;

/**
 * Implemented by keys/values that reference rows in other column families. The transaction layer
 * uses this to optionally verify that every referenced foreign key exists before a write (see
 * {@code ForeignKeyChecker} and {@link ConsistencyChecksSettings#enableForeignKeyChecks()}).
 */
public interface ContainsForeignKeys {
  Collection<DbForeignKey<DbKey>> containedForeignKeys();
}
