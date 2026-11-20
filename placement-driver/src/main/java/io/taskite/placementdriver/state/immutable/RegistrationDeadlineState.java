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
package io.taskite.placementdriver.state.immutable;

import io.taskite.placementdriver.registry.RegistrationDeadline;
import java.util.List;

public interface RegistrationDeadlineState {
  RegistrationDeadlineState EMPTY =
      new RegistrationDeadlineState() {
        @Override
        public List<RegistrationDeadline> findDue(final long timestamp, final int limit) {
          return List.of();
        }

        @Override
        public long getNextDueDate() {
          return Long.MAX_VALUE;
        }
      };

  List<RegistrationDeadline> findDue(long timestamp, int limit);

  long getNextDueDate();
}
