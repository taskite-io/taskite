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
package io.taskite.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

final class SnapshotPolicyTest {

  @Test
  void rejectsDisablingBothTriggers() {
    assertThrows(IllegalArgumentException.class, () -> SnapshotPolicy.of(Duration.ZERO, 0));
  }

  @Test
  void exposesStableDefaults() {
    final SnapshotPolicy policy = SnapshotPolicy.defaults();

    assertEquals(Duration.ofMinutes(15), policy.interval());
    assertEquals(128L * 1024 * 1024, policy.logSizeThresholdBytes());
    assertEquals(Duration.ofSeconds(1), policy.initialRetryDelay());
    assertEquals(Duration.ofMinutes(1), policy.maxRetryDelay());
  }
}
