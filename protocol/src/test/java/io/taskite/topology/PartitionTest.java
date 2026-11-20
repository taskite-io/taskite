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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class PartitionTest {

  @Test
  void replicasExcludeLeaderWhileVotersContainAllMembers() {
    final Partition partition =
        new Partition(
            1,
            new Partition.Leadership("broker-0", 7),
            new Partition.Membership(
                List.of("broker-0", "broker-1", "broker-2"), List.of(), Map.of(), 0),
            PartitionStatus.ACTIVE);

    assertEquals(List.of("broker-1", "broker-2"), partition.replicas());
    assertEquals(List.of("broker-0", "broker-1", "broker-2"), partition.voters());
  }
}
