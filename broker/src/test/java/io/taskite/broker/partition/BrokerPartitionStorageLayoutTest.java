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
package io.taskite.broker.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class BrokerPartitionStorageLayoutTest {

  @Test
  void resolvesPartitionBelowClusterAndBroker() {
    final Path dataRoot = Path.of("data");

    assertEquals(
        Path.of("data", "cluster-a", "broker-2", "partitions", "7"),
        BrokerPartitionManager.partitionDataDirectory(dataRoot, "cluster-a", "broker-2", 7));
  }
}
