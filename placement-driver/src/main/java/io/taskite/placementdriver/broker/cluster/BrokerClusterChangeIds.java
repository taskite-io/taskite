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
package io.taskite.placementdriver.broker.cluster;

import java.util.List;

public final class BrokerClusterChangeIds {
  private static final String ADMIN_PREFIX = "admin:";

  private BrokerClusterChangeIds() {}

  public static List<TopologyOperation> scopeOperations(
      final String changeId, final List<TopologyOperation> operations) {
    return operations.stream()
        .map(
            operation ->
                new TopologyOperation(
                    changeId + ":" + operation.opId(),
                    operation.type(),
                    operation.targetBroker(),
                    operation.priority(),
                    operation.configuration()))
        .toList();
  }

  public static String createAdminChangeId(
      final String operation, final String batchId, final int partitionId) {
    return ADMIN_PREFIX + operation + ":" + batchId + ":" + partitionId;
  }

  public static String extractBatchKey(final String changeId) {
    if (changeId == null || !changeId.startsWith(ADMIN_PREFIX)) {
      return "";
    }
    final int separator = changeId.lastIndexOf(':');
    return separator <= ADMIN_PREFIX.length() ? "" : changeId.substring(0, separator);
  }

  public static String extractOperationId(final String changeId) {
    final String batchKey = extractBatchKey(changeId);
    if (batchKey.isBlank()) {
      return changeId;
    }
    final int separator = batchKey.lastIndexOf(':');
    return separator < 0 ? changeId : batchKey.substring(separator + 1);
  }
}
