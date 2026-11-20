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
package io.taskite.gateway.common;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Raised only when the placement driver explicitly confirms that a cluster does not exist. */
public final class UnknownClusterException extends RuntimeException {
  private final String requestedClusterId;
  private final Set<String> knownClusterIds;

  public UnknownClusterException(
      final String requestedClusterId, final Set<String> knownClusterIds, final Throwable cause) {
    super(message(requestedClusterId, knownClusterIds), cause);
    this.requestedClusterId = requestedClusterId;
    this.knownClusterIds =
        Collections.unmodifiableSet(new LinkedHashSet<>(new TreeSet<>(knownClusterIds)));
  }

  public String requestedClusterId() {
    return requestedClusterId;
  }

  public Set<String> knownClusterIds() {
    return knownClusterIds;
  }

  private static String message(
      final String requestedClusterId, final Set<String> knownClusterIds) {
    final String clusters =
        knownClusterIds.stream()
            .sorted()
            .map(clusterId -> "\"" + clusterId + "\"")
            .collect(Collectors.joining(", ", "[", "]"));
    return "UNKNOWN_CLUSTER: requested cluster \""
        + requestedClusterId
        + "\" is not known by this gateway, know clusters: "
        + clusters;
  }
}
