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
package io.taskite.placementdriver.runtime;

import io.taskite.util.HostPort;

record PlacementDriverEndpoint(String host, int port) {
  PlacementDriverEndpoint {
    host = HostPort.normalizeHost(host);
  }

  static PlacementDriverEndpoint parse(final String raw) {
    final HostPort endpoint = HostPort.parse(raw);
    return new PlacementDriverEndpoint(endpoint.host(), endpoint.port());
  }
}
