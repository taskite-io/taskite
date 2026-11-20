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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.state.immutable.BrokerClusterConfig;
import io.taskite.placementdriver.state.immutable.BrokerClusterConfigState;
import java.util.List;

public interface MutableBrokerClusterConfigState extends BrokerClusterConfigState {
  void putClusterConfig(String clusterId, BrokerClusterConfig config);

  void delete(String clusterId);

  /**
   * Persists a new voter roster for the cluster, bumping the monotonic roster epoch (T10). Sizing
   * fields are preserved; a no-op when the cluster has no config yet. Returns {@code true} when the
   * roster actually changed and was persisted.
   */
  boolean updateVoterRoster(String clusterId, List<String> voterRoster);
}
