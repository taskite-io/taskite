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

import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import java.util.List;
import java.util.Optional;

public interface JobSubscriptionMirrorState {
  Optional<JobSubscriptionMirrorRecord> mirror(long mirrorKey);

  Optional<JobSubscriptionMirrorRecord> mirror(String sourceClusterId, String targetClusterId);

  List<JobSubscriptionMirrorRecord> mirrors();

  default List<JobSubscriptionMirrorRecord> mirrorsFrom(final String sourceClusterId) {
    return mirrors().stream()
        .filter(mirror -> mirror.getSourceClusterId().equals(sourceClusterId))
        .toList();
  }

  default List<JobSubscriptionMirrorRecord> mirrorsTo(final String targetClusterId) {
    return mirrors().stream()
        .filter(mirror -> mirror.getTargetClusterId().equals(targetClusterId))
        .toList();
  }
}
