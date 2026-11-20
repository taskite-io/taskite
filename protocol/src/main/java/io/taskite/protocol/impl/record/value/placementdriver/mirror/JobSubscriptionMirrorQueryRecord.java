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
package io.taskite.protocol.impl.record.value.placementdriver.mirror;

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

public final class JobSubscriptionMirrorQueryRecord extends UnifiedRecordValue {
  private long mirrorKey;
  private String sourceClusterId = "";
  private String targetClusterId = "";
  private List<JobSubscriptionMirrorRecord> mirrors = new ArrayList<>();

  public long getMirrorKey() {
    return mirrorKey;
  }

  public JobSubscriptionMirrorQueryRecord setMirrorKey(final long mirrorKey) {
    this.mirrorKey = Math.max(0L, mirrorKey);
    return this;
  }

  public String getSourceClusterId() {
    return sourceClusterId;
  }

  public JobSubscriptionMirrorQueryRecord setSourceClusterId(final String sourceClusterId) {
    this.sourceClusterId = sourceClusterId == null ? "" : sourceClusterId;
    return this;
  }

  public String getTargetClusterId() {
    return targetClusterId;
  }

  public JobSubscriptionMirrorQueryRecord setTargetClusterId(final String targetClusterId) {
    this.targetClusterId = targetClusterId == null ? "" : targetClusterId;
    return this;
  }

  public List<JobSubscriptionMirrorRecord> getMirrors() {
    return mirrors;
  }

  public JobSubscriptionMirrorQueryRecord setMirrors(
      final List<JobSubscriptionMirrorRecord> mirrors) {
    this.mirrors = mirrors == null ? new ArrayList<>() : new ArrayList<>(mirrors);
    return this;
  }
}
