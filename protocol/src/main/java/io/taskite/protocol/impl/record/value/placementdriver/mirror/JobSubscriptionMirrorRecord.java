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
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorCreationOutcome;

public final class JobSubscriptionMirrorRecord extends UnifiedRecordValue {
  private long mirrorKey;
  private String sourceClusterId = "";
  private String targetClusterId = "";
  private long createdAtEpochMillis;
  private JobSubscriptionMirrorCreationOutcome outcome;

  public long getMirrorKey() {
    return mirrorKey;
  }

  public JobSubscriptionMirrorRecord setMirrorKey(final long mirrorKey) {
    this.mirrorKey = Math.max(0L, mirrorKey);
    return this;
  }

  public String getSourceClusterId() {
    return sourceClusterId;
  }

  public JobSubscriptionMirrorRecord setSourceClusterId(final String sourceClusterId) {
    this.sourceClusterId = sourceClusterId == null ? "" : sourceClusterId;
    return this;
  }

  public String getTargetClusterId() {
    return targetClusterId;
  }

  public JobSubscriptionMirrorRecord setTargetClusterId(final String targetClusterId) {
    this.targetClusterId = targetClusterId == null ? "" : targetClusterId;
    return this;
  }

  public long getCreatedAtEpochMillis() {
    return createdAtEpochMillis;
  }

  public JobSubscriptionMirrorRecord setCreatedAtEpochMillis(final long createdAtEpochMillis) {
    this.createdAtEpochMillis = Math.max(0L, createdAtEpochMillis);
    return this;
  }

  public JobSubscriptionMirrorCreationOutcome getOutcome() {
    return outcome;
  }

  public JobSubscriptionMirrorRecord setOutcome(
      final JobSubscriptionMirrorCreationOutcome outcome) {
    this.outcome = outcome;
    return this;
  }
}
