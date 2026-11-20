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
package io.taskite.protocol.impl.record.value.placementdriver.member;

import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;

public final class MemberConfigurationRecord extends UnifiedRecordValue {
  private long epoch;
  private int targetCount;
  private List<String> targetVoters = new ArrayList<>();
  private List<String> actualVoters = new ArrayList<>();
  private List<String> actualLearners = new ArrayList<>();
  private String leaderId = "";
  private long term;
  private String activeChangeId = "";

  public long getEpoch() {
    return epoch;
  }

  public MemberConfigurationRecord setEpoch(final long epoch) {
    this.epoch = Math.max(0L, epoch);
    return this;
  }

  public int getTargetCount() {
    return targetCount;
  }

  public MemberConfigurationRecord setTargetCount(final int targetCount) {
    this.targetCount = Math.max(0, targetCount);
    return this;
  }

  public List<String> getTargetVoters() {
    return targetVoters;
  }

  public MemberConfigurationRecord setTargetVoters(final List<String> targetVoters) {
    this.targetVoters = copy(targetVoters);
    return this;
  }

  public List<String> getActualVoters() {
    return actualVoters;
  }

  public MemberConfigurationRecord setActualVoters(final List<String> actualVoters) {
    this.actualVoters = copy(actualVoters);
    return this;
  }

  public List<String> getActualLearners() {
    return actualLearners;
  }

  public MemberConfigurationRecord setActualLearners(final List<String> actualLearners) {
    this.actualLearners = copy(actualLearners);
    return this;
  }

  public String getLeaderId() {
    return leaderId;
  }

  public MemberConfigurationRecord setLeaderId(final String leaderId) {
    this.leaderId = leaderId == null ? "" : leaderId;
    return this;
  }

  public long getTerm() {
    return term;
  }

  public MemberConfigurationRecord setTerm(final long term) {
    this.term = Math.max(0L, term);
    return this;
  }

  public String getActiveChangeId() {
    return activeChangeId;
  }

  public MemberConfigurationRecord setActiveChangeId(final String activeChangeId) {
    this.activeChangeId = activeChangeId == null ? "" : activeChangeId;
    return this;
  }

  private static List<String> copy(final List<String> values) {
    return values == null ? new ArrayList<>() : new ArrayList<>(values);
  }
}
