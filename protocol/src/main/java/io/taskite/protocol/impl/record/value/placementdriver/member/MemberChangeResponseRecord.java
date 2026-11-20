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

/** Query response combining a persisted member change with the current committed membership. */
public final class MemberChangeResponseRecord extends UnifiedRecordValue {
  private MemberChangeRecord change = new MemberChangeRecord();
  private List<String> actualVoters = new ArrayList<>();
  private List<String> actualLearners = new ArrayList<>();

  public MemberChangeRecord getChange() {
    return change;
  }

  public MemberChangeResponseRecord setChange(final MemberChangeRecord change) {
    this.change = change == null ? new MemberChangeRecord() : change.copy();
    return this;
  }

  public List<String> getActualVoters() {
    return actualVoters;
  }

  public MemberChangeResponseRecord setActualVoters(final List<String> actualVoters) {
    this.actualVoters = copy(actualVoters);
    return this;
  }

  public List<String> getActualLearners() {
    return actualLearners;
  }

  public MemberChangeResponseRecord setActualLearners(final List<String> actualLearners) {
    this.actualLearners = copy(actualLearners);
    return this;
  }

  private static List<String> copy(final List<String> values) {
    return values == null ? new ArrayList<>() : new ArrayList<>(values);
  }
}
