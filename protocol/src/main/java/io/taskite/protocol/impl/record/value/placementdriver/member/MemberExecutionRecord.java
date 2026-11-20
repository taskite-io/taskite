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

import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.record.UnifiedRecordValue;

/** Concrete PD execution observation and RPC result; not a generic workflow envelope. */
public final class MemberExecutionRecord extends UnifiedRecordValue {
  public enum Phase {
    READY,
    WAIT_ADVANCE,
    RPC
  }

  public record Operation(
      String resourceId,
      String changeId,
      long sourceEpoch,
      long targetEpoch,
      int index,
      String operationId,
      MemberChangeOperationType type,
      String memberId,
      long barrierIndex) {}

  public record Schedule(
      Operation operation,
      Phase phase,
      long attempt,
      long dueAt,
      long deadline,
      int readySamples,
      long sampledAt) {
    public Schedule ready(final long due) {
      return new Schedule(operation, Phase.READY, attempt, due, 0, readySamples, sampledAt);
    }

    public Schedule sample(final int count, final long at) {
      return new Schedule(operation, phase, attempt, dueAt, deadline, count, at);
    }
  }

  private Schedule schedule;
  private MemberChangeScope scope = MemberChangeScope.PLACEMENT_DRIVER;
  private boolean success;

  public MemberChangeScope getScope() {
    return scope;
  }

  public MemberExecutionRecord setScope(final MemberChangeScope value) {
    scope = value;
    return this;
  }

  public Schedule getSchedule() {
    return schedule;
  }

  public MemberExecutionRecord setSchedule(final Schedule value) {
    schedule = value;
    return this;
  }

  public boolean isSuccess() {
    return success;
  }

  public MemberExecutionRecord setSuccess(final boolean value) {
    success = value;
    return this;
  }
}
