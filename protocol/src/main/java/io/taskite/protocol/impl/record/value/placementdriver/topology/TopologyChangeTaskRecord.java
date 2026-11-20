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
package io.taskite.protocol.impl.record.value.placementdriver.topology;

import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.record.UnifiedRecordValue;

/** Durable state of one concrete membership or partition operation. */
public final class TopologyChangeTaskRecord extends UnifiedRecordValue {
  public enum Scope {
    BROKER_PARTITION,
    BROKER_MEMBER,
    PLACEMENT_DRIVER_MEMBER
  }

  public enum Status {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    CANCELLED,
    SUPERSEDED;

    public boolean terminal() {
      return this == COMPLETED || this == CANCELLED || this == SUPERSEDED;
    }
  }

  public record Key(
      Scope scope, String clusterId, String changeId, int partitionId, String operationId) {}

  public record Task(
      Key key,
      Status status,
      PartitionExecutionRecord.Schedule partitionSchedule,
      MemberExecutionRecord.Schedule memberSchedule) {
    public long dueAt() {
      return partitionSchedule != null ? partitionSchedule.dueAt() : memberSchedule.dueAt();
    }

    public long deadline() {
      return partitionSchedule != null ? partitionSchedule.deadline() : memberSchedule.deadline();
    }

    public boolean reserved() {
      return partitionSchedule != null
          ? partitionSchedule.phase() != PartitionExecutionRecord.Phase.READY
          : memberSchedule.phase() == MemberExecutionRecord.Phase.RPC;
    }

    public Task withStatus(final Status value) {
      return new Task(key, value, partitionSchedule, memberSchedule);
    }
  }

  private Task task;

  public Task getTask() {
    return task;
  }

  public TopologyChangeTaskRecord setTask(final Task value) {
    task = value;
    return this;
  }
}
