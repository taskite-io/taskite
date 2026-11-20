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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.taskite.protocol.record.UnifiedRecordValue;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Shared command/event payload for Broker and placement-driver member changes. */
public final class MemberChangeRecord extends UnifiedRecordValue {
  private MemberChangeScope scope = MemberChangeScope.BROKER;
  private String resourceId = "";
  private String changeId = "";
  private List<String> requestedAdditions = new ArrayList<>();
  private List<String> requestedRemovals = new ArrayList<>();
  private Integer requestedCount;
  private long requestedAtEpochMillis;
  private long sourceEpoch;
  private List<String> sourceVoters = new ArrayList<>();
  private long targetEpoch;
  private int targetCount;
  private List<String> targetVoters = new ArrayList<>();
  private List<String> removedMembers = new ArrayList<>();
  private MemberChangeStatus status = MemberChangeStatus.PENDING;
  private int nextOperationIndex;
  private List<MemberChangeOperation> operations = new ArrayList<>();
  private List<String> childPartitionChangeIds = new ArrayList<>();
  private MemberChangeOutcome outcome;
  private String error = "";

  public MemberChangeScope getScope() {
    return scope;
  }

  public MemberChangeRecord setScope(final MemberChangeScope scope) {
    this.scope = scope == null ? MemberChangeScope.BROKER : scope;
    return this;
  }

  public String getResourceId() {
    return resourceId;
  }

  public MemberChangeRecord setResourceId(final String resourceId) {
    this.resourceId = value(resourceId);
    return this;
  }

  public String getChangeId() {
    return changeId;
  }

  public MemberChangeRecord setChangeId(final String changeId) {
    this.changeId = value(changeId);
    return this;
  }

  public List<String> getRequestedAdditions() {
    return requestedAdditions;
  }

  public MemberChangeRecord setRequestedAdditions(final List<String> requestedAdditions) {
    this.requestedAdditions = copy(requestedAdditions);
    return this;
  }

  public List<String> getRequestedRemovals() {
    return requestedRemovals;
  }

  public MemberChangeRecord setRequestedRemovals(final List<String> requestedRemovals) {
    this.requestedRemovals = copy(requestedRemovals);
    return this;
  }

  public Integer getRequestedCount() {
    return requestedCount;
  }

  public MemberChangeRecord setRequestedCount(final Integer requestedCount) {
    this.requestedCount = requestedCount;
    return this;
  }

  public long getRequestedAtEpochMillis() {
    return requestedAtEpochMillis;
  }

  public MemberChangeRecord setRequestedAtEpochMillis(final long requestedAtEpochMillis) {
    this.requestedAtEpochMillis = Math.max(0L, requestedAtEpochMillis);
    return this;
  }

  public long getSourceEpoch() {
    return sourceEpoch;
  }

  public MemberChangeRecord setSourceEpoch(final long sourceEpoch) {
    this.sourceEpoch = Math.max(0L, sourceEpoch);
    return this;
  }

  public List<String> getSourceVoters() {
    return sourceVoters;
  }

  public MemberChangeRecord setSourceVoters(final List<String> sourceVoters) {
    this.sourceVoters = copy(sourceVoters);
    return this;
  }

  public long getTargetEpoch() {
    return targetEpoch;
  }

  public MemberChangeRecord setTargetEpoch(final long targetEpoch) {
    this.targetEpoch = Math.max(0L, targetEpoch);
    return this;
  }

  public int getTargetCount() {
    return targetCount;
  }

  public MemberChangeRecord setTargetCount(final int targetCount) {
    this.targetCount = Math.max(0, targetCount);
    return this;
  }

  public List<String> getTargetVoters() {
    return targetVoters;
  }

  public MemberChangeRecord setTargetVoters(final List<String> targetVoters) {
    this.targetVoters = copy(targetVoters);
    return this;
  }

  public List<String> getRemovedMembers() {
    return removedMembers;
  }

  public MemberChangeRecord setRemovedMembers(final List<String> removedMembers) {
    this.removedMembers = copy(removedMembers);
    return this;
  }

  public MemberChangeStatus getStatus() {
    return status;
  }

  public MemberChangeRecord setStatus(final MemberChangeStatus status) {
    this.status = status == null ? MemberChangeStatus.PENDING : status;
    return this;
  }

  public int getNextOperationIndex() {
    return nextOperationIndex;
  }

  public MemberChangeRecord setNextOperationIndex(final int nextOperationIndex) {
    this.nextOperationIndex = Math.max(0, nextOperationIndex);
    return this;
  }

  public List<MemberChangeOperation> getOperations() {
    return operations;
  }

  public MemberChangeRecord setOperations(final List<MemberChangeOperation> operations) {
    this.operations =
        operations == null
            ? new ArrayList<>()
            : operations.stream()
                .map(MemberChangeOperation::copy)
                .collect(Collectors.toCollection(ArrayList::new));
    return this;
  }

  public List<String> getChildPartitionChangeIds() {
    return childPartitionChangeIds;
  }

  public MemberChangeRecord setChildPartitionChangeIds(final List<String> childPartitionChangeIds) {
    this.childPartitionChangeIds = copy(childPartitionChangeIds);
    return this;
  }

  public MemberChangeOutcome getOutcome() {
    return outcome;
  }

  public MemberChangeRecord setOutcome(final MemberChangeOutcome outcome) {
    this.outcome = outcome;
    return this;
  }

  public String getError() {
    return error;
  }

  public MemberChangeRecord setError(final String error) {
    this.error = value(error);
    return this;
  }

  @JsonIgnore
  public boolean isTerminal() {
    return status.isTerminal();
  }

  public MemberChangeRecord copy() {
    return new MemberChangeRecord()
        .setScope(scope)
        .setResourceId(resourceId)
        .setChangeId(changeId)
        .setRequestedAdditions(requestedAdditions)
        .setRequestedRemovals(requestedRemovals)
        .setRequestedCount(requestedCount)
        .setRequestedAtEpochMillis(requestedAtEpochMillis)
        .setSourceEpoch(sourceEpoch)
        .setSourceVoters(sourceVoters)
        .setTargetEpoch(targetEpoch)
        .setTargetCount(targetCount)
        .setTargetVoters(targetVoters)
        .setRemovedMembers(removedMembers)
        .setStatus(status)
        .setNextOperationIndex(nextOperationIndex)
        .setOperations(operations)
        .setChildPartitionChangeIds(childPartitionChangeIds)
        .setOutcome(outcome)
        .setError(error);
  }

  public enum MemberChangeScope {
    BROKER,
    PLACEMENT_DRIVER
  }

  public enum MemberChangeOutcome {
    ACCEPTED,
    NO_CHANGES;

    @JsonCreator
    public static MemberChangeOutcome fromJson(final String value) {
      return value == null || value.isEmpty() ? null : valueOf(value);
    }
  }

  public enum MemberChangeStatus {
    PENDING,
    WAITING_CATCH_UP,
    IN_PROGRESS,
    CANCELLING,
    COMPLETED,
    CANCELLED,
    FAILED;

    public boolean isTerminal() {
      return this == COMPLETED || this == CANCELLED || this == FAILED;
    }
  }

  public enum MemberChangeOperationType {
    MEMBER_JOIN,
    MEMBER_ADD_LEARNER,
    MEMBER_PROMOTE_VOTER,
    TRANSFER_LEADER,
    MEMBER_LEAVE,
    MEMBER_REMOVE
  }

  public enum MemberChangeOperationStatus {
    PENDING,
    IN_PROGRESS,
    COMPLETED,
    CANCELLED,
    FAILED
  }

  public static final class MemberChangeOperation {
    private String operationId = "";
    private MemberChangeOperationType type = MemberChangeOperationType.MEMBER_JOIN;
    private String memberId = "";
    private MemberChangeOperationStatus status = MemberChangeOperationStatus.PENDING;
    private long barrierIndex;
    private String error = "";

    public String getOperationId() {
      return operationId;
    }

    public MemberChangeOperation setOperationId(final String operationId) {
      this.operationId = value(operationId);
      return this;
    }

    public MemberChangeOperationType getType() {
      return type;
    }

    public MemberChangeOperation setType(final MemberChangeOperationType type) {
      this.type = type == null ? MemberChangeOperationType.MEMBER_JOIN : type;
      return this;
    }

    public String getMemberId() {
      return memberId;
    }

    public MemberChangeOperation setMemberId(final String memberId) {
      this.memberId = value(memberId);
      return this;
    }

    public MemberChangeOperationStatus getStatus() {
      return status;
    }

    public MemberChangeOperation setStatus(final MemberChangeOperationStatus status) {
      this.status = status == null ? MemberChangeOperationStatus.PENDING : status;
      return this;
    }

    public long getBarrierIndex() {
      return barrierIndex;
    }

    public MemberChangeOperation setBarrierIndex(final long barrierIndex) {
      this.barrierIndex = Math.max(0L, barrierIndex);
      return this;
    }

    public String getError() {
      return error;
    }

    public MemberChangeOperation setError(final String error) {
      this.error = value(error);
      return this;
    }

    public MemberChangeOperation copy() {
      return new MemberChangeOperation()
          .setOperationId(operationId)
          .setType(type)
          .setMemberId(memberId)
          .setStatus(status)
          .setBarrierIndex(barrierIndex)
          .setError(error);
    }
  }

  private static String value(final String value) {
    return value == null ? "" : value;
  }

  private static List<String> copy(final List<String> values) {
    return values == null ? new ArrayList<>() : new ArrayList<>(values);
  }
}
