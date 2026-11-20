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
package io.taskite.placementdriver.processing.topology;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.Set;

public final class TopologyMembershipChangeBehavior {
  private final Writers writers;
  private static final String CHANGE_NOT_FOUND =
      "MEMBER_CHANGE_NOT_FOUND: Expected to advance member change '%s' for scope '%s' and resource '%s', but no matching active change was found";
  private static final String STALE_PROGRESS =
      "STALE_MEMBER_CHANGE_PROGRESS: Expected to advance member change '%s' for scope '%s' and resource '%s', but the epoch, operation index or operation identity is stale (requested epoch '%d', active epoch '%d', requested index '%d', active index '%d')";
  private static final String PENDING_OPERATIONS =
      "MEMBER_CHANGE_CANNOT_COMPLETE_WITH_PENDING_OPERATIONS: Expected to advance member change '%s' for scope '%s' and resource '%s' to status '%s', but operation index '%d' has not advanced";
  private final ProcessingState state;

  public TopologyMembershipChangeBehavior(final TypedRecordProcessorContext context) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
  }

  public void advance(
      final TypedRecord<MemberChangeRecord> record,
      final MemberChangeIntent event,
      final Set<MemberChangeOperationType> types) {
    final MemberChangeRecord requested = record.getValue();
    final MemberChangeRecord active =
        state
            .getMemberChangeState()
            .active(requested.getScope(), requested.getResourceId())
            .orElse(null);
    if (active == null || !active.getChangeId().equals(requested.getChangeId())) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.NOT_FOUND,
              CHANGE_NOT_FOUND.formatted(
                  requested.getChangeId(), requested.getScope(), requested.getResourceId()));
      return;
    }
    if (requested.getTargetEpoch() != active.getTargetEpoch()
        || requested.getSourceEpoch() != active.getSourceEpoch()
        || requested.getNextOperationIndex() < active.getNextOperationIndex()
        || requested.getNextOperationIndex() > active.getNextOperationIndex() + 1
        || (!requested.getOperations().isEmpty() && !sameOperations(active, requested))) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              STALE_PROGRESS.formatted(
                  requested.getChangeId(),
                  requested.getScope(),
                  requested.getResourceId(),
                  requested.getTargetEpoch(),
                  active.getTargetEpoch(),
                  requested.getNextOperationIndex(),
                  active.getNextOperationIndex()));
      return;
    }
    if (!types.isEmpty()
        && (active.getNextOperationIndex() >= active.getOperations().size()
            || !types.contains(active.getOperations().get(active.getNextOperationIndex()).getType())
            || requested.getNextOperationIndex() != active.getNextOperationIndex() + 1)) {
      writers
          .rejection()
          .appendRejection(
              RejectionType.INVALID_STATE,
              STALE_PROGRESS.formatted(
                  requested.getChangeId(),
                  requested.getScope(),
                  requested.getResourceId(),
                  requested.getTargetEpoch(),
                  active.getTargetEpoch(),
                  requested.getNextOperationIndex(),
                  active.getNextOperationIndex()));
      return;
    }
    if (requested.getNextOperationIndex() == active.getNextOperationIndex()) {
      if (requested.getStatus().isTerminal()
          && active.getNextOperationIndex() < active.getOperations().size()) {
        writers
            .rejection()
            .appendRejection(
                RejectionType.INVALID_STATE,
                PENDING_OPERATIONS.formatted(
                    requested.getChangeId(),
                    requested.getScope(),
                    requested.getResourceId(),
                    requested.getStatus(),
                    requested.getNextOperationIndex()));
        return;
      }
      final MemberChangeRecord updated =
          active.copy().setStatus(requested.getStatus()).setError(requested.getError());
      if (!requested.getOperations().isEmpty()) {
        updated.setOperations(requested.getOperations());
      }
      writers.state().appendFollowUpEvent(record.getKey(), event, updated);
      return;
    }

    final MemberChangeRecord advanced =
        active
            .copy()
            .setNextOperationIndex(requested.getNextOperationIndex())
            .setStatus(requested.getStatus())
            .setError(requested.getError());
    if (!requested.getOperations().isEmpty()) {
      advanced.setOperations(requested.getOperations());
    }
    if (advanced.getNextOperationIndex() >= advanced.getOperations().size()
        && !advanced.getStatus().isTerminal()) {
      advanced.setStatus(MemberChangeStatus.COMPLETED);
    }
    writers.state().appendFollowUpEvent(record.getKey(), event, advanced);
  }

  private static boolean sameOperations(
      final MemberChangeRecord active, final MemberChangeRecord requested) {
    if (requested.getOperations().size() != active.getOperations().size()) {
      return false;
    }
    for (int index = 0; index < active.getOperations().size(); index++) {
      final var expected = active.getOperations().get(index);
      final var actual = requested.getOperations().get(index);
      if (!expected.getOperationId().equals(actual.getOperationId())
          || expected.getType() != actual.getType()
          || !expected.getMemberId().equals(actual.getMemberId())
          || expected.getBarrierIndex() != actual.getBarrierIndex()) {
        return false;
      }
    }
    return true;
  }

  public static MemberChangeIntent command(final MemberChangeOperationType type) {
    return switch (type) {
      case MEMBER_JOIN, MEMBER_ADD_LEARNER -> MemberChangeIntent.JOIN;
      case MEMBER_LEAVE, MEMBER_REMOVE -> MemberChangeIntent.LEAVE;
      case MEMBER_PROMOTE_VOTER -> MemberChangeIntent.PROMOTE;
      case TRANSFER_LEADER -> MemberChangeIntent.TRANSFER_LEADER;
    };
  }
}
