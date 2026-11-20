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
package io.taskite.placementdriver.processing.member;

import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.BrokerPartitionChangeIntent;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.state.KeyGenerator;
import io.taskite.streamplatform.api.writer.Writers;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class MemberRequestProcessor implements TypedRecordProcessor<MemberChangeRecord> {
  private final Writers writers;
  private static final String UNSUPPORTED_SCOPE =
      "UNSUPPORTED_MEMBER_CHANGE_SCOPE: Expected to request member change '%s' for resource '%s', but no planner supports scope '%s'";
  private static final String CHANGE_IN_PROGRESS =
      "MEMBER_CHANGE_IN_PROGRESS: Expected to request member change '%s' for scope '%s' and resource '%s', but active change '%s' has a different target";
  private static final String INVALID_IDENTITY =
      "INVALID_MEMBER_CHANGE: Expected to request a member change with a resourceId, changeId and positive requestedAtEpochMillis, but received scope '%s', resource '%s', change '%s' and timestamp '%d'";
  private static final String MISSING_CHANGE =
      "INVALID_MEMBER_CHANGE: Expected to request member change '%s' for scope '%s' and resource '%s', but no additions, removals or target count were supplied";
  private static final String INVALID_PLAN_IDENTITY =
      "INVALID_MEMBER_CHANGE_PLAN: Expected the planner to preserve identity of member change '%s' for scope '%s' and resource '%s', but the plan is missing or has a different identity";
  private static final String MISSING_OPERATIONS =
      "INVALID_MEMBER_CHANGE_PLAN: Expected member change '%s' for scope '%s' and resource '%s' to contain operations unless COMPLETED, but status '%s' has no operations";
  private static final String UNFROZEN_OPERATION =
      "UNFROZEN_OPERATION: Expected partition operation '%s' for member change '%s' and resource '%s' to have a frozen configuration, but none was provided";
  private final ProcessingState state;
  private final KeyGenerator keyGenerator;
  private final List<MemberChangePlanner> planners;

  public MemberRequestProcessor(
      final TypedRecordProcessorContext context, final List<MemberChangePlanner> planners) {
    writers = context.getWriters();
    state = context.getProcessingState(ProcessingState.class);
    keyGenerator = context.getKeyGenerator();
    this.planners = List.copyOf(planners);
  }

  @Override
  public void processRecord(final TypedRecord<MemberChangeRecord> record) {
    final MemberChangeRecord request = record.getValue();
    try {
      validate(request);
      final MemberChangePlanner planner =
          planners.stream()
              .filter(candidate -> candidate.scope() == request.getScope())
              .findFirst()
              .orElseThrow(
                  () ->
                      new MemberChangeRejection(
                          RejectionType.INVALID_ARGUMENT,
                          UNSUPPORTED_SCOPE.formatted(
                              request.getChangeId(), request.getResourceId(), request.getScope())));
      final MemberChangeRecord active =
          state
              .getMemberChangeState()
              .active(request.getScope(), request.getResourceId())
              .orElse(null);
      if (active != null) {
        if (active.getChangeId().equals(request.getChangeId())) {
          writers.state().appendFollowUpEvent(record.getKey(), MemberChangeIntent.PLANNED, active);
          writers
              .response()
              .writeEventOnCommand(record.getKey(), MemberChangeIntent.PLANNED, active, record);
        } else if (sameRequest(active, request)) {
          writers.state().appendFollowUpEvent(record.getKey(), MemberChangeIntent.PLANNED, active);
          writers
              .response()
              .writeEventOnCommand(record.getKey(), MemberChangeIntent.PLANNED, active, record);
        } else {
          final MemberChangeRecord requestedTarget =
              planner.calculate(request, state).memberChange();
          if (sameTarget(active, requestedTarget)) {
            writers
                .state()
                .appendFollowUpEvent(record.getKey(), MemberChangeIntent.PLANNED, active);
            writers
                .response()
                .writeEventOnCommand(record.getKey(), MemberChangeIntent.PLANNED, active, record);
          } else {
            throw new MemberChangeRejection(
                RejectionType.INVALID_STATE,
                CHANGE_IN_PROGRESS.formatted(
                    request.getChangeId(),
                    request.getScope(),
                    request.getResourceId(),
                    active.getChangeId()));
          }
        }
        return;
      }

      final MemberChangePlan plan = planner.calculate(request, state);
      final MemberChangeRecord planned = plan.memberChange();
      validatePlan(request, plan);
      plan.partitionChanges()
          .forEach(
              partitionChange ->
                  writers
                      .state()
                      .appendFollowUpEvent(
                          keyGenerator.nextKey(),
                          ValueType.PARTITION_CHANGE,
                          BrokerPartitionChangeIntent.SUBMITTED,
                          partitionChange));
      writers.state().appendFollowUpEvent(record.getKey(), MemberChangeIntent.PLANNED, planned);
      writers
          .response()
          .writeEventOnCommand(record.getKey(), MemberChangeIntent.PLANNED, planned, record);
    } catch (final MemberChangeRejection rejection) {
      writers.rejection().appendRejection(rejection.rejectionType(), rejection.getMessage());
      writers
          .response()
          .writeRejectionOnCommand(record, rejection.rejectionType(), rejection.getMessage());
    }
  }

  private static void validate(final MemberChangeRecord request) {
    if (request.getResourceId().isBlank()
        || request.getChangeId().isBlank()
        || request.getRequestedAtEpochMillis() <= 0) {
      throw new MemberChangeRejection(
          RejectionType.INVALID_ARGUMENT,
          INVALID_IDENTITY.formatted(
              request.getScope(),
              request.getResourceId(),
              request.getChangeId(),
              request.getRequestedAtEpochMillis()));
    }
    if (request.getRequestedAdditions().isEmpty()
        && request.getRequestedRemovals().isEmpty()
        && request.getRequestedCount() == null) {
      throw new MemberChangeRejection(
          RejectionType.INVALID_ARGUMENT,
          MISSING_CHANGE.formatted(
              request.getChangeId(), request.getScope(), request.getResourceId()));
    }
  }

  private static void validatePlan(final MemberChangeRecord request, final MemberChangePlan plan) {
    final MemberChangeRecord planned = plan.memberChange();
    if (planned == null
        || planned.getScope() != request.getScope()
        || !planned.getResourceId().equals(request.getResourceId())
        || !planned.getChangeId().equals(request.getChangeId())) {
      throw new MemberChangeRejection(
          RejectionType.PROCESSING_ERROR,
          INVALID_PLAN_IDENTITY.formatted(
              request.getChangeId(), request.getScope(), request.getResourceId()));
    }
    if (planned.getStatus() != MemberChangeStatus.COMPLETED && planned.getOperations().isEmpty()) {
      throw new MemberChangeRejection(
          RejectionType.PROCESSING_ERROR,
          MISSING_OPERATIONS.formatted(
              planned.getChangeId(),
              planned.getScope(),
              planned.getResourceId(),
              planned.getStatus()));
    }
    for (final var partitionChange : plan.partitionChanges()) {
      for (final var operation : partitionChange.getOperations()) {
        if (operation.configuration() == null) {
          throw new MemberChangeRejection(
              RejectionType.PROCESSING_ERROR,
              UNFROZEN_OPERATION.formatted(
                  operation.opId(), request.getChangeId(), request.getResourceId()));
        }
      }
    }
  }

  private static boolean sameTarget(
      final MemberChangeRecord active, final MemberChangeRecord requested) {
    return active.getTargetCount() == requested.getTargetCount()
        && Set.copyOf(active.getTargetVoters()).equals(Set.copyOf(requested.getTargetVoters()))
        && Set.copyOf(active.getRemovedMembers()).equals(Set.copyOf(requested.getRemovedMembers()));
  }

  private static boolean sameRequest(
      final MemberChangeRecord active, final MemberChangeRecord requested) {
    return Objects.equals(active.getRequestedCount(), requested.getRequestedCount())
        && Set.copyOf(active.getRequestedAdditions())
            .equals(Set.copyOf(requested.getRequestedAdditions()))
        && Set.copyOf(active.getRequestedRemovals())
            .equals(Set.copyOf(requested.getRequestedRemovals()));
  }
}
