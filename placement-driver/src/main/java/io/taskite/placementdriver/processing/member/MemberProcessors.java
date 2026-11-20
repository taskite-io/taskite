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

import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.processor.TypedRecordProcessors;
import java.util.List;

public final class MemberProcessors {
  private MemberProcessors() {}

  public static TypedRecordProcessors register(
      final TypedRecordProcessors processors,
      final TypedRecordProcessorContext context,
      final List<MemberChangePlanner> planners) {
    return processors
        .onCommand(
            ValueType.MEMBER_CHANGE,
            MemberChangeIntent.REQUEST,
            new MemberRequestProcessor(context, planners))
        .onCommand(
            ValueType.MEMBER_CHANGE,
            MemberChangeIntent.ADVANCE,
            new MemberAdvanceProcessor(context));
  }
}
