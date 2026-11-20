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

import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.processor.TypedRecordProcessor;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import java.util.Set;

public final class MemberPromoteProcessor implements TypedRecordProcessor<MemberChangeRecord> {
  private final TopologyMembershipChangeBehavior behavior;

  public MemberPromoteProcessor(final TypedRecordProcessorContext context) {
    behavior = new TopologyMembershipChangeBehavior(context);
  }

  @Override
  public void processRecord(final TypedRecord<MemberChangeRecord> record) {
    behavior.advance(
        record,
        MemberChangeIntent.PROMOTED,
        Set.of(MemberChangeOperationType.MEMBER_PROMOTE_VOTER));
  }
}
