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
package io.taskite.placementdriver.state.appliers;

import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.record.intent.placementdriver.MemberChangeIntent;
import io.taskite.streamplatform.api.state.TypedEventApplier;

public final class MemberLeaderTransferredApplier
    implements TypedEventApplier<MemberChangeIntent, MemberChangeRecord> {
  private final MemberChangeStateUpdater behavior;

  public MemberLeaderTransferredApplier(final MutableProcessingState state) {
    behavior = new MemberChangeStateUpdater(state);
  }

  @Override
  public void applyState(final long key, final MemberChangeRecord value) {
    behavior.update(value);
  }
}
