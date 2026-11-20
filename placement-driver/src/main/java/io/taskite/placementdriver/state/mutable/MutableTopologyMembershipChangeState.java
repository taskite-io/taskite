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
package io.taskite.placementdriver.state.mutable;

import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionExecutionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;

public interface MutableTopologyMembershipChangeState extends TopologyMembershipChangeState {
  void markPartition(ChangePlan plan, boolean active);

  void markMember(MemberChangeRecord change, boolean active);

  void putPartition(PartitionExecutionRecord.Schedule schedule);

  void putMember(
      MemberChangeRecord.MemberChangeScope scope, MemberExecutionRecord.Schedule schedule);

  void removeActive(Scope scope, String clusterId, int partitionId);

  boolean deletePlan(Scope scope, String clusterId, String changeId);

  void deleteResource(Scope scope, String clusterId);
}
