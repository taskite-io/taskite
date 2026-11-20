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
package io.taskite.protocol.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.taskite.protocol.impl.record.value.placementdriver.partitionchange.PartitionChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyRecord;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeCancellationReason;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeStatus;
import io.taskite.protocol.record.value.placementdriver.topology.ChangeType;
import io.taskite.protocol.record.value.placementdriver.topology.ClusterConfigurationOutcome;
import io.taskite.protocol.record.value.placementdriver.topology.PlanBlockedReason;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.topology.BrokerPartitionLifecycle;
import io.taskite.topology.BrokerTopologyOperationStatus;
import io.taskite.topology.MemberRole;
import io.taskite.topology.PartitionStatus;
import io.taskite.topology.RaftNodeState;
import io.taskite.topology.RegistrationStatus;
import java.util.List;
import java.util.Set;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

final class StatusEnumContractTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void enumJsonNamesRoundTripWithoutChangingThePublicContract() throws Exception {
    for (final Class<? extends Enum<?>> type :
        List.<Class<? extends Enum<?>>>of(
            MemberRole.class,
            RegistrationStatus.class,
            RaftNodeState.class,
            PartitionStatus.class,
            BrokerPartitionLifecycle.class,
            BrokerTopologyOperationStatus.class,
            TopologyUpdateType.class,
            ChangeStatus.class,
            ClusterConfigurationOutcome.class)) {
      for (final Enum<?> value : type.getEnumConstants()) {
        final String json = MAPPER.writeValueAsString(value);
        assertEquals("\"" + value.name() + "\"", json, type.getSimpleName());
        assertEquals(value, MAPPER.readValue(json, type));
      }
      if (type == BrokerPartitionLifecycle.class) {
        assertEquals(BrokerPartitionLifecycle.ABSENT, MAPPER.readValue("\"INVALID_STATE\"", type));
      } else if (type == BrokerTopologyOperationStatus.class) {
        assertEquals(
            BrokerTopologyOperationStatus.NONE, MAPPER.readValue("\"INVALID_STATE\"", type));
      } else {
        assertThrows(Exception.class, () -> MAPPER.readValue("\"INVALID_STATE\"", type));
      }
    }
  }

  @Test
  void optionalReadStatesRemainUnassignedForOldEmptyJson() throws Exception {
    for (final Class<? extends Enum<?>> type :
        List.<Class<? extends Enum<?>>>of(
            TopologyUpdateType.class,
            ChangeStatus.class,
            ClusterConfigurationOutcome.class,
            PartitionStatus.class)) {
      assertNull(MAPPER.readValue("\"\"", type));
      assertNull(MAPPER.readValue("null", type));
    }
    final var topology =
        MAPPER.readValue(
            "{\"partitions\":[{\"partitionId\":1,\"status\":\"\"}]}", TopologyRecord.class);
    assertNull(topology.getPartitions().getFirst().getStatus());
  }

  @Test
  void oldMissingMemberObservationsHaveNoInventedOnlineOrVoterState() throws Exception {
    for (final String json :
        List.of(
            "{\"memberId\":\"pd-0\"}", "{\"memberId\":\"pd-0\",\"role\":\"\",\"status\":\"\"}")) {
      final var member = MAPPER.readValue(json, MemberTopologyRecord.MemberRecord.class);
      assertEquals(MemberRole.UNKNOWN, member.getRole());
      assertEquals(RegistrationStatus.UNKNOWN, member.getStatus());
      assertFalse(member.getRole().isAdmitted());
      assertFalse(member.getRole().isVoter());
    }
    assertEquals(BrokerPartitionLifecycle.ABSENT, BrokerPartitionLifecycle.parse("FUTURE"));
    assertEquals(BrokerTopologyOperationStatus.NONE, BrokerTopologyOperationStatus.parse("FUTURE"));
  }

  @Test
  void voterAndAdmissionRolesRemainDistinct() {
    final var voters = Set.of(MemberRole.VOTER, MemberRole.LEADER, MemberRole.FOLLOWER);
    for (final MemberRole role : MemberRole.values()) {
      assertEquals(voters.contains(role), role.isVoter(), role.name());
      assertEquals(
          voters.contains(role) || role == MemberRole.LEARNER, role.isAdmitted(), role.name());
    }
  }

  @Test
  void barriersAndChangeKindsRetainHistoricalEmptyNames() throws Exception {
    for (final Class<? extends Enum<?>> type :
        List.<Class<? extends Enum<?>>>of(
            PlanBlockedReason.class, ChangeType.class, ChangeCancellationReason.class)) {
      for (final Enum<?> value : type.getEnumConstants()) {
        final String json = MAPPER.writeValueAsString(value);
        assertEquals("\"" + (value.name().equals("NONE") ? "" : value.name()) + "\"", json);
        assertEquals(value, MAPPER.readValue(json, type));
      }
      assertEquals(type.getEnumConstants()[0], MAPPER.readValue("\"\"", type));
      assertThrows(Exception.class, () -> MAPPER.readValue("\"unbounded-detail\"", type));
    }
    assertEquals(PlanBlockedReason.NO_QUORUM, PlanBlockedReason.fromFailure("NO_QUORUM"));
    assertEquals(
        PlanBlockedReason.CONFIGURATION_READ_UNAVAILABLE,
        PlanBlockedReason.fromFailure("external failure"));
  }

  @Test
  void partitionBlockedReasonsRoundTripInSbeAndJson() throws Exception {
    for (final PlanBlockedReason reason : PlanBlockedReason.values()) {
      final var record = new PartitionChangeRecord().setBlockedReason(reason);
      final byte[] bytes = new byte[record.getLength()];
      record.write(new UnsafeBuffer(bytes), 0);
      final var restored = new PartitionChangeRecord();
      restored.wrap(new UnsafeBuffer(bytes));
      assertEquals(reason, restored.getBlockedReason());
      assertEquals(
          reason.wireValue(), MAPPER.readTree(record.toJson()).path("blockedReason").asText());
    }
  }
}
