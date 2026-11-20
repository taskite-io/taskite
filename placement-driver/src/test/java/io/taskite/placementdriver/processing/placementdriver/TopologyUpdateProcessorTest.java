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
package io.taskite.placementdriver.processing.placementdriver;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.taskite.placementdriver.PlacementDriverCluster;
import io.taskite.placementdriver.state.appliers.PlacementDriverTopologyUpdatedApplier;
import io.taskite.placementdriver.state.immutable.ProcessingState;
import io.taskite.placementdriver.state.mutable.MutableProcessingState;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperation;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationStatus;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeOperationType;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeStatus;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord.MemberRecord;
import io.taskite.protocol.record.RejectionType;
import io.taskite.protocol.record.TypedRecord;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.state.PartitionStorage;
import io.taskite.state.processing.StateStoreProcessingStore;
import io.taskite.streamplatform.api.processor.TypedRecordProcessorContext;
import io.taskite.streamplatform.api.writer.*;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

final class TopologyUpdateProcessorTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"", "pd-0-0", "previous-incarnation"})
  void configurationPreservesRegisteredIdentityAfterAsynchronousSnapshot(
      final String staleIdentity) {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      fixture.accept(registration("REGISTER", "current-incarnation", "127.0.0.1", 26501));
      final MemberRecord stale =
          member(staleIdentity, "127.0.0.1", 26501)
              .setRole(MemberRole.LEADER)
              .setEndpointEpoch(0)
              .setStatus(RegistrationStatus.UNKNOWN)
              .setCommitIndex(0)
              .setLastApplied(0)
              .setLastLogIndex(0)
              .setReadyForPromotion(false);

      final MemberTopologyRecord configured = fixture.accept(configuration(stale));
      final MemberRecord current = configured.getMembers().getFirst();
      assertEquals("current-incarnation", current.getIncarnationId());
      assertEquals(1, current.getEndpointEpoch());
      assertEquals(RegistrationStatus.ONLINE, current.getStatus());
      assertEquals(MemberRole.LEADER, current.getRole());
      assertEquals(1, configured.getConfigurationEpoch());
      assertEquals(42, current.getCommitIndex());
      assertEquals(40, current.getLastApplied());
      assertEquals(43, current.getLastLogIndex());
      assertTrue(current.isReadyForPromotion());
      assertEquals(
          1,
          fixture
              .accept(registration("RENEW", "current-incarnation", "127.0.0.1", 26501))
              .getConfigurationEpoch());
    }
  }

  @Test
  void staleConfigurationRetainsRegistrationWhileLearnerAdmissionIsPending() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      final MemberRecord seed = member("seed", "127.0.0.3", 28501).setMemberId("pd-1");
      fixture.accept(configuration(seed));
      fixture.accept(registration("REGISTER", "new", "127.0.0.1", 26501));
      final MemberTopologyRecord pending = fixture.accept(configuration(seed));
      assertEquals(2, pending.getMembers().size());
      assertEquals(MemberRole.REGISTERED, pending.getMembers().getLast().getRole());
      assertEquals("new", pending.getMembers().getLast().getIncarnationId());
      final MemberTopologyRecord admitted =
          fixture.accept(
              configuration(
                  seed,
                  member("seed-id", "127.0.0.1", 26501)
                      .setRole(MemberRole.LEARNER)
                      .setStatus(RegistrationStatus.UNKNOWN)));
      final MemberRecord current = admitted.getMembers().getLast();
      assertEquals("new", current.getIncarnationId());
      assertEquals(RegistrationStatus.ONLINE, current.getStatus());
      assertEquals(MemberRole.LEARNER, current.getRole());
      fixture.accept(registration("RENEW", "new", "127.0.0.1", 26501));
      assertEquals(1, fixture.accept(configuration(seed)).getMembers().size());
    }
  }

  @Test
  void promotionPreservesLearnerRegistrationAcrossIntermediateRaftConfiguration() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      final MemberRecord seed = member("seed", "127.0.0.3", 28501).setMemberId("pd-1");
      fixture.accept(configuration(seed));
      fixture.accept(registration("REGISTER", "current", "127.0.0.1", 26501));
      final long admittedEpoch =
          fixture
              .accept(
                  configuration(
                      seed, member("current", "127.0.0.1", 26501).setRole(MemberRole.LEARNER)))
              .getConfigurationEpoch();
      fixture.storeChange(promotion(MemberChangeStatus.IN_PROGRESS));

      final MemberTopologyRecord intermediate = fixture.accept(configuration(seed));
      assertEquals(2, intermediate.getMembers().size());
      final MemberRecord retained = intermediate.getMembers().getLast();
      assertEquals(MemberRole.LEARNER, retained.getRole());
      assertEquals("current", retained.getIncarnationId());
      assertEquals(1, retained.getEndpointEpoch());
      assertEquals(admittedEpoch, intermediate.getConfigurationEpoch());
      fixture.accept(registration("RENEW", "current", "127.0.0.1", 26501));
      fixture.reject(
          registration("RENEW", "old", "127.0.0.1", 26501),
          "STALE_PD_MEMBER_ENDPOINT_EPOCH: Expected to renew PD member 'pd-0' with incarnation 'old' and endpoint epoch '0', but the registered incarnation is 'current' with endpoint epoch '1'");

      final MemberTopologyRecord promoted =
          fixture.accept(
              configuration(
                  seed, member("current", "127.0.0.1", 26501).setRole(MemberRole.FOLLOWER)));
      assertEquals(MemberRole.FOLLOWER, promoted.getMembers().getLast().getRole());
      assertEquals(admittedEpoch + 1, promoted.getConfigurationEpoch());
      fixture
          .state
          .getMemberChangeState()
          .put(promotion(MemberChangeStatus.COMPLETED).setNextOperationIndex(1));
      assertEquals(1, fixture.accept(configuration(seed)).getMembers().size());
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = MemberChangeStatus.class,
      names = {"PENDING", "WAITING_CATCH_UP", "CANCELLING", "COMPLETED", "CANCELLED", "FAILED"})
  void inactivePromotionCannotPreventMemberRemoval(final MemberChangeStatus status) {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      final MemberRecord seed = member("seed", "127.0.0.3", 28501).setMemberId("pd-1");
      fixture.accept(
          configuration(seed, member("current", "127.0.0.1", 26501).setRole(MemberRole.LEARNER)));
      fixture.storeChange(promotion(status));
      assertEquals(1, fixture.accept(configuration(seed)).getMembers().size());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"another-member", "future-operation", "leave-operation", "broker-scope"})
  void unrelatedPromotionCannotPreventMemberRemoval(final String scenario) {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      final MemberRecord seed = member("seed", "127.0.0.3", 28501).setMemberId("pd-1");
      fixture.accept(
          configuration(seed, member("current", "127.0.0.1", 26501).setRole(MemberRole.LEARNER)));
      final var change = promotion(MemberChangeStatus.IN_PROGRESS);
      switch (scenario) {
        case "another-member" -> change.getOperations().getFirst().setMemberId("pd-2");
        case "future-operation" ->
            change.setOperations(
                List.of(
                    new MemberChangeOperation()
                        .setOperationId("join")
                        .setMemberId("pd-0")
                        .setType(MemberChangeOperationType.MEMBER_ADD_LEARNER)
                        .setStatus(MemberChangeOperationStatus.IN_PROGRESS),
                    change.getOperations().getFirst()));
        case "leave-operation" ->
            change.getOperations().getFirst().setType(MemberChangeOperationType.MEMBER_REMOVE);
        case "broker-scope" -> change.setScope(MemberChangeScope.BROKER);
        default -> fail("unknown scenario");
      }
      fixture.storeChange(change);
      assertEquals(1, fixture.accept(configuration(seed)).getMembers().size());
    }
  }

  private static MemberChangeRecord promotion(final MemberChangeStatus status) {
    return new MemberChangeRecord()
        .setScope(MemberChangeScope.PLACEMENT_DRIVER)
        .setResourceId(PlacementDriverCluster.CLUSTER_ID)
        .setChangeId("promotion")
        .setStatus(status)
        .setOperations(
            List.of(
                new MemberChangeOperation()
                    .setOperationId("promote")
                    .setMemberId("pd-0")
                    .setType(MemberChangeOperationType.MEMBER_PROMOTE_VOTER)
                    .setStatus(MemberChangeOperationStatus.IN_PROGRESS)));
  }

  @Test
  void configurationPreservesReboundEndpointsAndRejectsOldIncarnation() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      fixture.accept(
          configuration(member("pd-0-0", "127.0.0.1", 26501).setRole(MemberRole.FOLLOWER)));
      fixture.accept(registration("REGISTER", "old-incarnation", "127.0.0.1", 26501));
      final MemberTopologyRecord rebound =
          fixture.accept(registration("REGISTER", "new-incarnation", "127.0.0.1", 26501));
      final long epoch = rebound.getMembers().getFirst().getEndpointEpoch();

      final MemberTopologyRecord configured =
          fixture.accept(
              configuration(
                  member("old-incarnation", "127.0.0.1", 26501)
                      .setEndpointEpoch(epoch - 1)
                      .setRole(MemberRole.LEADER)));
      final MemberRecord current = configured.getMembers().getFirst();
      assertEquals("new-incarnation", current.getIncarnationId());
      assertEquals(epoch, current.getEndpointEpoch());
      assertEquals("127.0.0.1", current.getHost());
      assertEquals(26500, current.getHttpPort());
      assertEquals(26501, current.getGrpcPort());
      assertEquals(26502, current.getRaftPort());
      assertEquals("127.0.0.1:26501", current.getEndpoint());
      assertEquals(current.getEndpoint(), configured.getLeaderEndpoint());
      assertEquals(MemberRole.LEADER, current.getRole());
      assertEquals(1, configured.getConfigurationEpoch(), "leader election does not change voters");
      fixture.accept(registration("RENEW", "new-incarnation", "127.0.0.1", 26501));
      fixture.reject(
          registration("RENEW", "old-incarnation", "127.0.0.1", 26501),
          "STALE_PD_MEMBER_ENDPOINT_EPOCH: Expected to renew PD member 'pd-0' with incarnation 'old-incarnation' and endpoint epoch '0', but the registered incarnation is 'new-incarnation' with endpoint epoch '2'");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"REGISTER", "REBIND", "RENEW"})
  void rejectsExistingMemberIdAtAnotherEndpoint(final String type) {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      fixture.accept(registration("REGISTER", "current", "127.0.0.1", 26501));
      for (final MemberRecord conflicting :
          List.of(
              member("current", "127.0.0.2", 26501),
              member("next", "127.0.0.1", 27501),
              member("next", "127.0.0.1", 26501).setRaftPort(27502))) {
        fixture.reject(
            new MemberTopologyRecord()
                .setUpdateType(TopologyUpdateType.fromJson(type))
                .setMembers(List.of(conflicting)),
            "PD_MEMBER_ID_CONFLICT: Member 'pd-0' is registered at host '127.0.0.1', gRPC port '26501' and Raft port '26502'; cannot register at host '%s', gRPC port '%d' and Raft port '%d'"
                .formatted(
                    conflicting.getHost(), conflicting.getGrpcPort(), conflicting.getRaftPort()));
        final MemberRecord current =
            fixture
                .state
                .getPlacementDriverTopologyState()
                .topology()
                .orElseThrow()
                .getMembers()
                .getFirst();
        assertEquals("current", current.getIncarnationId());
        assertEquals(1, current.getEndpointEpoch());
      }
    }
  }

  @Test
  void removedIdCanRegisterAtANewEndpoint() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      fixture.accept(registration("REGISTER", "old", "127.0.0.1", 26501));
      final MemberRecord seed = member("seed", "127.0.0.3", 28501).setMemberId("pd-1");
      fixture.accept(
          configuration(seed, member("old", "127.0.0.1", 26501).setRole(MemberRole.LEARNER)));
      fixture.accept(configuration(seed));
      final MemberTopologyRecord registered =
          fixture.accept(registration("REGISTER", "new", "127.0.0.2", 27501));
      assertEquals("pd-0", registered.getLocalMemberId());
      final MemberRecord member = registered.getMembers().getLast();
      assertEquals(MemberRole.REGISTERED, member.getRole());
      assertEquals("127.0.0.2", member.getHost());
      assertEquals(1, member.getEndpointEpoch());
    }
  }

  @Test
  void configurationKeepsSeedEndpointsAndRemovesAbsentRegisteredMember() {
    try (var storage = new PartitionStorage(directory)) {
      final var fixture = new Fixture(storage);
      final MemberRecord seed = member("pd-1-0", "127.0.0.3", 28501).setMemberId("pd-1");
      final MemberTopologyRecord initial =
          fixture.accept(configuration(member("pd-0-0", "127.0.0.1", 26501), seed));
      assertEquals("127.0.0.3:28501", initial.getMembers().get(1).getEndpoint());
      assertEquals(0, initial.getMembers().get(1).getEndpointEpoch());
      fixture.accept(registration("REGISTER", "current-incarnation", "127.0.0.1", 26501));

      final MemberTopologyRecord removed = fixture.accept(configuration(seed));
      assertEquals(
          List.of("pd-1"), removed.getMembers().stream().map(MemberRecord::getMemberId).toList());
      assertEquals(2, removed.getConfigurationEpoch());
      fixture.reject(
          registration("RENEW", "current-incarnation", "127.0.0.1", 26501),
          "PD_MEMBER_MUST_REGISTER_BEFORE_RENEW: Expected to renew PD member 'pd-0' with incarnation 'current-incarnation', but no registration was found");
    }
  }

  private static MemberRecord member(final String identity, final String host, final int grpcPort) {
    return new MemberRecord()
        .setMemberId("pd-0")
        .setHost(host)
        .setHttpPort(grpcPort - 1)
        .setGrpcPort(grpcPort)
        .setRaftPort(grpcPort + 1)
        .setEndpoint(host + ":" + grpcPort)
        .setIncarnationId(identity)
        .setRole(MemberRole.FOLLOWER)
        .setStatus(RegistrationStatus.ONLINE)
        .setCommitIndex(42)
        .setLastApplied(40)
        .setLastLogIndex(43)
        .setReadyForPromotion(true);
  }

  private static MemberTopologyRecord registration(
      final String type, final String identity, final String host, final int grpcPort) {
    return new MemberTopologyRecord()
        .setUpdateType(TopologyUpdateType.fromJson(type))
        .setLocalMemberId("pd-0")
        .setMembers(List.of(member(identity, host, grpcPort)));
  }

  private static MemberTopologyRecord configuration(final MemberRecord... members) {
    return new MemberTopologyRecord()
        .setUpdateType(TopologyUpdateType.CONFIGURATION)
        .setLeaderId("pd-0")
        .setLeaderEndpoint("127.0.0.1:26501")
        .setTerm(2)
        .setMembers(List.of(members));
  }

  private static final class Fixture {
    final MutableProcessingState state;
    final StateWriter events = mock(StateWriter.class);
    final TypedRejectionWriter rejection = mock(TypedRejectionWriter.class);
    final TypedResponseWriter response = mock(TypedResponseWriter.class);
    final TopologyUpdateProcessor processor;

    Fixture(final PartitionStorage storage) {
      state = new MutableProcessingState(new StateStoreProcessingStore(storage));
      final Writers writers = mock(Writers.class);
      when(writers.state()).thenReturn(events);
      when(writers.rejection()).thenReturn(rejection);
      when(writers.response()).thenReturn(response);
      final TypedRecordProcessorContext context = mock(TypedRecordProcessorContext.class);
      when(context.getWriters()).thenReturn(writers);
      when(context.getProcessingState(ProcessingState.class)).thenReturn(state);
      processor = new TopologyUpdateProcessor(context);
    }

    void process(final MemberTopologyRecord value) {
      clearInvocations(events, rejection, response);
      final TypedRecord<MemberTopologyRecord> record = mock(TypedRecord.class);
      when(record.getKey()).thenReturn(1L);
      when(record.getValue()).thenReturn(value);
      processor.processRecord(record);
    }

    void storeChange(final MemberChangeRecord change) {
      state.getMemberChangeState().put(change);
      state.getMemberChangeState().setActive(change);
    }

    MemberTopologyRecord accept(final MemberTopologyRecord value) {
      process(value);
      verifyNoInteractions(rejection);
      final var captured = ArgumentCaptor.forClass(MemberTopologyRecord.class);
      verify(events).appendFollowUpEvent(eq(1L), eq(TopologyIntent.UPDATED), captured.capture());
      new PlacementDriverTopologyUpdatedApplier(state).applyState(1L, captured.getValue());
      return state.getPlacementDriverTopologyState().topology().orElseThrow();
    }

    void reject(final MemberTopologyRecord value, final String reason) {
      process(value);
      verify(rejection).appendRejection(RejectionType.INVALID_STATE, reason);
      verifyNoInteractions(events);
    }
  }
}
