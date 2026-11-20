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
package io.taskite.placementdriver.runtime;

import io.taskite.protocol.impl.record.value.placementdriver.topology.MemberTopologyRecord;
import io.taskite.protocol.record.value.placementdriver.topology.TopologyUpdateType;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.topology.MemberRole;
import io.taskite.topology.RegistrationStatus;
import io.taskite.util.HostPort;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers and renews the local PD endpoint through the current PD leader. */
public final class PlacementDriverMemberRegistrationActor implements AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementDriverMemberRegistrationActor.class);
  private static final Duration RENEW_INTERVAL = Duration.ofSeconds(5);
  private static final Duration ADMISSION_RENEW_INTERVAL = Duration.ofMillis(200);

  private final String memberId;
  private final String host;
  private final int httpPort;
  private final int grpcPort;
  private final int raftPort;
  private final String incarnationId = UUID.randomUUID().toString();
  private final ConcurrencyControl actor;
  private final BooleanSupplier localMember;
  private final Runnable localMemberRemoved;
  private final Supplier<PlacementDriverNodeSnapshot> snapshotSupplier;
  private final Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> topologyUpdater;
  private final CompletableActorFuture<Void> initialRegistration = new CompletableActorFuture<>();
  private long endpointEpoch = 1;
  private Duration renewInterval = ADMISSION_RENEW_INTERVAL;
  private boolean registered;
  private boolean admitted;
  private boolean closed;
  private ScheduledTimer timer;

  public PlacementDriverMemberRegistrationActor(final Endpoint endpoint, final Context context) {
    memberId = endpoint.memberId();
    host = endpoint.host();
    httpPort = endpoint.httpPort();
    grpcPort = endpoint.grpcPort();
    raftPort = endpoint.raftPort();
    actor = context.actor();
    localMember = context.localMember();
    localMemberRemoved = Objects.requireNonNull(context.localMemberRemoved(), "localMemberRemoved");
    snapshotSupplier = context.snapshotSupplier();
    topologyUpdater = context.topologyUpdater();
  }

  public record Endpoint(String memberId, String host, int httpPort, int grpcPort, int raftPort) {}

  public record Context(
      ConcurrencyControl actor,
      BooleanSupplier localMember,
      Runnable localMemberRemoved,
      Supplier<PlacementDriverNodeSnapshot> snapshotSupplier,
      Function<MemberTopologyRecord, ActorFuture<MemberTopologyRecord>> topologyUpdater) {}

  public ActorFuture<Void> start() {
    actor.run(this::submitAndSchedule);
    return initialRegistration;
  }

  private void submitAndSchedule() {
    if (closed) {
      return;
    }
    if (admitted && !localMember.getAsBoolean()) {
      closed = true;
      LOG.info("stop renewing removed placement-driver member {}", memberId);
      localMemberRemoved.run();
      return;
    }
    final PlacementDriverNodeSnapshot snapshot = snapshotSupplier.get();
    final MemberTopologyRecord value =
        new MemberTopologyRecord()
            .setUpdateType(registered ? TopologyUpdateType.RENEW : TopologyUpdateType.REGISTER)
            .setLocalMemberId(memberId)
            .setMembers(
                List.of(
                    new MemberTopologyRecord.MemberRecord()
                        .setMemberId(memberId)
                        .setEndpoint(HostPort.format(host, grpcPort))
                        .setRole(MemberRole.LEARNER)
                        .setSelf(true)
                        .setHost(host)
                        .setHttpPort(httpPort)
                        .setGrpcPort(grpcPort)
                        .setRaftPort(raftPort)
                        .setStatus(RegistrationStatus.ONLINE)
                        .setIncarnationId(incarnationId)
                        .setEndpointEpoch(endpointEpoch)
                        .setCommitIndex(snapshot.commitIndex())
                        .setLastApplied(snapshot.lastApplied())
                        .setLastLogIndex(snapshot.lastLogIndex())));
    actor.runOnCompletion(
        topologyUpdater.apply(value),
        (topology, error) -> {
          if (closed) {
            return;
          }
          if (error == null) {
            topology.getMembers().stream()
                .filter(member -> memberId.equals(member.getMemberId()))
                .findFirst()
                .ifPresent(
                    member -> {
                      endpointEpoch = member.getEndpointEpoch();
                      admitted |= member.getRole().isAdmitted();
                      renewInterval =
                          member.getRole() == MemberRole.REGISTERED
                                  || member.getRole() == MemberRole.LEARNER
                              ? ADMISSION_RENEW_INTERVAL
                              : RENEW_INTERVAL;
                    });
            registered = true;
            if (!initialRegistration.isDone()) {
              initialRegistration.complete(null);
            }
          } else {
            LOG.debug("failed to register/renew placement-driver member {}", memberId, error);
            if (!registered && hasReason(error, "PD_MEMBER_ID_CONFLICT")) {
              closed = true;
              initialRegistration.completeExceptionally(error);
              return;
            }
            if (registered && registrationRevoked(error)) {
              closed = true;
              LOG.info("stop removed or superseded placement-driver member {}", memberId);
              localMemberRemoved.run();
              return;
            }
          }
          schedule();
        });
  }

  private static boolean registrationRevoked(final Throwable error) {
    return hasReason(error, "PD_MEMBER_MUST_REGISTER_BEFORE_RENEW")
        || hasReason(error, "STALE_PD_MEMBER_ENDPOINT_EPOCH")
        || hasReason(error, "PD_MEMBER_ID_CONFLICT");
  }

  private static boolean hasReason(final Throwable error, final String reason) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      final String message = cause.getMessage();
      if (message != null && message.contains(reason)) {
        return true;
      }
    }
    return false;
  }

  private void schedule() {
    if (!closed) {
      timer = actor.schedule(renewInterval, this::submitAndSchedule);
    }
  }

  @Override
  public void close() {
    closed = true;
    if (!initialRegistration.isDone()) {
      initialRegistration.completeExceptionally(
          new IllegalStateException("placement-driver registration closed before completion"));
    }
    if (timer != null) {
      timer.cancel();
    }
  }
}
