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
import io.taskite.protocol.record.ValueType;
import io.taskite.protocol.record.intent.placementdriver.TopologyIntent;
import io.taskite.scheduler.ConcurrencyControl;
import io.taskite.scheduler.ScheduledTimer;
import io.taskite.scheduler.future.ActorFuture;
import io.taskite.scheduler.future.CompletableActorFuture;
import io.taskite.streamplatform.api.processor.ProcessingScheduleService.CommandWriter;
import java.time.Duration;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class PlacementDriverClusterLeadershipListener implements AutoCloseable {
  private final String localNodeId;
  private final ConcurrencyControl concurrencyControl;
  private final BooleanSupplier localLeader;
  private final Supplier<MemberTopologyRecord> topologySupplier;
  private final CommandWriter commandWriter;
  private final Supplier<Optional<MemberTopologyRecord>> committedTopology;
  private final CompletableActorFuture<Void> ready = new CompletableActorFuture<>();
  private ScheduledTimer timer;
  private boolean closed;
  private long retryAt;
  private String submittedSignature = "";

  public PlacementDriverClusterLeadershipListener(
      final String localNodeId,
      final ConcurrencyControl concurrencyControl,
      final BooleanSupplier localLeader,
      final Supplier<MemberTopologyRecord> topologySupplier,
      final Supplier<Optional<MemberTopologyRecord>> committedTopology,
      final CommandWriter commandWriter) {
    this.localNodeId = localNodeId;
    this.concurrencyControl = concurrencyControl;
    this.localLeader = localLeader;
    this.topologySupplier = topologySupplier;
    this.committedTopology = committedTopology;
    this.commandWriter = commandWriter;
  }

  public void onClusterLeadershipChanged() {
    requestPlacementDriverClusterTopologyUpdate();
  }

  public ActorFuture<Void> initializeClusterLeadership() {
    requestPlacementDriverClusterTopologyUpdate();
    return ready;
  }

  private void requestPlacementDriverClusterTopologyUpdate() {
    if (closed) {
      return;
    }
    if (!localLeader.getAsBoolean()) {
      submittedSignature = "";
      if (!ready.isDone()) {
        ready.complete(null);
      }
      return;
    }
    final MemberTopologyRecord topology = topologySupplier.get();
    if (topology.getLeaderEndpoint().isBlank() || !localNodeId.equals(topology.getLeaderId())) {
      schedule();
      return;
    }
    final String signature = signature(topology);
    if (committedTopology
        .get()
        .map(PlacementDriverClusterLeadershipListener::signature)
        .filter(signature::equals)
        .isPresent()) {
      submittedSignature = signature;
      if (!ready.isDone()) {
        ready.complete(null);
      }
      return;
    }
    if (!signature.equals(submittedSignature) || System.currentTimeMillis() >= retryAt) {
      if (commandWriter.tryAppend(0, ValueType.TOPOLOGY, TopologyIntent.UPDATE, topology)) {
        submittedSignature = signature;
        retryAt = System.currentTimeMillis() + 15_000;
      }
    }
    schedule();
  }

  private void schedule() {
    if (timer == null) {
      timer =
          concurrencyControl.schedule(
              Duration.ofMillis(100),
              () -> {
                timer = null;
                requestPlacementDriverClusterTopologyUpdate();
              });
    }
  }

  @Override
  public void close() {
    closed = true;
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    if (!ready.isDone()) {
      ready.completeExceptionally(new IllegalStateException("placement-driver runtime is closed"));
    }
  }

  private static String signature(final MemberTopologyRecord topology) {
    return topology.getTerm()
        + ":"
        + topology.getLeaderId()
        + ":"
        + topology.getLeaderEndpoint()
        + ":"
        + topology.getMembers().stream()
            .map(
                member ->
                    member.getMemberId()
                        + "@"
                        + member.getHost()
                        + ":"
                        + member.getRaftPort()
                        + ":"
                        + member.getRole()
                        + ":"
                        + member.getEndpointEpoch())
            .sorted()
            .collect(Collectors.joining(","));
  }
}
