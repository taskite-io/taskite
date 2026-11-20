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
package io.taskite.placementdriver.bootstrap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.taskite.cluster.raft.RaftPeer;
import io.taskite.placementdriver.PlacementDriverPeer;
import io.taskite.placementdriver.protocol.PlacementDriverGrpc;
import io.taskite.placementdriver.protocol.PlacementDriverMember;
import io.taskite.placementdriver.protocol.PlacementDriverTopology;
import io.taskite.placementdriver.protocol.WatchPlacementDriverTopologyRequest;
import io.taskite.topology.MemberRole;
import io.taskite.util.HostPort;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

final class PlacementDriverBootstrapDiscovery {
  private static final Duration SEED_TIMEOUT = Duration.ofMillis(750);
  private static final String MEMBER_ID_CONFLICT =
      "PD_MEMBER_ID_CONFLICT: Member '%s' already belongs to service %s (Raft port %d); cannot start service %s (Raft port %d)";

  List<RaftPeer> discover(final PlacementDriverStartupContext context) {
    for (final PlacementDriverPeer seed : context.config().consensus().bootstrapPeers()) {
      final PlacementDriverTopology topology = query(seed);
      if (isBootstrappable(topology)) {
        validateLocalIdentity(context, topology);
        context.bootstrapLeaderEndpoint(leaderEndpoint(topology));
        return bootstrapPeers(context, topology);
      }
    }
    return List.of();
  }

  private static void validateLocalIdentity(
      final PlacementDriverStartupContext context, final PlacementDriverTopology topology) {
    final var config = context.config();
    topology.getMembersList().stream()
        .filter(member -> config.nodeId().equals(member.getMemberId()))
        .filter(
            member ->
                !HostPort.normalizeHost(config.network().host())
                        .equals(
                            member.getHost().isBlank()
                                ? endpointHost(member.getEndpoint())
                                : HostPort.normalizeHost(member.getHost()))
                    || (config.network().grpcPort() > 0
                        && config.network().grpcPort() != member.getGrpcPort())
                    || config.network().raftPort() != member.getRaftPort())
        .findFirst()
        .ifPresent(
            member -> {
              throw new IllegalStateException(
                  MEMBER_ID_CONFLICT.formatted(
                      config.nodeId(),
                      member.getEndpoint(),
                      member.getRaftPort(),
                      HostPort.format(config.network().host(), config.network().grpcPort()),
                      config.network().raftPort()));
            });
  }

  private static PlacementDriverTopology query(final PlacementDriverPeer seed) {
    final ManagedChannel channel =
        ManagedChannelBuilder.forAddress(seed.host(), seed.grpcPort()).usePlaintext().build();
    final CountDownLatch completed = new CountDownLatch(1);
    final AtomicReference<PlacementDriverTopology> result = new AtomicReference<>();
    try {
      PlacementDriverGrpc.newStub(channel)
          .watchPlacementDriverTopology(
              WatchPlacementDriverTopologyRequest.getDefaultInstance(),
              new StreamObserver<>() {
                @Override
                public void onNext(final PlacementDriverTopology value) {
                  result.compareAndSet(null, value);
                  completed.countDown();
                }

                @Override
                public void onError(final Throwable throwable) {
                  completed.countDown();
                }

                @Override
                public void onCompleted() {
                  completed.countDown();
                }
              });
      completed.await(SEED_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      return result.get();
    } catch (final InterruptedException error) {
      Thread.currentThread().interrupt();
      return null;
    } finally {
      channel.shutdownNow();
    }
  }

  private static List<RaftPeer> bootstrapPeers(
      final PlacementDriverStartupContext context, final PlacementDriverTopology topology) {
    final List<RaftPeer> peers = new ArrayList<>();
    for (final PlacementDriverMember member : topology.getMembersList()) {
      if (!isCommittedMember(member)) {
        continue;
      }
      final int raftPort = member.getRaftPort();
      final String host =
          member.getHost().isBlank() ? endpointHost(member.getEndpoint()) : member.getHost();
      if (!host.isBlank() && raftPort > 0) {
        peers.add(
            new RaftPeer(
                member.getMemberId(),
                host,
                raftPort,
                MemberRole.parse(member.getRole()) != MemberRole.LEARNER));
      }
    }
    if (peers.stream().noneMatch(peer -> peer.brokerId().equals(context.config().nodeId()))) {
      peers.add(
          new RaftPeer(
              context.config().nodeId(),
              context.config().network().host(),
              context.config().network().raftPort(),
              false));
    }
    return List.copyOf(peers);
  }

  private static boolean isCommittedMember(final PlacementDriverMember member) {
    return MemberRole.parse(member.getRole()).isAdmitted();
  }

  static boolean isBootstrappable(final PlacementDriverTopology topology) {
    if (topology == null || topology.getLeaderId().isBlank()) {
      return false;
    }
    final Set<String> memberIds = new HashSet<>();
    boolean containsLeader = false;
    boolean containsVoter = false;
    for (final PlacementDriverMember member : topology.getMembersList()) {
      if (!isCommittedMember(member)) {
        continue;
      }
      final String memberId = member.getMemberId();
      final String host =
          member.getHost().isBlank() ? endpointHost(member.getEndpoint()) : member.getHost();
      final int grpcPort =
          member.getGrpcPort() > 0 ? member.getGrpcPort() : endpointPort(member.getEndpoint());
      final int raftPort = member.getRaftPort();
      if (memberId.isBlank()
          || !memberIds.add(memberId)
          || host.isBlank()
          || grpcPort <= 0
          || raftPort <= 0
          || raftPort > 65_535) {
        return false;
      }
      final boolean voter = MemberRole.parse(member.getRole()) != MemberRole.LEARNER;
      containsVoter |= voter;
      containsLeader |= voter && topology.getLeaderId().equals(memberId);
    }
    return containsLeader && containsVoter;
  }

  private static String leaderEndpoint(final PlacementDriverTopology topology) {
    if (!topology.getLeaderEndpoint().isBlank()) {
      return topology.getLeaderEndpoint();
    }
    return topology.getMembersList().stream()
        .filter(member -> topology.getLeaderId().equals(member.getMemberId()))
        .map(
            member -> {
              final String host =
                  member.getHost().isBlank()
                      ? endpointHost(member.getEndpoint())
                      : member.getHost();
              final int grpcPort =
                  member.getGrpcPort() > 0
                      ? member.getGrpcPort()
                      : endpointPort(member.getEndpoint());
              return HostPort.format(host, grpcPort);
            })
        .findFirst()
        .orElse("");
  }

  private static int endpointPort(final String endpoint) {
    final int separator = endpoint == null ? -1 : endpoint.lastIndexOf(':');
    if (separator <= 0 || separator == endpoint.length() - 1) {
      return 0;
    }
    try {
      return Integer.parseInt(endpoint.substring(separator + 1));
    } catch (final NumberFormatException ignored) {
      return 0;
    }
  }

  private static String endpointHost(final String endpoint) {
    final int separator = endpoint == null ? -1 : endpoint.lastIndexOf(':');
    return separator <= 0 ? "" : HostPort.normalizeHost(endpoint.substring(0, separator));
  }
}
