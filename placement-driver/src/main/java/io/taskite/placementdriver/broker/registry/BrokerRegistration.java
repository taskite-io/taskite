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
package io.taskite.placementdriver.broker.registry;

import io.taskite.topology.BrokerPartitionState;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record BrokerRegistration(
    Identity identity,
    Endpoint endpoint,
    Lease lease,
    BrokerStatus status,
    List<BrokerPartitionState> partitions) {
  public static final long DEFAULT_RENEWAL_TIMEOUT_MILLIS = 30_000L;

  public BrokerRegistration {
    Objects.requireNonNull(identity, "identity");
    Objects.requireNonNull(endpoint, "endpoint");
    Objects.requireNonNull(lease, "lease");
    partitions = List.copyOf(partitions == null ? List.of() : partitions);
  }

  public String clusterId() {
    return identity.clusterId();
  }

  public String brokerId() {
    return identity.brokerId();
  }

  public String version() {
    return identity.version();
  }

  public String incarnationId() {
    return identity.incarnationId();
  }

  public String host() {
    return endpoint.host();
  }

  public int port() {
    return endpoint.port();
  }

  public int raftPort() {
    return endpoint.raftPort();
  }

  public Instant registeredAt() {
    return lease.registeredAt();
  }

  public Instant lastRenewal() {
    return lease.lastRenewal();
  }

  public long renewalTimeoutMillis() {
    return lease.renewalTimeoutMillis();
  }

  public BrokerRegistration renew(
      Instant renewedAt, BrokerStatus newStatus, List<BrokerPartitionState> newPartitions) {
    return new BrokerRegistration(
        identity,
        endpoint,
        new Lease(registeredAt(), renewedAt, renewalTimeoutMillis()),
        newStatus,
        newPartitions == null ? partitions : newPartitions);
  }

  public BrokerRegistration renew(Instant renewedAt, BrokerStatus newStatus) {
    return renew(renewedAt, newStatus, partitions);
  }

  public record Identity(String clusterId, String brokerId, String version, String incarnationId) {
    public Identity {
      incarnationId = incarnationId == null ? "" : incarnationId;
    }
  }

  public record Endpoint(String host, int port, int raftPort) {
    public Endpoint {
      if (port <= 0 || port > 65_535) {
        throw new IllegalArgumentException("port must be between 1 and 65535");
      }
      if (raftPort <= 0 || raftPort > 65_535) {
        throw new IllegalArgumentException("raftPort must be between 1 and 65535");
      }
      if (raftPort == port) {
        throw new IllegalArgumentException("raftPort must be different from the business API port");
      }
    }
  }

  public record Lease(Instant registeredAt, Instant lastRenewal, long renewalTimeoutMillis) {
    public Lease {
      if (renewalTimeoutMillis <= 0) {
        throw new IllegalArgumentException("renewalTimeoutMillis must be positive");
      }
    }
  }
}
