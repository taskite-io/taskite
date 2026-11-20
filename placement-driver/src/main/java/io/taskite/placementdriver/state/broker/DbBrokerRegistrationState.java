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
package io.taskite.placementdriver.state.broker;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.registry.BrokerRegistration;
import io.taskite.placementdriver.broker.registry.BrokerRegistryKey;
import io.taskite.placementdriver.broker.registry.BrokerStatus;
import io.taskite.placementdriver.state.broker.BrokerPlacementExclusionRecordValue.Exclusion;
import io.taskite.placementdriver.state.mutable.MutableBrokerRegistrationState;
import io.taskite.placementdriver.state.mutable.MutableRegistrationDeadlineState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.state.processing.ProcessingStateStore;
import io.taskite.topology.BrokerPartitionState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DbBrokerRegistrationState implements MutableBrokerRegistrationState {
  // [clusterId, brokerId] encoded as "clusterId:brokerId".
  private final DbString brokerKey;
  private final BrokerRegistrationRecordValue brokerValue;
  // PD_BROKER_REGISTRATIONS: ["clusterId:brokerId"] -> BrokerRegistrationRecordCodec bytes.
  private final ColumnFamily<DbString, BrokerRegistrationRecordValue>
      brokerRegistrationsColumnFamilies;
  // [clusterId, brokerId] encoded as "clusterId:brokerId".
  private final DbString exclusionKey;
  private final BrokerPlacementExclusionRecordValue exclusionValue;
  // PD_BROKER_PLACEMENT_EXCLUSIONS: ["clusterId:brokerId"]
  // -> [formatVersion (byte), removed incarnationId (UTF-8)].
  // Preserves voter exclusion and the startup fence used to admit a returned broker as a learner.
  private final ColumnFamily<DbString, BrokerPlacementExclusionRecordValue>
      placementExclusionsColumnFamilies;
  private final MutableRegistrationDeadlineState deadlines;

  public DbBrokerRegistrationState(
      final ProcessingStateStore storage, final MutableRegistrationDeadlineState deadlines) {
    brokerKey = new DbString();
    brokerValue = new BrokerRegistrationRecordValue();
    exclusionKey = new DbString();
    exclusionValue = new BrokerPlacementExclusionRecordValue();

    this.deadlines = deadlines;
    brokerRegistrationsColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_BROKER_REGISTRATIONS, brokerKey, brokerValue);
    placementExclusionsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_BROKER_PLACEMENT_EXCLUSIONS, exclusionKey, exclusionValue);
  }

  @Override
  public List<BrokerRegistration> registrations(final String clusterId) {
    final String prefix = clusterId + ":";
    final List<BrokerRegistration> registrations = new ArrayList<>();
    brokerRegistrationsColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            registrations.add(value.getRecord());
          }
        });
    registrations.sort(Comparator.comparing(BrokerRegistration::brokerId));
    return registrations;
  }

  @Override
  public Optional<BrokerRegistration> registration(final BrokerRegistryKey key) {
    return Optional.ofNullable(getRegistration(key.clusterId(), key.brokerId()));
  }

  @Override
  public boolean isVoterExcluded(final String clusterId, final String brokerId) {
    exclusionKey.wrapString(brokerKey(clusterId, brokerId));
    return placementExclusionsColumnFamilies.exists(exclusionKey);
  }

  @Override
  public boolean isLearnerExcluded(
      final String clusterId, final String brokerId, final String incarnationId) {
    final Exclusion exclusion = getExclusion(clusterId, brokerId);
    if (exclusion == null) {
      return false;
    }
    final String removedIncarnation =
        exclusion.legacy()
            ? registration(new BrokerRegistryKey(clusterId, brokerId))
                .map(BrokerRegistration::incarnationId)
                .orElse("")
            : exclusion.incarnationId();
    // An empty incarnation cannot prove that this is a new startup.
    return incarnationId.isBlank() || incarnationId.equals(removedIncarnation);
  }

  @Override
  public void excludeFromPlacement(final String clusterId, final String brokerId) {
    final var current = registration(new BrokerRegistryKey(clusterId, brokerId));
    if (current.isEmpty() && isVoterExcluded(clusterId, brokerId)) {
      // The final partition ACK can remove the registration before the parent completes.
      // Keep the captured startup fence when replaying subsequent removal status events.
      return;
    }
    putExclusion(clusterId, brokerId, current.map(BrokerRegistration::incarnationId).orElse(""));
  }

  private void putExclusion(
      final String clusterId, final String brokerId, final String incarnationId) {
    exclusionKey.wrapString(brokerKey(clusterId, brokerId));
    exclusionValue.setRecord(new Exclusion(incarnationId, false));
    placementExclusionsColumnFamilies.upsert(exclusionKey, exclusionValue);
  }

  private Exclusion getExclusion(final String clusterId, final String brokerId) {
    exclusionKey.wrapString(brokerKey(clusterId, brokerId));
    final var stored =
        placementExclusionsColumnFamilies.get(
            exclusionKey, BrokerPlacementExclusionRecordValue::new);
    return stored == null ? null : stored.getRecord();
  }

  @Override
  public void includeInPlacement(final String clusterId, final String brokerId) {
    exclusionKey.wrapString(brokerKey(clusterId, brokerId));
    placementExclusionsColumnFamilies.deleteIfExists(exclusionKey);
  }

  @Override
  public void registerBroker(
      final String clusterId,
      final String brokerId,
      final String host,
      final int port,
      final int raftPort,
      final String version,
      final long renewalTimeoutMillis,
      final Instant now,
      final List<BrokerPartitionState> partitions) {
    registerBroker(
        clusterId,
        brokerId,
        host,
        port,
        raftPort,
        version,
        renewalTimeoutMillis,
        now,
        partitions,
        "");
  }

  @Override
  public void registerBroker(
      final String clusterId,
      final String brokerId,
      final String host,
      final int port,
      final int raftPort,
      final String version,
      final long renewalTimeoutMillis,
      final Instant now,
      final List<BrokerPartitionState> partitions,
      final String incarnationId) {
    final var previous = registration(new BrokerRegistryKey(clusterId, brokerId));
    final Exclusion exclusion = getExclusion(clusterId, brokerId);
    if (exclusion != null && exclusion.legacy()) {
      // Upgrade in the REGISTER event transaction before replacing the old identity.
      putExclusion(clusterId, brokerId, previous.map(BrokerRegistration::incarnationId).orElse(""));
    }
    if (previous.isPresent()
        && previous.get().status() != BrokerStatus.OFFLINE
        && !incarnationId.isBlank()
        && incarnationId.equals(previous.get().incarnationId())) {
      // A lost REGISTER response must not erase observations or reset DRAINING state.
      putRegistration(
          previous
              .get()
              .renew(
                  now,
                  previous.get().status(),
                  partitions.isEmpty() ? previous.get().partitions() : partitions));
      return;
    }
    putRegistration(
        new BrokerRegistration(
            new BrokerRegistration.Identity(clusterId, brokerId, version, incarnationId),
            new BrokerRegistration.Endpoint(host, port, raftPort),
            new BrokerRegistration.Lease(now, now, renewalTimeoutMillis),
            isLearnerExcluded(clusterId, brokerId, incarnationId)
                ? BrokerStatus.DRAINING
                : BrokerStatus.ONLINE,
            partitions));
  }

  @Override
  public void renewBroker(
      final String clusterId,
      final String brokerId,
      final Instant now,
      final List<BrokerPartitionState> partitions) {
    renewBroker(clusterId, brokerId, now, BrokerStatus.ONLINE, partitions);
  }

  @Override
  public void markBrokerOffline(final String clusterId, final String brokerId, final Instant now) {
    renewBroker(clusterId, brokerId, now, BrokerStatus.OFFLINE, List.of());
  }

  @Override
  public void markBrokerDraining(final String clusterId, final String brokerId, final Instant now) {
    final BrokerRegistration existing = getRegistration(clusterId, brokerId);
    if (existing == null) {
      return;
    }
    putRegistration(existing.renew(now, BrokerStatus.DRAINING, existing.partitions()));
  }

  @Override
  public void markBrokerOnline(final String clusterId, final String brokerId) {
    final BrokerRegistration existing = getRegistration(clusterId, brokerId);
    if (existing == null) {
      return;
    }
    putRegistration(
        existing.renew(existing.lastRenewal(), BrokerStatus.ONLINE, existing.partitions()));
  }

  @Override
  public void removeBroker(final String clusterId, final String brokerId) {
    final BrokerRegistration existing = getRegistration(clusterId, brokerId);
    if (existing == null) {
      return;
    }
    brokerKey.wrapString(brokerKey(clusterId, brokerId));
    brokerRegistrationsColumnFamilies.deleteIfExists(brokerKey);
    deadlines.removeBroker(existing);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    registrations(clusterId)
        .forEach(registration -> removeBroker(clusterId, registration.brokerId()));
    final List<String> excluded = new ArrayList<>();
    final String prefix = clusterId + ":";
    placementExclusionsColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            excluded.add(key.toString());
          }
        });
    excluded.forEach(
        key -> {
          exclusionKey.wrapString(key);
          placementExclusionsColumnFamilies.deleteIfExists(exclusionKey);
        });
  }

  private void renewBroker(
      final String clusterId,
      final String brokerId,
      final Instant now,
      final BrokerStatus status,
      final List<BrokerPartitionState> partitions) {
    final BrokerRegistration current = getRegistration(clusterId, brokerId);
    if (current == null) {
      return;
    }
    final BrokerStatus effectiveStatus =
        status == BrokerStatus.ONLINE && current.status() == BrokerStatus.DRAINING
            ? BrokerStatus.DRAINING
            : status;
    final List<BrokerPartitionState> updatedPartitions =
        effectiveStatus == BrokerStatus.OFFLINE ? List.of() : partitions;
    putRegistration(current.renew(now, effectiveStatus, updatedPartitions));
  }

  private void putRegistration(final BrokerRegistration registration) {
    final BrokerRegistration previous =
        getRegistration(registration.clusterId(), registration.brokerId());
    brokerKey.wrapString(brokerKey(registration.clusterId(), registration.brokerId()));
    brokerValue.setRecord(registration);
    brokerRegistrationsColumnFamilies.upsert(brokerKey, brokerValue);
    deadlines.updateBroker(previous, registration);
  }

  private BrokerRegistration getRegistration(final String clusterId, final String brokerId) {
    brokerKey.wrapString(brokerKey(clusterId, brokerId));
    final var stored =
        brokerRegistrationsColumnFamilies.get(brokerKey, BrokerRegistrationRecordValue::new);
    return stored == null ? null : stored.getRecord();
  }

  // ["clusterId:brokerId"]: shared identity for registration and placement-exclusion records.
  private static String brokerKey(final String clusterId, final String brokerId) {
    return clusterId + ":" + brokerId;
  }
}
