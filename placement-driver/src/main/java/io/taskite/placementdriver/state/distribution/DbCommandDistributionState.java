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
package io.taskite.placementdriver.state.distribution;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.immutable.BrokerDistributionScheduleState.Entry;
import io.taskite.placementdriver.state.mutable.MutableBrokerDistributionScheduleState;
import io.taskite.placementdriver.state.mutable.MutableCommandDistributionState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionRecord;
import io.taskite.protocol.impl.record.value.placementdriver.commanddistribution.CommandDistributionTargetRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DbCommandDistributionState implements MutableCommandDistributionState {
  private final DbBrokerDistributionScheduleState scheduling;
  // [distributionKey]
  private final DbLong distributionKey;
  private final CommandDistributionRecordValue distributionValue;
  // [dedupeKey]: caller-supplied idempotency key.
  private final DbString dedupeKey;
  // Distribution key stored as the value of the deduplication index.
  private final DbLong dedupeDistributionKey;
  // [distributionKey, clusterId, partitionId] encoded as "distributionKey:clusterId:partitionId".
  private final DbString pendingKey;
  // [distributionKey, clusterId] encoded as "distributionKey:clusterId".
  private final DbString targetKey;
  // [distributionKey, clusterId, partitionId]: typed pending-target key and recovery cursor.
  private final DbCompositeKey<DbLong, DbCompositeKey<DbString, DbInt>> pendingTargetKey;
  private final CommandDistributionTargetRecordValue targetValue;
  // PD_COMMAND_DISTRIBUTIONS: [distributionKey] -> CommandDistributionRecord bytes.
  private final ColumnFamily<DbLong, CommandDistributionRecordValue> distributionsColumnFamilies;
  // PD_COMMAND_DISTRIBUTION_DEDUPE: [dedupeKey] -> distributionKey.
  private final ColumnFamily<DbString, DbLong> dedupeColumnFamilies;
  // PD_PENDING_DISTRIBUTIONS: ["distributionKey:clusterId:partitionId"] -> nil.
  // Presence means this partition has not acknowledged the current distribution.
  private final ColumnFamily<DbString, DbNil> pendingDistributionsColumnFamilies;
  // PD_COMMAND_DISTRIBUTION_TARGETS: ["distributionKey:clusterId"]
  // -> CommandDistributionTargetRecord bytes containing expected and acknowledged partition counts.
  private final ColumnFamily<DbString, CommandDistributionTargetRecordValue> targetsColumnFamilies;
  // PD_PENDING_DISTRIBUTION_TARGETS: [distributionKey, clusterId, partitionId] -> nil.
  // Typed, ordered counterpart of PD_PENDING_DISTRIBUTIONS for bounded recovery scans.
  private final ColumnFamily<DbCompositeKey<DbLong, DbCompositeKey<DbString, DbInt>>, DbNil>
      pendingTargetsColumnFamilies;

  public DbCommandDistributionState(final ProcessingStateStore storage) {
    distributionKey = new DbLong();
    distributionValue = new CommandDistributionRecordValue();
    dedupeKey = new DbString();
    dedupeDistributionKey = new DbLong();
    pendingKey = new DbString();
    targetKey = new DbString();
    pendingTargetKey =
        new DbCompositeKey<>(new DbLong(), new DbCompositeKey<>(new DbString(), new DbInt()));
    targetValue = new CommandDistributionTargetRecordValue();

    scheduling = new DbBrokerDistributionScheduleState(storage);
    distributionsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_COMMAND_DISTRIBUTIONS, distributionKey, distributionValue);
    dedupeColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_COMMAND_DISTRIBUTION_DEDUPE, dedupeKey, dedupeDistributionKey);
    pendingDistributionsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PENDING_DISTRIBUTIONS, pendingKey, DbNil.INSTANCE);
    targetsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_COMMAND_DISTRIBUTION_TARGETS, targetKey, targetValue);
    pendingTargetsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PENDING_DISTRIBUTION_TARGETS, pendingTargetKey, DbNil.INSTANCE);
  }

  @Override
  public MutableBrokerDistributionScheduleState scheduling() {
    return scheduling;
  }

  @Override
  public List<PendingTarget> pendingTargetsAfter(final PendingTarget cursor, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final List<PendingTarget> result = new ArrayList<>();
    pendingTargetsColumnFamilies.whileTrue(
        wrapPendingTargetKey(cursor),
        (key, value) -> {
          final var target =
              new PendingTarget(
                  key.first().getValue(),
                  key.second().first().toString(),
                  key.second().second().getValue());
          if (!target.equals(cursor)) {
            result.add(target);
          }
          return result.size() < limit;
        });
    return result;
  }

  /** Called in the recovery transaction; legacy primary pending keys remain snapshot-compatible. */
  public void rebuildPendingIndex() {
    pendingTargetsColumnFamilies.whileTrue(
        (key, value) -> {
          pendingTargetsColumnFamilies.deleteIfExists(key);
          return true;
        });
    pendingDistributionsColumnFamilies.forEach(
        (key, value) -> {
          final String stored = key.toString();
          final int first = stored.indexOf(':');
          final int last = stored.lastIndexOf(':');
          final var target =
              new PendingTarget(
                  Long.parseLong(stored.substring(0, first)),
                  stored.substring(first + 1, last),
                  Integer.parseInt(stored.substring(last + 1)));
          pendingTargetsColumnFamilies.upsert(wrapPendingTargetKey(target), DbNil.INSTANCE);
          ensureSchedule(target);
        });
  }

  // [distributionKey, clusterId, partitionId]: typed pending-target index key.
  private DbCompositeKey<DbLong, DbCompositeKey<DbString, DbInt>> wrapPendingTargetKey(
      final PendingTarget target) {
    pendingTargetKey.first().wrapLong(target.distributionKey());
    pendingTargetKey.second().first().wrapString(target.clusterId());
    pendingTargetKey.second().second().wrapInt(target.partitionId());
    return pendingTargetKey;
  }

  @Override
  public Optional<CommandDistributionRecord<?>> distribution(final long distributionKey) {
    this.distributionKey.wrapLong(distributionKey);
    final var stored =
        distributionsColumnFamilies.get(this.distributionKey, CommandDistributionRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public Optional<Long> distributionKeyByDedupeKey(final String dedupeKey) {
    this.dedupeKey.wrapString(dedupeKey);
    final DbLong stored = dedupeColumnFamilies.get(this.dedupeKey);
    return stored == null ? Optional.empty() : Optional.of(stored.getValue());
  }

  @Override
  public List<CommandDistributionRecord<?>> distributions() {
    final List<CommandDistributionRecord<?>> records = new ArrayList<>();
    distributionsColumnFamilies.forEach((key, value) -> records.add(value.getRecord()));
    records.sort(Comparator.comparingLong(CommandDistributionRecord::getDistributionKey));
    return records;
  }

  @Override
  public boolean isPending(
      final long distributionKey, final String clusterId, final int partitionId) {
    pendingKey.wrapString(pendingKey(distributionKey, clusterId, partitionId));
    return pendingDistributionsColumnFamilies.exists(pendingKey);
  }

  @Override
  public List<Integer> pendingPartitions(final long distributionKey, final String clusterId) {
    final List<Integer> partitions = new ArrayList<>();
    final String prefix = pendingKeyPrefix(distributionKey, clusterId);
    pendingDistributionsColumnFamilies.forEach(
        (key, value) -> {
          final String storedKey = key.toString();
          if (storedKey.startsWith(prefix)) {
            partitionId(storedKey.substring(prefix.length())).ifPresent(partitions::add);
          }
        });
    partitions.sort(Integer::compareTo);
    return partitions;
  }

  @Override
  public List<CommandDistributionTargetRecord> targets(final long distributionKey) {
    final List<CommandDistributionTargetRecord> targets = new ArrayList<>();
    final String prefix = distributionKey + ":";
    targetsColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            targets.add(value.getRecord());
          }
        });
    final String originClusterId =
        distribution(distributionKey).map(CommandDistributionRecord::getClusterId).orElse("");
    targets.sort(
        Comparator.comparingInt(
                (CommandDistributionTargetRecord target) ->
                    target.getClusterId().equals(originClusterId) ? 0 : 1)
            .thenComparing(CommandDistributionTargetRecord::getClusterId));
    return targets;
  }

  @Override
  public boolean isTarget(final long distributionKey, final String clusterId) {
    targetKey.wrapString(targetKey(distributionKey, clusterId));
    return targetsColumnFamilies.exists(targetKey);
  }

  @Override
  public boolean isTargetComplete(final long distributionKey, final String clusterId) {
    return target(distributionKey, clusterId)
        .map(CommandDistributionTargetRecord::isCompleted)
        .orElse(false);
  }

  @Override
  public boolean isPlanComplete(final long distributionKey) {
    final List<CommandDistributionTargetRecord> targets = targets(distributionKey);
    return !targets.isEmpty()
        && targets.stream().allMatch(CommandDistributionTargetRecord::isCompleted);
  }

  @Override
  public void putDistribution(final CommandDistributionRecord<?> record) {
    distributionKey.wrapLong(record.getDistributionKey());
    distributionValue.setRecord(record);
    distributionsColumnFamilies.upsert(distributionKey, distributionValue);
  }

  @Override
  public void putDedupeKey(final String dedupeKey, final long distributionKey) {
    this.dedupeKey.wrapString(dedupeKey);
    dedupeDistributionKey.wrapLong(distributionKey);
    dedupeColumnFamilies.upsert(this.dedupeKey, dedupeDistributionKey);
  }

  @Override
  public void putTarget(
      final long distributionKey, final String clusterId, final int partitionCount) {
    if (distributionKey <= 0 || clusterId == null || clusterId.isBlank() || partitionCount <= 0) {
      return;
    }
    final Optional<CommandDistributionTargetRecord> existing = target(distributionKey, clusterId);
    final CommandDistributionTargetRecord target =
        existing.orElseGet(
            () ->
                new CommandDistributionTargetRecord()
                    .setDistributionKey(distributionKey)
                    .setClusterId(clusterId)
                    .setExpectedPartitionCount(partitionCount)
                    .setAcknowledgedPartitionCount(partitionCount));
    target.setExpectedPartitionCount(Math.max(target.getExpectedPartitionCount(), partitionCount));
    putTarget(target);
  }

  @Override
  public void resetTarget(
      final long distributionKey, final String clusterId, final int partitionCount) {
    for (final int partition : pendingPartitions(distributionKey, clusterId)) {
      pendingKey.wrapString(pendingKey(distributionKey, clusterId, partition));
      pendingDistributionsColumnFamilies.deleteIfExists(pendingKey);
      pendingTargetsColumnFamilies.deleteIfExists(
          wrapPendingTargetKey(new PendingTarget(distributionKey, clusterId, partition)));
      scheduling.remove(new PendingTarget(distributionKey, clusterId, partition));
    }
    putTarget(
        new CommandDistributionTargetRecord()
            .setDistributionKey(distributionKey)
            .setClusterId(clusterId)
            .setExpectedPartitionCount(partitionCount)
            .setAcknowledgedPartitionCount(0));
    for (int partition = 1; partition <= partitionCount; partition++) {
      pendingKey.wrapString(pendingKey(distributionKey, clusterId, partition));
      pendingDistributionsColumnFamilies.upsert(pendingKey, DbNil.INSTANCE);
      pendingTargetsColumnFamilies.upsert(
          wrapPendingTargetKey(new PendingTarget(distributionKey, clusterId, partition)),
          DbNil.INSTANCE);
      ensureSchedule(new PendingTarget(distributionKey, clusterId, partition));
    }
  }

  @Override
  public void markPending(
      final long distributionKey, final String clusterId, final int partitionId) {
    if (distributionKey <= 0 || clusterId == null || clusterId.isBlank() || partitionId <= 0) {
      return;
    }
    if (target(distributionKey, clusterId).isEmpty()) {
      putTarget(distributionKey, clusterId, partitionId);
    }
    pendingKey.wrapString(pendingKey(distributionKey, clusterId, partitionId));
    pendingDistributionsColumnFamilies.upsert(pendingKey, DbNil.INSTANCE);
    pendingTargetsColumnFamilies.upsert(
        wrapPendingTargetKey(new PendingTarget(distributionKey, clusterId, partitionId)),
        DbNil.INSTANCE);
    ensureSchedule(new PendingTarget(distributionKey, clusterId, partitionId));
    synchronizeAcknowledgedPartitionCount(distributionKey, clusterId);
  }

  @Override
  public boolean acknowledge(
      final long distributionKey, final String clusterId, final int partitionId) {
    pendingKey.wrapString(pendingKey(distributionKey, clusterId, partitionId));
    if (!pendingDistributionsColumnFamilies.exists(pendingKey)) {
      return false;
    }
    pendingDistributionsColumnFamilies.deleteIfExists(pendingKey);
    pendingTargetsColumnFamilies.deleteIfExists(
        wrapPendingTargetKey(new PendingTarget(distributionKey, clusterId, partitionId)));
    scheduling.remove(new PendingTarget(distributionKey, clusterId, partitionId));
    synchronizeAcknowledgedPartitionCount(distributionKey, clusterId);
    return true;
  }

  @Override
  public void deleteCluster(final String clusterId) {
    final List<Long> distributionKeys =
        distributions().stream()
            .filter(record -> record.getClusterId().equals(clusterId))
            .map(CommandDistributionRecord::getDistributionKey)
            .toList();
    if (distributionKeys.isEmpty()) {
      return;
    }
    final List<String> dedupeKeys = new ArrayList<>();
    dedupeColumnFamilies.forEach(
        (key, value) -> {
          if (distributionKeys.contains(value.getValue())) {
            dedupeKeys.add(key.toString());
          }
        });
    final List<String> pendingKeys = new ArrayList<>();
    pendingDistributionsColumnFamilies.forEach(
        (key, value) -> {
          if (distributionKeys.stream()
              .anyMatch(distribution -> key.toString().startsWith(distribution + ":"))) {
            pendingKeys.add(key.toString());
          }
        });
    final List<String> targetKeys = new ArrayList<>();
    targetsColumnFamilies.forEach(
        (key, value) -> {
          if (distributionKeys.stream()
              .anyMatch(distribution -> key.toString().startsWith(distribution + ":"))) {
            targetKeys.add(key.toString());
          }
        });
    distributionKeys.forEach(
        key -> {
          distributionKey.wrapLong(key);
          distributionsColumnFamilies.deleteIfExists(distributionKey);
        });
    dedupeKeys.forEach(
        key -> {
          dedupeKey.wrapString(key);
          dedupeColumnFamilies.deleteIfExists(dedupeKey);
        });
    pendingKeys.forEach(
        key -> {
          final int first = key.indexOf(':');
          final int last = key.lastIndexOf(':');
          scheduling.remove(
              new PendingTarget(
                  Long.parseLong(key.substring(0, first)),
                  key.substring(first + 1, last),
                  Integer.parseInt(key.substring(last + 1))));
          pendingTargetsColumnFamilies.deleteIfExists(
              wrapPendingTargetKey(
                  new PendingTarget(
                      Long.parseLong(key.substring(0, first)),
                      key.substring(first + 1, last),
                      Integer.parseInt(key.substring(last + 1)))));
          pendingKey.wrapString(key);
          pendingDistributionsColumnFamilies.deleteIfExists(pendingKey);
        });
    targetKeys.forEach(
        key -> {
          targetKey.wrapString(key);
          targetsColumnFamilies.deleteIfExists(targetKey);
        });
  }

  private void ensureSchedule(final PendingTarget target) {
    final var current = distribution(target.distributionKey()).orElse(null);
    if (current != null
        && scheduling
            .get(target)
            .filter(entry -> entry.generation() == current.getGeneration())
            .isEmpty()) {
      scheduling.put(new Entry(target, current.getGeneration(), 0, 0));
    }
  }

  private Optional<CommandDistributionTargetRecord> target(
      final long distributionKey, final String clusterId) {
    targetKey.wrapString(targetKey(distributionKey, clusterId));
    final var stored =
        targetsColumnFamilies.get(targetKey, CommandDistributionTargetRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  private void putTarget(final CommandDistributionTargetRecord target) {
    targetKey.wrapString(targetKey(target.getDistributionKey(), target.getClusterId()));
    targetValue.setRecord(target);
    targetsColumnFamilies.upsert(targetKey, targetValue);
  }

  private void synchronizeAcknowledgedPartitionCount(
      final long distributionKey, final String clusterId) {
    target(distributionKey, clusterId)
        .ifPresent(
            target -> {
              target.setAcknowledgedPartitionCount(
                  Math.max(
                      0,
                      target.getExpectedPartitionCount()
                          - pendingPartitions(distributionKey, clusterId).size()));
              putTarget(target);
            });
  }

  // ["distributionKey:clusterId"]: identifies one cluster's distribution target.
  private static String targetKey(final long distributionKey, final String clusterId) {
    return distributionKey + ":" + clusterId;
  }

  // ["distributionKey:clusterId:partitionId"]: primary pending-target key.
  private static String pendingKey(
      final long distributionKey, final String clusterId, final int partitionId) {
    return pendingKeyPrefix(distributionKey, clusterId) + partitionId;
  }

  // ["distributionKey:clusterId:"]: prefix for pending partitions of one target cluster.
  private static String pendingKeyPrefix(final long distributionKey, final String clusterId) {
    return distributionKey + ":" + clusterId + ":";
  }

  private static Optional<Integer> partitionId(final String value) {
    try {
      final int partitionId = Integer.parseInt(value);
      return partitionId > 0 ? Optional.of(partitionId) : Optional.empty();
    } catch (final NumberFormatException ignored) {
      return Optional.empty();
    }
  }
}
