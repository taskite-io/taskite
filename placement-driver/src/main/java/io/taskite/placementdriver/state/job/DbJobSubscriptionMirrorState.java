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
package io.taskite.placementdriver.state.job;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbLong;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.mutable.MutableJobSubscriptionMirrorState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.mirror.JobSubscriptionMirrorRecord;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class DbJobSubscriptionMirrorState implements MutableJobSubscriptionMirrorState {
  // [mirrorKey]
  private final DbLong mirrorKey;
  private final JobSubscriptionMirrorRecordValue mirrorValue;
  // [sourceClusterId, targetClusterId] encoded as "sourceClusterId:targetClusterId".
  private final DbString pairKey;
  // Mirror key stored as the value of the directed cluster-pair index.
  private final DbLong pairMirrorKey;
  // PD_JOB_SUBSCRIPTION_MIRRORS: [mirrorKey] -> JobSubscriptionMirrorRecord bytes.
  private final ColumnFamily<DbLong, JobSubscriptionMirrorRecordValue> mirrorsColumnFamilies;
  // PD_JOB_SUBSCRIPTION_MIRROR_PAIRS: ["sourceClusterId:targetClusterId"] -> mirrorKey.
  // Source and target order matters: each entry identifies a directed mirror.
  private final ColumnFamily<DbString, DbLong> pairsColumnFamilies;

  public DbJobSubscriptionMirrorState(final ProcessingStateStore storage) {
    mirrorKey = new DbLong();
    mirrorValue = new JobSubscriptionMirrorRecordValue();
    pairKey = new DbString();
    pairMirrorKey = new DbLong();

    mirrorsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_SUBSCRIPTION_MIRRORS, mirrorKey, mirrorValue);
    pairsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_JOB_SUBSCRIPTION_MIRROR_PAIRS, pairKey, pairMirrorKey);
  }

  @Override
  public Optional<JobSubscriptionMirrorRecord> mirror(final long mirrorKey) {
    this.mirrorKey.wrapLong(mirrorKey);
    final var stored =
        mirrorsColumnFamilies.get(this.mirrorKey, JobSubscriptionMirrorRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public Optional<JobSubscriptionMirrorRecord> mirror(
      final String sourceClusterId, final String targetClusterId) {
    pairKey.wrapString(pairKey(sourceClusterId, targetClusterId));
    final DbLong stored = pairsColumnFamilies.get(pairKey);
    return stored == null ? Optional.empty() : mirror(stored.getValue());
  }

  @Override
  public List<JobSubscriptionMirrorRecord> mirrors() {
    final List<JobSubscriptionMirrorRecord> result = new ArrayList<>();
    mirrorsColumnFamilies.forEach((key, value) -> result.add(value.getRecord()));
    result.sort(Comparator.comparingLong(JobSubscriptionMirrorRecord::getMirrorKey));
    return result;
  }

  @Override
  public void put(final JobSubscriptionMirrorRecord mirror) {
    mirrorKey.wrapLong(mirror.getMirrorKey());
    mirrorValue.setRecord(mirror);
    mirrorsColumnFamilies.upsert(mirrorKey, mirrorValue);
    pairKey.wrapString(pairKey(mirror.getSourceClusterId(), mirror.getTargetClusterId()));
    pairMirrorKey.wrapLong(mirror.getMirrorKey());
    pairsColumnFamilies.upsert(pairKey, pairMirrorKey);
  }

  @Override
  public void delete(final long mirrorKey) {
    mirror(mirrorKey)
        .ifPresent(
            mirror -> {
              pairKey.wrapString(pairKey(mirror.getSourceClusterId(), mirror.getTargetClusterId()));
              pairsColumnFamilies.deleteIfExists(pairKey);
            });
    this.mirrorKey.wrapLong(mirrorKey);
    mirrorsColumnFamilies.deleteIfExists(this.mirrorKey);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    final List<Long> mirrorKeys =
        mirrors().stream()
            .filter(
                mirror ->
                    mirror.getSourceClusterId().equals(clusterId)
                        || mirror.getTargetClusterId().equals(clusterId))
            .map(JobSubscriptionMirrorRecord::getMirrorKey)
            .toList();
    mirrorKeys.forEach(this::delete);
  }

  // ["sourceClusterId:targetClusterId"]: lookup key for the directed mirror.
  private static String pairKey(final String sourceClusterId, final String targetClusterId) {
    return sourceClusterId + ":" + targetClusterId;
  }
}
