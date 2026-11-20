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
package io.taskite.placementdriver.state.topology;

import io.taskite.db.ColumnFamily;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.state.immutable.TopologyMembershipChangeState;
import io.taskite.placementdriver.state.mutable.MutableBrokerTopologyChangeState;
import io.taskite.placementdriver.state.mutable.MutableMemberChangeState;
import io.taskite.placementdriver.state.mutable.MutableTopologyMembershipChangeState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

public final class DbMemberChangeState implements MutableMemberChangeState {
  private static final Comparator<MemberChangeRecord> CHANGE_ORDER =
      Comparator.comparingLong(MemberChangeRecord::getRequestedAtEpochMillis)
          .thenComparing(MemberChangeRecord::getChangeId);

  // [scope, resourceId, changeId] encoded as "scope:resourceId:changeId".
  private final DbString changeKey;
  private final MemberChangeRecordValue changeValue;
  // [scope, resourceId] encoded as "scope:resourceId".
  private final DbString activeKey;
  private final DbString activeValue;
  // [scope, resourceId] encoded as "scope:resourceId".
  private final DbString configurationKey;
  private final MemberChangeRecordValue configurationValue;
  private final int historyLimit;
  // PD_MEMBER_CHANGES: ["scope:resourceId:changeId"] -> MemberChangeRecord bytes.
  // Stores active and retained terminal plans for both BROKER and PLACEMENT_DRIVER scopes.
  private final ColumnFamily<DbString, MemberChangeRecordValue> changesColumnFamilies;
  // PD_ACTIVE_MEMBER_CHANGES: ["scope:resourceId"] -> active changeId.
  private final ColumnFamily<DbString, DbString> activeChangesColumnFamilies;
  // PD_MEMBER_CONFIGURATIONS: ["scope:resourceId"] -> last applied MemberChangeRecord bytes.
  private final ColumnFamily<DbString, MemberChangeRecordValue> configurationsColumnFamilies;
  private final MutableTopologyMembershipChangeState executions;
  private final MutableBrokerTopologyChangeState topologyChanges;

  public DbMemberChangeState(
      final ProcessingStateStore storage,
      final int historyLimit,
      final MutableTopologyMembershipChangeState executions,
      final MutableBrokerTopologyChangeState topologyChanges) {
    changeKey = new DbString();
    changeValue = new MemberChangeRecordValue();
    activeKey = new DbString();
    activeValue = new DbString();
    configurationKey = new DbString();
    configurationValue = new MemberChangeRecordValue();

    if (historyLimit <= 0) {
      throw new IllegalArgumentException("change history limit must be greater than zero");
    }
    this.historyLimit = historyLimit;
    this.executions = executions;
    this.topologyChanges = topologyChanges;
    changesColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_MEMBER_CHANGES, changeKey, changeValue);
    activeChangesColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_ACTIVE_MEMBER_CHANGES, activeKey, activeValue);
    configurationsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_MEMBER_CONFIGURATIONS, configurationKey, configurationValue);
  }

  @Override
  public Optional<MemberChangeRecord> get(
      final MemberChangeScope scope, final String resourceId, final String changeId) {
    changeKey.wrapString(changeKey(scope, resourceId, changeId));
    return decode(changesColumnFamilies.get(changeKey, MemberChangeRecordValue::new));
  }

  @Override
  public Optional<MemberChangeRecord> active(
      final MemberChangeScope scope, final String resourceId) {
    activeKey.wrapString(resourceKey(scope, resourceId));
    final DbString stored = activeChangesColumnFamilies.get(activeKey);
    return stored == null ? Optional.empty() : get(scope, resourceId, stored.toString());
  }

  @Override
  public List<MemberChangeRecord> recentChanges(
      final MemberChangeScope scope, final String resourceId, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final String prefix = resourceKey(scope, resourceId) + ":";
    final PriorityQueue<MemberChangeRecord> recent = new PriorityQueue<>(CHANGE_ORDER);
    changesColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            decode(value)
                .ifPresent(
                    change -> {
                      recent.add(change);
                      if (recent.size() > limit) {
                        recent.poll();
                      }
                    });
          }
        });
    final List<MemberChangeRecord> result = new ArrayList<>(recent);
    result.sort(CHANGE_ORDER.reversed());
    return result;
  }

  @Override
  public Optional<MemberChangeRecord> configuration(
      final MemberChangeScope scope, final String resourceId) {
    configurationKey.wrapString(resourceKey(scope, resourceId));
    return decode(configurationsColumnFamilies.get(configurationKey, MemberChangeRecordValue::new));
  }

  @Override
  public void put(final MemberChangeRecord change) {
    changeKey.wrapString(
        changeKey(change.getScope(), change.getResourceId(), change.getChangeId()));
    changeValue.setRecord(change);
    changesColumnFamilies.upsert(changeKey, changeValue);
    executions.markMember(
        change,
        active(change.getScope(), change.getResourceId())
            .map(active -> active.getChangeId().equals(change.getChangeId()))
            .orElse(false));
    pruneHistory(change.getScope(), change.getResourceId(), change.getChangeId());
  }

  @Override
  public void setActive(final MemberChangeRecord change) {
    activeKey.wrapString(resourceKey(change.getScope(), change.getResourceId()));
    activeValue.wrapString(change.getChangeId());
    activeChangesColumnFamilies.upsert(activeKey, activeValue);
    executions.markMember(change, true);
  }

  @Override
  public void clearActive(final MemberChangeScope scope, final String resourceId) {
    activeKey.wrapString(resourceKey(scope, resourceId));
    activeChangesColumnFamilies.deleteIfExists(activeKey);
    executions.removeActive(TopologyMembershipChangeState.scope(scope), resourceId, 1);
  }

  @Override
  public void putConfiguration(final MemberChangeRecord change) {
    configurationKey.wrapString(resourceKey(change.getScope(), change.getResourceId()));
    configurationValue.setRecord(change);
    configurationsColumnFamilies.upsert(configurationKey, configurationValue);
  }

  @Override
  public void deleteResource(final MemberChangeScope scope, final String resourceId) {
    clearActive(scope, resourceId);
    executions.deleteResource(TopologyMembershipChangeState.scope(scope), resourceId);
    if (scope == MemberChangeScope.BROKER) {
      topologyChanges.deleteCluster(resourceId);
    }
    configurationKey.wrapString(resourceKey(scope, resourceId));
    configurationsColumnFamilies.deleteIfExists(configurationKey);
    final String prefix = resourceKey(scope, resourceId) + ":";
    final List<String> keys = new ArrayList<>();
    changesColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            keys.add(key.toString());
          }
        });
    for (final String key : keys) {
      changeKey.wrapString(key);
      changesColumnFamilies.deleteIfExists(changeKey);
    }
  }

  private void pruneHistory(
      final MemberChangeScope scope, final String resourceId, final String protectedChangeId) {
    final List<MemberChangeRecord> history = recentChanges(scope, resourceId, Integer.MAX_VALUE);
    int entriesToDelete =
        (int) history.stream().filter(MemberChangeRecord::isTerminal).count() - historyLimit;
    for (int index = history.size() - 1; index >= 0 && entriesToDelete > 0; index--) {
      final MemberChangeRecord candidate = history.get(index);
      if (candidate.getChangeId().equals(protectedChangeId) || !candidate.isTerminal()) {
        continue;
      }
      if (executions
          .tasks(TopologyMembershipChangeState.scope(scope), resourceId, candidate.getChangeId())
          .stream()
          .anyMatch(task -> !task.status().terminal())) {
        continue;
      }
      if (scope == MemberChangeScope.BROKER
          && !topologyChanges.deleteTerminalMemberPlans(
              resourceId, candidate.getChangeId(), candidate.getChildPartitionChangeIds())) {
        continue;
      }
      if (!executions.deletePlan(
          TopologyMembershipChangeState.scope(scope), resourceId, candidate.getChangeId())) {
        continue;
      }
      changeKey.wrapString(changeKey(scope, resourceId, candidate.getChangeId()));
      changesColumnFamilies.deleteIfExists(changeKey);
      entriesToDelete--;
    }
  }

  private static Optional<MemberChangeRecord> decode(final MemberChangeRecordValue stored) {
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  // ["scope:resourceId"]: scope uses its enum name to separate Broker and PD configurations.
  private static String resourceKey(final MemberChangeScope scope, final String resourceId) {
    return scope.name() + ":" + resourceId;
  }

  // ["scope:resourceId:changeId"]: one member-change plan within a scoped resource.
  private static String changeKey(
      final MemberChangeScope scope, final String resourceId, final String changeId) {
    return resourceKey(scope, resourceId) + ":" + changeId;
  }
}
