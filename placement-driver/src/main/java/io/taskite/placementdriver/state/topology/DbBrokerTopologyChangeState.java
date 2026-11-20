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
import io.taskite.db.impl.DbCompositeKey;
import io.taskite.db.impl.DbInt;
import io.taskite.db.impl.DbNil;
import io.taskite.db.impl.DbString;
import io.taskite.placementdriver.broker.cluster.BrokerClusterChangeIds;
import io.taskite.placementdriver.broker.cluster.ChangePlan;
import io.taskite.placementdriver.broker.cluster.ClusterChangeOperation;
import io.taskite.placementdriver.state.mutable.MutableBrokerTopologyChangeState;
import io.taskite.placementdriver.state.mutable.MutableTopologyMembershipChangeState;
import io.taskite.protocol.ColumnFamilies;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord;
import io.taskite.protocol.impl.record.value.placementdriver.member.MemberChangeRecord.MemberChangeScope;
import io.taskite.protocol.impl.record.value.placementdriver.topology.TopologyChangeTaskRecord.Scope;
import io.taskite.state.processing.ProcessingStateStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

public final class DbBrokerTopologyChangeState implements MutableBrokerTopologyChangeState {
  private static final Comparator<ClusterChangeOperation> CLUSTER_CHANGE_ORDER =
      Comparator.comparingLong(ClusterChangeOperation::startedAtEpochMillis)
          .thenComparing(ClusterChangeOperation::changeId);

  // [clusterId, changeId] encoded as "clusterId:changeId".
  private final DbString planKey;
  private final ChangePlanRecordValue planValue;
  // [clusterId, partitionId] encoded as "clusterId:partitionId".
  private final DbString pendingKey;
  private final DbString pendingChangeId;
  // [clusterId, operationId] encoded as "clusterId:operationId".
  private final DbString operationKey;
  // [clusterId, changeId] encoded as "clusterId:changeId".
  private final DbString clusterChangeKey;
  private final ClusterChangeOperationRecordValue clusterChangeValue;
  // [clusterId]
  private final DbString activeChangeKey;
  // [clusterId, partitionId]: typed pending-plan index key and recovery cursor.
  private final DbCompositeKey<DbString, DbInt> indexedKey;
  // ["BROKER:clusterId:changeId"]: parent plan in the shared PD_MEMBER_CHANGES column.
  private final DbString memberChangeKey;
  private final DbString activeChangeId;
  private final int historyLimit;
  private final MutableTopologyMembershipChangeState execution;
  // PD_CHANGE_PLANS: ["clusterId:changeId"] -> ChangePlanRecordCodec bytes.
  private final ColumnFamily<DbString, ChangePlanRecordValue> plansColumnFamilies;
  // PD_PENDING_CHANGES: ["clusterId:partitionId"] -> changeId of the current pending owner.
  private final ColumnFamily<DbString, DbString> pendingColumnFamilies;
  // PD_TOPOLOGY_OP_DEDUPE: ["clusterId:operationId"] -> nil; records applied operations.
  private final ColumnFamily<DbString, DbNil> appliedOperationsColumnFamilies;
  // PD_CLUSTER_CHANGE_OPERATIONS: ["clusterId:changeId"] -> ClusterChangeOperationCodec bytes.
  private final ColumnFamily<DbString, ClusterChangeOperationRecordValue>
      clusterChangesColumnFamilies;
  // PD_ACTIVE_CLUSTER_CHANGES: [clusterId] -> active cluster changeId.
  private final ColumnFamily<DbString, DbString> activeChangesColumnFamilies;
  // PD_PENDING_PARTITION_CHANGES: [clusterId, partitionId] -> pending changeId.
  // Typed, ordered counterpart of PD_PENDING_CHANGES for bounded recovery scans.
  private final ColumnFamily<DbCompositeKey<DbString, DbInt>, DbString> pendingIndexColumnFamilies;
  // PD_MEMBER_CHANGES: ["scope:resourceId:changeId"] -> MemberChangeRecord bytes.
  // Shared with DbMemberChangeState; this view reads/removes BROKER-scoped parent plans.
  private final ColumnFamily<DbString, MemberChangeRecordValue> memberChangesColumnFamilies;

  public DbBrokerTopologyChangeState(
      final ProcessingStateStore storage,
      final int historyLimit,
      final MutableTopologyMembershipChangeState execution) {
    planKey = new DbString();
    planValue = new ChangePlanRecordValue();
    pendingKey = new DbString();
    pendingChangeId = new DbString();
    operationKey = new DbString();
    clusterChangeKey = new DbString();
    clusterChangeValue = new ClusterChangeOperationRecordValue();
    activeChangeKey = new DbString();
    indexedKey = new DbCompositeKey<>(new DbString(), new DbInt());
    memberChangeKey = new DbString();
    activeChangeId = new DbString();

    if (historyLimit <= 0) {
      throw new IllegalArgumentException("change history limit must be greater than zero");
    }
    this.historyLimit = historyLimit;
    this.execution = execution;
    plansColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_CHANGE_PLANS, planKey, planValue);
    pendingColumnFamilies =
        storage.createColumnFamily(ColumnFamilies.PD_PENDING_CHANGES, pendingKey, pendingChangeId);
    appliedOperationsColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_TOPOLOGY_OP_DEDUPE, operationKey, DbNil.INSTANCE);
    clusterChangesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_CLUSTER_CHANGE_OPERATIONS, clusterChangeKey, clusterChangeValue);
    activeChangesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_ACTIVE_CLUSTER_CHANGES, activeChangeKey, activeChangeId);
    pendingIndexColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_PENDING_PARTITION_CHANGES, indexedKey, new DbString());
    memberChangesColumnFamilies =
        storage.createColumnFamily(
            ColumnFamilies.PD_MEMBER_CHANGES, memberChangeKey, new MemberChangeRecordValue());
  }

  @Override
  public List<ChangePlan> pendingChangePlansAfter(
      final String clusterId, final int partitionId, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final var start = wrapIndexedKey(clusterId, partitionId);
    final List<ChangePlan> result = new ArrayList<>();
    pendingIndexColumnFamilies.whileTrue(
        start,
        (key, value) -> {
          if (!key.first().toString().equals(clusterId) || key.second().getValue() != partitionId) {
            changePlan(key.first().toString(), value.toString()).ifPresent(result::add);
          }
          return result.size() < limit;
        });
    return result;
  }

  public void rebuildPendingIndex() {
    pendingIndexColumnFamilies.whileTrue(
        (key, value) -> {
          pendingIndexColumnFamilies.deleteIfExists(key);
          return true;
        });
    pendingColumnFamilies.forEach(
        (key, value) -> {
          final String stored = key.toString();
          final int separator = stored.lastIndexOf(':');
          pendingIndexColumnFamilies.upsert(
              wrapIndexedKey(
                  stored.substring(0, separator),
                  Integer.parseInt(stored.substring(separator + 1))),
              value);
        });
  }

  // [clusterId, partitionId]: typed pending-plan index key.
  private DbCompositeKey<DbString, DbInt> wrapIndexedKey(
      final String clusterId, final int partitionId) {
    indexedKey.first().wrapString(clusterId);
    indexedKey.second().wrapInt(partitionId);
    return indexedKey;
  }

  @Override
  public Optional<ChangePlan> changePlan(final String clusterId, final String changeId) {
    planKey.wrapString(planKey(clusterId, changeId));
    final var stored = plansColumnFamilies.get(planKey, ChangePlanRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public Optional<ClusterChangeOperation> clusterChangeOperation(
      final String clusterId, final String changeId) {
    clusterChangeKey.wrapString(planKey(clusterId, changeId));
    final var stored =
        clusterChangesColumnFamilies.get(clusterChangeKey, ClusterChangeOperationRecordValue::new);
    return stored == null ? Optional.empty() : Optional.of(stored.getRecord());
  }

  @Override
  public Optional<ClusterChangeOperation> activeClusterChange(final String clusterId) {
    activeChangeKey.wrapString(clusterId);
    final DbString stored = activeChangesColumnFamilies.get(activeChangeKey);
    return stored == null
        ? Optional.empty()
        : clusterChangeOperation(clusterId, stored.toString())
            .filter(operation -> !operation.status().isTerminal());
  }

  @Override
  public List<ClusterChangeOperation> recentClusterChanges(
      final String clusterId, final int limit) {
    if (limit <= 0) {
      return List.of();
    }
    final String prefix = clusterId + ":";
    final PriorityQueue<ClusterChangeOperation> recent = new PriorityQueue<>(CLUSTER_CHANGE_ORDER);
    clusterChangesColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            recent.add(value.getRecord());
            if (recent.size() > limit) {
              recent.poll();
            }
          }
        });
    final List<ClusterChangeOperation> result = new ArrayList<>(recent);
    result.sort(CLUSTER_CHANGE_ORDER.reversed());
    return result;
  }

  @Override
  public List<ChangePlan> pendingChangePlans(final String clusterId) {
    final String prefix = clusterId + ":";
    final List<ChangePlan> result = new ArrayList<>();
    pendingColumnFamilies.forEach(
        (key, value) -> {
          if (!key.toString().startsWith(prefix)) {
            return;
          }
          changePlan(clusterId, value.toString()).ifPresent(result::add);
        });
    result.sort(
        Comparator.comparingInt(ChangePlan::partitionId).thenComparing(ChangePlan::changeId));
    return result;
  }

  @Override
  public List<ChangePlan> changePlans(final String clusterId, final String operationId) {
    final String prefix = clusterId + ":";
    final List<ChangePlan> result = new ArrayList<>();
    plansColumnFamilies.forEach(
        (key, value) -> {
          if (!key.toString().startsWith(prefix)) {
            return;
          }
          final ChangePlan plan = value.getRecord();
          if (plan.changeId().equals(operationId)
              || BrokerClusterChangeIds.extractOperationId(plan.changeId()).equals(operationId)) {
            result.add(plan);
          }
        });
    result.sort(
        Comparator.comparingInt(ChangePlan::partitionId).thenComparing(ChangePlan::changeId));
    return result;
  }

  @Override
  public boolean hasPendingPlanForPartition(final String clusterId, final int partitionId) {
    pendingKey.wrapString(pendingKey(clusterId, partitionId));
    return pendingColumnFamilies.exists(pendingKey);
  }

  @Override
  public boolean ownsPendingPlan(final ChangePlan plan) {
    pendingKey.wrapString(pendingKey(plan.clusterId(), plan.partitionId()));
    final DbString owner = pendingColumnFamilies.get(pendingKey);
    return owner != null && owner.toString().equals(plan.changeId());
  }

  @Override
  public boolean hasChangePlanForPartition(final String clusterId, final int partitionId) {
    final String prefix = clusterId + ":";
    final boolean[] found = {false};
    plansColumnFamilies.whileTrue(
        (key, value) -> {
          if (key.toString().startsWith(prefix) && value.getRecord().partitionId() == partitionId) {
            found[0] = true;
            return false;
          }
          return true;
        });
    return found[0];
  }

  @Override
  public boolean isOperationApplied(final String clusterId, final String opId) {
    operationKey.wrapString(operationKey(clusterId, opId));
    return appliedOperationsColumnFamilies.exists(operationKey);
  }

  @Override
  public void putChangePlan(final ChangePlan plan) {
    planKey.wrapString(planKey(plan.clusterId(), plan.changeId()));
    planValue.setRecord(plan);
    plansColumnFamilies.upsert(planKey, planValue);
    execution.markPartition(plan, ownsPendingPlan(plan));
    if (plan.isTerminal()) {
      prunePartitionHistory(plan.clusterId(), plan.partitionId(), plan.changeId());
    }
  }

  @Override
  public void putClusterChangeOperation(final ClusterChangeOperation operation) {
    clusterChangeKey.wrapString(planKey(operation.clusterId(), operation.changeId()));
    clusterChangeValue.setRecord(operation);
    clusterChangesColumnFamilies.upsert(clusterChangeKey, clusterChangeValue);
    final var active = activeClusterChange(operation.clusterId());
    activeChangeKey.wrapString(operation.clusterId());
    if (operation.status().isTerminal()) {
      // A recovery history update must not remove an unrelated active topology operation.
      if (active.isEmpty()) {
        activeChangesColumnFamilies.deleteIfExists(activeChangeKey);
        recentClusterChanges(operation.clusterId(), Integer.MAX_VALUE).stream()
            .filter(change -> !change.status().isTerminal())
            .findFirst()
            .ifPresent(
                change -> {
                  activeChangeId.wrapString(change.changeId());
                  activeChangesColumnFamilies.upsert(activeChangeKey, activeChangeId);
                });
      }
    } else if (active.isEmpty() || active.orElseThrow().changeId().equals(operation.changeId())) {
      activeChangeId.wrapString(operation.changeId());
      activeChangesColumnFamilies.upsert(activeChangeKey, activeChangeId);
    }
    pruneClusterChangeHistory(operation.clusterId(), operation.changeId());
  }

  @Override
  public void removeActiveClusterChange(final String clusterId) {
    activeChangeKey.wrapString(clusterId);
    activeChangesColumnFamilies.deleteIfExists(activeChangeKey);
  }

  @Override
  public void markPending(final ChangePlan plan) {
    pendingKey.wrapString(pendingKey(plan.clusterId(), plan.partitionId()));
    pendingChangeId.wrapString(plan.changeId());
    pendingColumnFamilies.upsert(pendingKey, pendingChangeId);
    pendingIndexColumnFamilies.upsert(
        wrapIndexedKey(plan.clusterId(), plan.partitionId()), pendingChangeId);
    execution.markPartition(plan, true);
  }

  @Override
  public void removePending(final String clusterId, final int partitionId) {
    pendingKey.wrapString(pendingKey(clusterId, partitionId));
    pendingColumnFamilies.deleteIfExists(pendingKey);
    pendingIndexColumnFamilies.deleteIfExists(wrapIndexedKey(clusterId, partitionId));
    execution.removeActive(Scope.BROKER_PARTITION, clusterId, partitionId);
  }

  @Override
  public void markOperationApplied(final String clusterId, final String opId) {
    if (opId == null || opId.isBlank()) {
      return;
    }
    operationKey.wrapString(operationKey(clusterId, opId));
    appliedOperationsColumnFamilies.upsert(operationKey, DbNil.INSTANCE);
  }

  @Override
  public void deleteCluster(final String clusterId) {
    final String prefix = clusterId + ":";
    final List<String> planKeys = new ArrayList<>();
    final List<String> pendingKeys = new ArrayList<>();
    final List<String> operationKeys = new ArrayList<>();
    final List<String> clusterChangeKeys = new ArrayList<>();
    plansColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            planKeys.add(key.toString());
          }
        });
    pendingColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            pendingKeys.add(key.toString());
          }
        });
    appliedOperationsColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            operationKeys.add(key.toString());
          }
        });
    clusterChangesColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(prefix)) {
            clusterChangeKeys.add(key.toString());
          }
        });
    planKeys.forEach(
        key -> {
          planKey.wrapString(key);
          plansColumnFamilies.deleteIfExists(planKey);
        });
    pendingKeys.forEach(
        key -> {
          execution.removeActive(
              Scope.BROKER_PARTITION, clusterId, Integer.parseInt(key.substring(prefix.length())));
          pendingIndexColumnFamilies.deleteIfExists(
              wrapIndexedKey(clusterId, Integer.parseInt(key.substring(prefix.length()))));
          pendingKey.wrapString(key);
          pendingColumnFamilies.deleteIfExists(pendingKey);
        });
    operationKeys.forEach(
        key -> {
          operationKey.wrapString(key);
          appliedOperationsColumnFamilies.deleteIfExists(operationKey);
        });
    clusterChangeKeys.forEach(
        key -> {
          clusterChangeKey.wrapString(key);
          clusterChangesColumnFamilies.deleteIfExists(clusterChangeKey);
        });
    execution.deleteResource(Scope.BROKER_PARTITION, clusterId);
    removeActiveClusterChange(clusterId);
  }

  private void pruneClusterChangeHistory(final String clusterId, final String protectedChangeId) {
    final List<ClusterChangeOperation> history = recentClusterChanges(clusterId, Integer.MAX_VALUE);
    int entriesToDelete =
        (int) history.stream().filter(change -> change.status().isTerminal()).count()
            - historyLimit;
    for (int index = history.size() - 1; index >= 0 && entriesToDelete > 0; index--) {
      final ClusterChangeOperation candidate = history.get(index);
      if (candidate.changeId().equals(protectedChangeId) || !candidate.status().isTerminal()) {
        continue;
      }
      final var pairedMember = memberChange(clusterId, candidate.changeId());
      if (pairedMember != null
          && (!pairedMember.isTerminal()
              || execution.tasks(Scope.BROKER_MEMBER, clusterId, candidate.changeId()).stream()
                  .anyMatch(task -> !task.status().terminal()))) {
        continue;
      }
      final List<String> childIds = new ArrayList<>(candidate.planChangeIds());
      if (pairedMember != null) {
        childIds.addAll(pairedMember.getChildPartitionChangeIds());
      }
      if (!deleteTerminalPlans(
          clusterId,
          childIds,
          candidate.changeId(),
          pairedMember == null ? "" : candidate.changeId())) {
        continue;
      }
      if (pairedMember != null) {
        execution.deletePlan(Scope.BROKER_MEMBER, clusterId, candidate.changeId());
        memberChangesColumnFamilies.deleteIfExists(
            wrapMemberChangeKey(clusterId, candidate.changeId()));
      }
      deleteClusterChange(clusterId, candidate.changeId());
      entriesToDelete--;
    }
  }

  @Override
  public boolean deleteTerminalMemberPlans(
      final String clusterId, final String memberChangeId, final List<String> planIds) {
    final var pairedCluster = clusterChangeOperation(clusterId, memberChangeId).orElse(null);
    if (pairedCluster != null && !pairedCluster.status().isTerminal()) {
      return false;
    }
    final List<String> childIds = new ArrayList<>(planIds);
    if (pairedCluster != null) {
      childIds.addAll(pairedCluster.planChangeIds());
    }
    if (!deleteTerminalPlans(
        clusterId, childIds, pairedCluster == null ? "" : memberChangeId, memberChangeId)) {
      return false;
    }
    if (pairedCluster != null) {
      deleteClusterChange(clusterId, memberChangeId);
    }
    return true;
  }

  private MemberChangeRecord memberChange(final String clusterId, final String changeId) {
    final var value =
        memberChangesColumnFamilies.get(
            wrapMemberChangeKey(clusterId, changeId), MemberChangeRecordValue::new);
    return value == null ? null : value.getRecord();
  }

  // ["BROKER:clusterId:changeId"]: parent plan key in the shared PD_MEMBER_CHANGES column.
  private DbString wrapMemberChangeKey(final String clusterId, final String changeId) {
    memberChangeKey.wrapString(MemberChangeScope.BROKER.name() + ":" + clusterId + ":" + changeId);
    return memberChangeKey;
  }

  private void deleteClusterChange(final String clusterId, final String changeId) {
    clusterChangeKey.wrapString(planKey(clusterId, changeId));
    clusterChangesColumnFamilies.deleteIfExists(clusterChangeKey);
    activeChangeKey.wrapString(clusterId);
    final var active = activeChangesColumnFamilies.get(activeChangeKey);
    if (active != null && changeId.equals(active.toString())) {
      activeChangesColumnFamilies.deleteIfExists(activeChangeKey);
    }
  }

  private List<ChangePlan> allPlans(final String clusterId) {
    final List<ChangePlan> result = new ArrayList<>();
    plansColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(clusterId + ":")) {
            final var plan = value.getRecord();
            if (plan.clusterId().equals(clusterId)) {
              result.add(plan);
            }
          }
        });
    return result;
  }

  private Set<String> referencedPlans(
      final String clusterId,
      final String excludedClusterChange,
      final String excludedMemberChange) {
    final Set<String> referenced = new HashSet<>();
    recentClusterChanges(clusterId, Integer.MAX_VALUE).stream()
        .filter(change -> !change.changeId().equals(excludedClusterChange))
        .forEach(change -> referenced.addAll(change.planChangeIds()));
    memberChangesColumnFamilies.forEach(
        (key, value) -> {
          if (key.toString().startsWith(MemberChangeScope.BROKER.name() + ":" + clusterId + ":")) {
            final var member = value.getRecord();
            if (member.getScope() == MemberChangeScope.BROKER
                && member.getResourceId().equals(clusterId)
                && !member.getChangeId().equals(excludedMemberChange)) {
              referenced.addAll(member.getChildPartitionChangeIds());
            }
          }
        });
    return referenced;
  }

  private static List<ChangePlan> chain(final List<ChangePlan> all, final List<String> seeds) {
    final Set<String> ids = new HashSet<>(seeds);
    boolean changed;
    do {
      changed = false;
      for (final var plan : all) {
        if (ids.contains(plan.changeId())
            || ids.contains(plan.rootChangeId())
            || (!plan.execution().predecessor().isBlank()
                && ids.contains(plan.execution().predecessor()))
            || (!plan.execution().successor().isBlank()
                && ids.contains(plan.execution().successor()))) {
          changed |= ids.add(plan.changeId());
          changed |= ids.add(plan.rootChangeId());
          if (!plan.execution().predecessor().isBlank()) {
            changed |= ids.add(plan.execution().predecessor());
          }
          if (!plan.execution().successor().isBlank()) {
            changed |= ids.add(plan.execution().successor());
          }
        }
      }
    } while (changed);
    return all.stream().filter(plan -> ids.contains(plan.changeId())).toList();
  }

  private boolean deleteTerminalPlans(
      final String clusterId,
      final List<String> seeds,
      final String excludedClusterChange,
      final String excludedMemberChange) {
    final var all = allPlans(clusterId);
    final var candidates = chain(all, seeds);
    final var referenced = referencedPlans(clusterId, excludedClusterChange, excludedMemberChange);
    if (candidates.stream()
        .anyMatch(
            plan ->
                !plan.isTerminal()
                    || ownsPendingPlan(plan)
                    || referenced.contains(plan.changeId())
                    || execution.tasks(Scope.BROKER_PARTITION, clusterId, plan.changeId()).stream()
                        .anyMatch(task -> !task.status().terminal()))) {
      return false;
    }
    final Set<String> removedIds =
        candidates.stream().map(ChangePlan::changeId).collect(Collectors.toSet());
    final Set<String> retainedOperations =
        all.stream()
            .filter(plan -> !removedIds.contains(plan.changeId()))
            .flatMap(plan -> plan.operations().stream())
            .map(op -> op.opId())
            .collect(Collectors.toSet());
    for (final var candidate : candidates) {
      execution.deletePlan(Scope.BROKER_PARTITION, clusterId, candidate.changeId());
      planKey.wrapString(planKey(clusterId, candidate.changeId()));
      plansColumnFamilies.deleteIfExists(planKey);
      candidate.operations().stream()
          .filter(op -> !retainedOperations.contains(op.opId()))
          .forEach(
              op -> {
                operationKey.wrapString(operationKey(clusterId, op.opId()));
                appliedOperationsColumnFamilies.deleteIfExists(operationKey);
              });
    }
    return true;
  }

  private void prunePartitionHistory(
      final String clusterId, final int partitionId, final String protectedChangeId) {
    final var all = allPlans(clusterId);
    final var referenced = referencedPlans(clusterId, "", "");
    final Set<String> seen = new HashSet<>();
    final List<ChangePlan> roots = new ArrayList<>();
    for (final var plan : all) {
      if (plan.partitionId() != partitionId || !seen.add(plan.rootChangeId())) {
        continue;
      }
      final var group = chain(all, List.of(plan.rootChangeId()));
      if (group.stream().allMatch(ChangePlan::isTerminal)
          && group.stream().noneMatch(item -> referenced.contains(item.changeId()))) {
        roots.add(
            group.stream()
                .min(
                    Comparator.comparingLong(ChangePlan::startedAtEpochMillis)
                        .thenComparing(ChangePlan::changeId))
                .orElseThrow());
      }
    }
    roots.sort(
        Comparator.comparingLong(ChangePlan::startedAtEpochMillis)
            .thenComparing(ChangePlan::changeId));
    int excess = roots.size() - historyLimit;
    for (final var root : roots) {
      if (excess <= 0) {
        break;
      }
      final var group = chain(all, List.of(root.rootChangeId()));
      if (group.stream().anyMatch(plan -> plan.changeId().equals(protectedChangeId))) {
        continue;
      }
      if (deleteTerminalPlans(clusterId, List.of(root.rootChangeId()), "", "")) {
        excess--;
      }
    }
  }

  // ["clusterId:changeId"]: shared key format for partition plans and cluster change operations.
  private static String planKey(final String clusterId, final String changeId) {
    return clusterId + ":" + changeId;
  }

  // ["clusterId:partitionId"]: identifies the current pending owner of a partition.
  private static String pendingKey(final String clusterId, final int partitionId) {
    return clusterId + ":" + partitionId;
  }

  // ["clusterId:operationId"]: deduplication key for an applied topology operation.
  private static String operationKey(final String clusterId, final String opId) {
    return clusterId + ":" + opId;
  }
}
