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
import {RegistrationStatus} from '@/shared/api/enums';
import type {BrokerTopology, ClusterConfiguration} from '@/shared/api/types';

export interface ConfigurationDraft {
  brokersCount?: number;
  partitionCount?: number;
  replicationFactor?: number;
  brokersAdd?: string[];
  brokersRemove?: string[];
}

export function validateConfiguration(draft: ConfigurationDraft, config: ClusterConfiguration, topology: BrokerTopology | null) {
  const errors: string[] = [];
  const additions = new Set(draft.brokersAdd ?? []);
  const removals = new Set(draft.brokersRemove ?? []);
  // Older PD versions omit the roster; use observed voters only if the complete roster is known.
  const observedVoters = new Set(topology?.partitions.flatMap(p => [p.leaderId, ...p.replicas]).filter(Boolean) ?? []);
  const roster = new Set(config.voterRoster ?? observedVoters);
  const rosterKnown = roster.size === config.clusterSize;
  const target = new Set([...roster].filter(id => !removals.has(id)));
  additions.forEach(id => target.add(id));
  const memberChange = additions.size > 0 || removals.size > 0;
  const resizing = draft.brokersCount != null && draft.brokersCount !== config.clusterSize;
  const replicaChange = draft.replicationFactor != null && draft.replicationFactor !== config.replicationFactor;
  const brokers = topology?.brokers ?? [];
  const online = new Set(brokers.filter(b => b.status === RegistrationStatus.ONLINE).map(b => b.brokerId));
  const availableVoters = new Set(brokers.filter(b => [RegistrationStatus.ONLINE, RegistrationStatus.DRAINING].includes(b.status))
    .map(b => b.brokerId));
  const targetCount = draft.brokersCount ?? (memberChange ? target.size : config.clusterSize);
  const replicationFactor = draft.replicationFactor ?? config.replicationFactor;
  if (!Number.isInteger(targetCount) || targetCount < 1) errors.push('目标 Broker 数量必须为大于 0 的整数');
  if (!Number.isInteger(replicationFactor) || replicationFactor < 1) errors.push('副本因子必须为大于 0 的整数');
  if ([...additions].some(id => removals.has(id))) errors.push('添加和移除列表存在交集');
  if ((memberChange || resizing) && !rosterKnown) errors.push('当前成员名单不完整，无法校验目标数量，请刷新配置后重试');
  const unknownRemovals = [...removals].filter(id => !roster.has(id) && !brokers.some(b => b.brokerId === id));
  if (unknownRemovals.length) errors.push(`移除列表中存在未知 Broker：${unknownRemovals.join('、')}`);
  if (memberChange && rosterKnown && draft.brokersCount != null && targetCount !== target.size) {
    errors.push(`添加 / 移除后目标 Broker 为 ${target.size} 个，与填写的目标数量 ${targetCount} 不一致`);
  }
  if (!memberChange && resizing && Number.isInteger(targetCount) && targetCount > 0) {
    if (target.size < targetCount) {
      brokers.filter(b => online.has(b.brokerId) && !target.has(b.brokerId))
        .map(b => b.brokerId).sort().slice(0, targetCount - target.size).forEach(id => target.add(id));
    } else {
      [...target].sort().reverse().slice(0, target.size - targetCount).forEach(id => target.delete(id));
    }
    if (rosterKnown && target.size !== targetCount) errors.push(`当前在线节点不足，无法达到目标 Broker 数量 ${targetCount}`);
  }
  if (targetCount < replicationFactor) {
    errors.push(`变更后目标 Broker 仅 ${targetCount} 个，少于副本因子 ${replicationFactor}；请减少移除节点或调整副本因子`);
  }
  const currentQuorum = Math.floor(config.replicationFactor / 2) + 1;
  if (replicationFactor < config.replicationFactor && replicationFactor < currentQuorum) {
    errors.push(`副本因子不能低于当前多数派 ${currentQuorum}，请分步调整`);
  }
  const removedVoters = new Set([...roster].filter(id => !target.has(id)));
  if ((memberChange || resizing || replicaChange) && rosterKnown) {
    const onlineTargets = [...target].filter(id => online.has(id) && !removals.has(id)).length;
    if (onlineTargets < replicationFactor) {
      errors.push(`目标成员中仅 ${onlineTargets} 个 Broker 在线，副本因子 ${replicationFactor} 需要至少 ${replicationFactor} 个在线节点`);
    }
  }
  if (removedVoters.size || replicationFactor < config.replicationFactor) {
    const partitions = topology?.partitions ?? [];
    if (partitions.length < config.partitionSize) errors.push('分区拓扑不完整，无法校验在线多数派，请刷新后重试');
    const losesQuorum = partitions.some(partition => {
      const voters = new Set([partition.leaderId, ...partition.replicas].filter(Boolean));
      const quorum = Math.floor(voters.size / 2) + 1;
      // PD promotes replacements before removing old voters. Planned removals still vote while
      // available; new Learners do not count towards the existing Raft configuration's quorum.
      const available = [...voters].filter(id => availableVoters.has(id)).length;
      return available < quorum;
    });
    if (losesQuorum) {
      errors.push('当前分区的可用投票节点不足以维持多数派，请先恢复原投票节点');
    }
  }
  return {targetCount, replicationFactor, errors};
}
