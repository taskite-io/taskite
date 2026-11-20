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
import React, { useMemo } from 'react';
import { Empty, Tooltip } from 'antd';
import type { BrokerInfo, BrokerTopology } from '@/shared/api/types';
import {MemberRole, RegistrationStatus} from '@/shared/api/enums';

enum PartitionRole {
  LEADER = 'leader',
  FOLLOWER = 'follower',
  LEARNER = 'learner',
  UNKNOWN = 'unknown',
}

const observedRoles: Partial<Record<MemberRole, PartitionRole>> = {
  [MemberRole.LEADER]: PartitionRole.LEADER,
  [MemberRole.FOLLOWER]: PartitionRole.FOLLOWER,
  [MemberRole.LEARNER]: PartitionRole.LEARNER,
};

const roles: { role: PartitionRole; label: string; description: string }[] = [
  { role: PartitionRole.LEADER, label: 'Leader', description: '节点上报为分区 Leader' },
  { role: PartitionRole.FOLLOWER, label: 'Follower', description: '节点上报为分区 Follower' },
  { role: PartitionRole.LEARNER, label: 'Learner', description: '节点上报为分区 Learner' },
  { role: PartitionRole.UNKNOWN, label: 'Unknown', description: '角色无法确认，可能因离线、网络隔离或缺少有效上报' },
];
const brokerOrder = new Intl.Collator('en', { numeric: true });

export const PartitionOwnershipMatrix: React.FC<{
  topology: BrokerTopology;
  onBrokerSelect: (broker: BrokerInfo) => void;
}> = ({ topology, onBrokerSelect }) => {
  const { partitionIds, rows } = useMemo(() => {
    const placements = new Map(topology.partitions.map((partition) => [partition.partitionId, partition]));
    const brokers = new Map(topology.brokers.map((broker) => [broker.brokerId, broker]));
    const ids = new Set(placements.keys());
    const brokerIds = new Set(brokers.keys());
    topology.partitions.forEach((partition) => {
      [...partition.replicas, ...partition.learners, partition.leaderId].filter(Boolean)
        .forEach((id) => brokerIds.add(id));
    });
    topology.brokers.forEach((broker) => broker.partitions.forEach((report) => ids.add(report.partitionId)));
    const partitionIds = [...ids].sort((a, b) => a - b);

    return {
      partitionIds,
      rows: [...brokerIds].sort(brokerOrder.compare).map((brokerId) => {
        const broker = brokers.get(brokerId);
        const reports = new Map(broker?.partitions.map((report) => [report.partitionId, report]) || []);
        const online = broker?.status === RegistrationStatus.ONLINE;
        return {
          brokerId, broker, online,
          cells: partitionIds.map((partitionId) => {
            const placement = placements.get(partitionId);
            const report = reports.get(partitionId);
            const assigned = !!report || placement?.leaderId === brokerId
              || placement?.replicas.includes(brokerId) || placement?.learners.includes(brokerId);
            if (!assigned) return { partitionId, role: null, reason: '未承载此分区' };
            const role = online && report ? observedRoles[report.role] ?? PartitionRole.UNKNOWN : PartitionRole.UNKNOWN;
            const reason = !broker ? '节点未注册或已失去上报，当前角色无法确认'
              : !online ? '节点离线或不可达，最后上报的角色不代表当前角色'
              : !report ? '缺少此分区的有效上报，当前角色无法确认'
              : role === PartitionRole.UNKNOWN ? '节点未能确认角色，可能存在网络隔离或分区状态不可达'
              : `节点当前上报为 ${roles.find((entry) => entry.role === role)!.label}`;
            return { partitionId, role, reason };
          }),
        };
      }),
    };
  }, [topology]);

  if (!partitionIds.length || !rows.length) {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无分区归属信息" />;
  }

  return (
    <div className="partition-ownership">
      <div className="partition-matrix-toolbar">
        <span className="partition-matrix-summary">{rows.length} 个节点 · {partitionIds.length} 个分区</span>
        <div className="partition-role-legend" aria-label="分区角色图例">
          {roles.map(({ role, label, description }) => (
            <Tooltip key={role} title={description}>
              <span className={`partition-role-legend-item partition-role-${role}`}>
                {label}
              </span>
            </Tooltip>
          ))}
        </div>
      </div>
      <div className="partition-matrix-scroll" tabIndex={0} role="region" aria-label="分区归属矩阵，可横向滚动">
        <table className="partition-matrix" aria-label="Broker 分区归属" style={{ minWidth: 216 + partitionIds.length * 76 }}>
          <caption className="sr-only">行是 Broker 节点，列是按数值升序排列的分区 ID，单元格表示节点上报角色</caption>
          <thead>
            <tr>
              <th scope="col" className="partition-matrix-node-column">Broker ID</th>
              {partitionIds.map((id) => <th scope="col" key={id} data-partition-id={id}>{id}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map(({ brokerId, broker, online, cells }) => (
              <tr key={brokerId} data-broker-id={brokerId}>
                <th scope="row" className="partition-matrix-node-column">
                  <Tooltip title={`${brokerId} · ${online ? '在线' : broker ? '离线 / 不可达' : '未上报'}`}>
                    {broker ? (
                      <button className="partition-matrix-node" onClick={() => onBrokerSelect(broker)}>
                        <span className={`partition-node-dot ${online ? 'partition-node-online' : ''}`} />
                        <span className="font-mono-id">{brokerId}</span>
                      </button>
                    ) : (
                      <span className="partition-matrix-node"><span className="partition-node-dot" /><span className="font-mono-id">{brokerId}</span></span>
                    )}
                  </Tooltip>
                </th>
                {cells.map(({ partitionId, role, reason }) => (
                  <td key={partitionId} data-partition-id={partitionId} data-role={role || 'unassigned'}>
                    <Tooltip title={`${brokerId} · 分区 ${partitionId}：${reason}`}>
                      <span className={`partition-role-cell ${role ? `partition-role-${role}` : 'partition-role-unassigned'}`}>
                        {role ? roles.find((entry) => entry.role === role)!.label : '—'}
                      </span>
                    </Tooltip>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="partition-matrix-note">Unknown 表示无法确认当前角色，常见于离线、网络隔离或缺少有效上报；「—」表示未承载该分区。</p>
    </div>
  );
};
