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
import React from 'react';
import {Alert, Card, Progress, Table, Tag, Tooltip} from 'antd';
import {Clock} from 'lucide-react';
import {ChangeStatus, TopologyOperationType} from '@/shared/api/enums';
import type {BrokerChangeDetail, PendingItem, PlannedOperation} from '@/shared/api/types';
import {CopyableId} from '@/shared/ui/CopyableId';
import {isChangeTerminal, StatusTag} from '@/shared/ui/StatusTag';

const operationLabels: Record<TopologyOperationType, string> = {
  ADD: '添加副本',
  JOIN: '加入成员',
  PROMOTE_LEARNER: '提升为 Voter',
  LEAVE: '移除成员',
  RECONFIGURE_PRIORITY: '调整优先级',
  TRANSFER_LEADER: '转移 Leader',
  STEP_DOWN_IF_NOT_PRIMARY: '非主节点退位',
  RESET_PEERS: '重置成员',
};

function operationLabel(operation: PlannedOperation) {
  return operationLabels[operation.type] || operation.type;
}

function completedCount(row: PendingItem) {
  return Math.min(row.operationCount, Math.max(0, row.nextOperationIndex));
}

function nextOperationStatus(row: PendingItem): ChangeStatus {
  if (isChangeTerminal(row.status)) {
    return row.status === ChangeStatus.COMPLETED ? ChangeStatus.PENDING : row.status;
  }
  return row.blockedReason ? ChangeStatus.WAITING_READY : row.status;
}

function ExecutionStatus({row, blockedReasons, status = row.status}: {
  row: PendingItem;
  blockedReasons: Record<string, string>;
  status?: ChangeStatus;
}) {
  if (row.blockedReason && !isChangeTerminal(row.status)) {
    return <Tooltip title={blockedReasons[row.blockedReason] || row.blockedReason}>
      <Tag color="warning" icon={<Clock size={12} />} className="inline-flex items-center gap-1">等待中</Tag>
    </Tooltip>;
  }
  return <StatusTag status={status} category="change" />;
}

function PartitionDetails({row, blockedReasons}: {row: PendingItem; blockedReasons: Record<string, string>}) {
  const completed = completedCount(row);
  const remaining = row.remainingOperations.map((operation, index) => ({
    ...operation,
    step: completed + index + 1,
    status: index === 0 ? nextOperationStatus(row) :
      isChangeTerminal(row.status) ? nextOperationStatus(row) : ChangeStatus.PENDING,
  }));
  return <div className="partition-execution-details">
    {row.blockedReason && <Alert showIcon type="warning"
      message={blockedReasons[row.blockedReason] || row.blockedReason}
      description={`开始等待：${row.blockedSince || '—'}`} />}
    {remaining.length > 0 ? <Table
      className="partition-operation-table"
      dataSource={remaining}
      rowKey="opId"
      size="small"
      pagination={false}
      scroll={{x: 720}}
      columns={[
        {title: '步骤', dataIndex: 'step', width: 65},
        {title: '操作', width: 170, render: (_, operation) =>
          <span title={operation.type}>{operationLabel(operation)}</span>},
        {title: '目标 Broker', dataIndex: 'targetBroker', width: 220,
          render: (id: string) => <CopyableId id={id} maxLength={28} />},
        {title: '状态', dataIndex: 'status', width: 160,
          render: (status: ChangeStatus, operation) => operation.step === completed + 1
            ? <ExecutionStatus row={row} blockedReasons={blockedReasons} status={status} />
            : <StatusTag status={status} category="change" />},
        {title: '操作 ID', dataIndex: 'opId', width: 210,
          render: (id: string) => <CopyableId id={id} maxLength={24} />},
      ]}
    /> : <p className="partition-execution-note">{row.status === ChangeStatus.COMPLETED
      ? '全部操作已完成' : completed === row.operationCount
        ? '操作已全部应用，等待分区就绪' : '暂无后续操作'}</p>}
    <dl className="partition-execution-metadata">
      <div><dt>开始时间</dt><dd>{row.startedAt || '—'}</dd></div>
      <div><dt>执行计划</dt><dd><CopyableId id={row.changeId || '—'} maxLength={32} /></dd></div>
      <div><dt>执行代次</dt><dd>{row.executionGeneration || '0'}</dd></div>
      <div><dt>权限提交索引</dt><dd>{row.authorityIndex || '0'}</dd></div>
    </dl>
  </div>;
}

export function BrokerChangePartitions({data, blockedReasons}: {
  data: BrokerChangeDetail;
  blockedReasons: Record<string, string>;
}) {
  // Keep terminal leaves visible after they disappear from pendingChanges.
  // A pending successor always takes precedence over its partition's history.
  const byPartition = new Map<number, PendingItem>();
  for (const row of data.executionHistory || []) {
    if (isChangeTerminal(row.status) && row.status !== ChangeStatus.SUPERSEDED && !row.supersededBy) {
      byPartition.set(row.partitionId, row);
    }
  }
  for (const row of data.pendingChanges) byPartition.set(row.partitionId, row);
  const rows = [...byPartition.values()].sort((a, b) => a.partitionId - b.partitionId);
  if (!rows.length) return null;

  return <Card className="shadow-sm partition-execution-card"
    title={<div className="partition-execution-title">
      <span className="text-sm font-semibold">待执行分区</span>
      <span className="partition-execution-note">共 {rows.length} 个分区</span>
    </div>}>
    <Table
      dataSource={rows}
      rowKey="partitionId"
      size="small"
      pagination={false}
      scroll={{x: 880}}
      expandable={{expandedRowRender: row => <PartitionDetails row={row} blockedReasons={blockedReasons} />}}
      columns={[
        {title: '分区', dataIndex: 'partitionId', width: 70},
        {title: '状态', width: 170,
          render: (_, row) => <ExecutionStatus row={row} blockedReasons={blockedReasons} />},
        {title: '操作进度', width: 230, render: (_, row) => {
          const completed = completedCount(row);
          return <div className="partition-progress">
            <div><span>已完成 <strong>{completed}</strong> / {row.operationCount}</span>
              <span className="partition-execution-note">剩余 {row.operationCount - completed}</span></div>
            <Progress percent={row.operationCount ? Math.round(completed / row.operationCount * 100) : 0}
              showInfo={false} size="small"
              strokeColor="hsl(var(--success))" trailColor="hsl(var(--border))" />
          </div>;
        }},
        {title: '当前 / 下一步', width: 240, render: (_, row) =>
          row.status === ChangeStatus.COMPLETED ? <span className="text-[hsl(var(--success))]">全部完成</span> :
            isChangeTerminal(row.status) ? <span className="partition-execution-note">已终止</span> :
            row.currentOperation ? <div className="partition-operation-preview">
              <span>{operationLabel(row.currentOperation)}</span>
              <span className="partition-execution-note">{row.currentOperation.targetBroker}</span>
            </div> : <span className="partition-execution-note">
              {completedCount(row) === row.operationCount && !isChangeTerminal(row.status) ? '等待分区就绪' : '—'}
            </span>},
        {title: '截止时间', dataIndex: 'deadline', width: 220,
          render: (value: string) => <span className="partition-execution-note">{value || '—'}</span>},
      ]}
    />
  </Card>;
}
