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
/**
 * 变更详情页面
 * PD: sourceEpoch→targetEpoch、target/actual 成员、operation 计划
 * Broker: currentTopology/expectedTopology、plannedChanges、pendingChanges
 * 支持 Broker 取消
 */

import {ChangeStatus, ChangeType} from '@/shared/api/enums';
import React, { useCallback, useState, useEffect } from 'react';
import { Card, Tag, Button, Table, Alert, Skeleton, Empty, Row, Col } from 'antd';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { ListChecks, Ban, ArrowRightLeft, ChevronDown, ChevronUp } from 'lucide-react';
import { StatusTag, isChangeTerminal } from '@/shared/ui/StatusTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import { ConfirmModal } from '@/shared/ui/ConfirmModal';
import { PageHeader, DetailSection, MetricStrip } from '@/shared/ui/DetailPrimitives';
import { formatInt64 } from '@/shared/lib/format';
import { usePolling } from '@/shared/hooks/usePolling';
import { apiClient } from '@/shared/api/client';
import {RequestState} from '@/shared/ui/RequestState';
import {useBootstrap} from '@/shared/ui/ConsoleBootstrap';
import {rememberChange} from '@/shared/api/history';
import {PDChangeDetails} from './PDChangeDetails';
import {BrokerChangePartitions} from './BrokerChangePartitions';
import type { PDChangeDetail, BrokerChangeDetail } from '@/shared/api/types';

const POLLING_INTERVAL = 2000;
const ORIGINAL_PLAN_COLLAPSE_THRESHOLD = 30;
const cancellationReasons: Record<string, string> = {
  CHANGE_TERMINAL: '变更已经结束。',
  CHANGE_SUPERSEDED: '该执行计划已被替代，请查看后继计划。取消旧计划不能停止当前恢复。',
  CHANGE_RECONCILING: '正在核验已提交的 Raft 配置，暂不可取消。请等待对账；若缺少多数派，需要恢复原投票节点。',
  CHANGE_RECOVERY_REQUIRED: '该计划负责完成前序变更的恢复，需保留至配置对账和恢复完成，暂不可取消。',
  CHANGE_NOT_CURRENT: '该计划已不再拥有分区执行权限，请刷新查看当前计划。',
};
const cancellationMessage = (data: BrokerChangeDetail) =>
  cancellationReasons[data.cancellationReason || ''] || data.cancellationReason ||
  '暂无法确认取消条件，请刷新后重试。';

const blockedReasons: Record<string, string> = {
  TARGET_UNAVAILABLE: '目标节点已下线',
  NO_QUORUM: '已提交配置的在线投票节点不足多数派，等待恢复',
  INSUFFICIENT_CAPACITY: '可用节点不足以满足副本数',
  PLAN_DEADLINE_EXCEEDED: '原执行路径已超时，正在核验实际配置',
  JOB_SUBSCRIPTION_DISTRIBUTION_TIMEOUT: '等待任务订阅分发确认，已超过计划期限',
  LEADER_UNAVAILABLE: '等待有效 Leader',
  CONFIGURATION_READ_UNAVAILABLE: '暂时无法读取已提交配置',
  AUTHORITY_INSTALL_UNAVAILABLE: '等待确认执行权限',
  CONFIGURATION_CONFLICT: '配置已变化，正在对账',
  EXECUTION_AUTHORITY_CONFLICT: '执行权限不一致，等待核验',
  INCOMPLETE_SOURCE_CONFIGURATION: '配置证据不完整，等待重新读取',
  BROKER_UNAVAILABLE: 'Broker 暂时不可达，等待重试',
};

export const ChangeDetailPage: React.FC<{ type?: 'pd' | 'cluster' }> = ({ type }) => {
  const { changeId, clusterId } = useParams<{ changeId: string; clusterId: string }>();
  const navigate = useNavigate();
  const decodedChangeId = changeId || '';
  const decodedClusterId = clusterId;
  const bootstrap = useBootstrap();
  const {state: navigationState} = useLocation();
  const recovery = navigationState?.recovery as {partitionId: number; survivor: string; recoveryEpoch: string} | undefined;

  const [cancelOpen, setCancelOpen] = useState(false);
  const [originalPlanExpanded, setOriginalPlanExpanded] = useState(false);

  useEffect(() => {
    setOriginalPlanExpanded(false);
  }, [decodedChangeId, decodedClusterId]);

  const fetchChange = useCallback(async (signal: AbortSignal) => {
    if (type === 'pd') {
      return apiClient.get<PDChangeDetail>(`/pd/changes/${encodeURIComponent(decodedChangeId)}`, signal);
    }
    return apiClient.get<BrokerChangeDetail>(
      `/broker/clusters/${encodeURIComponent(decodedClusterId!)}/changes/${encodeURIComponent(decodedChangeId)}`,
      signal
    );
  }, [type, decodedChangeId, decodedClusterId]);

  const change = usePolling(fetchChange, {
    interval: POLLING_INTERVAL,
    enabled: !!decodedChangeId,
    cacheKey: JSON.stringify(['change', type, decodedClusterId, decodedChangeId]),
    isTerminal: (d) => isChangeTerminal(d.status),
  });

  const handleCancel = async () => {
    if (type !== 'cluster' || !decodedClusterId) return;
    const current = await fetchChange(new AbortController().signal);
    if (isChangeTerminal(current.status)) {setCancelOpen(false); change.refresh(); return;}
    if ((current as BrokerChangeDetail).cancellable !== true) {
      change.refresh();
      throw new Error(cancellationMessage(current as BrokerChangeDetail));
    }
    await apiClient.delete(
      `/broker/clusters/${encodeURIComponent(decodedClusterId)}/changes/${encodeURIComponent(decodedChangeId)}`
    );
    setCancelOpen(false);
    change.refresh();
  };

  const data = change.data;
  useEffect(() => {
    if (data) rememberChange(bootstrap.deploymentId, {scope: type === 'pd' ? 'pd' : 'cluster', clusterId: decodedClusterId, changeId: data.changeId});
  }, [data?.changeId, bootstrap.deploymentId, type, decodedClusterId]);
  const isPd = type === 'pd';
  const pdData = data as PDChangeDetail | undefined;
  const brokerData = data as BrokerChangeDetail | undefined;
  const canCancel = brokerData?.cancellable === true && !change.stale && !change.error;
  const originalPlanCollapsible = (brokerData?.plannedChanges.length ?? 0) > ORIGINAL_PLAN_COLLAPSE_THRESHOLD;
  const showOriginalPlan = !originalPlanCollapsible || originalPlanExpanded;

  return (
    <div className="space-y-6">
      <RequestState state={{...change, loading: false}} />
      {recovery && <Alert type="info" showIcon message={`恢复请求已受理 · 分区 ${recovery.partitionId} · survivor ${recovery.survivor} · recoveryEpoch ${recovery.recoveryEpoch}`} />}
      {/* 页头 */}
      <PageHeader title={isPd ? 'PD 变更详情' : 'Broker 变更详情'} icon={<ListChecks size={22} />}
        subtitle={isPd ? '控制平面成员变更 · 操作计划与实时执行状态' : `集群 ${decodedClusterId} · 拓扑计划与实时执行状态`}
        badges={data && <StatusTag status={data.status} category="change" />}
        onBack={() => navigate(isPd ? '/changes?tab=pd'
          : `/changes?tab=broker&clusterId=${encodeURIComponent(decodedClusterId!)}`)} />

      {change.loading && !data ? (
        <Skeleton active paragraph={{ rows: 5 }} />
      ) : data ? (
        <div className="space-y-4">
          {/* 基本信息卡片 */}
          {!isPd && <DetailSection title="变更信息">
            <div className="object-summary">
              <div className="flex items-center gap-3 flex-wrap min-w-0 max-w-full">
                <CopyableId id={data.changeId} maxLength={48} />
                <Tag color="blue">{decodedClusterId}</Tag>
                {isChangeTerminal(data.status) && (
                  <Tag className="rounded-full" color={
                    data.status === ChangeStatus.COMPLETED ? 'success' :
                    data.status === ChangeStatus.FAILED ? 'error' : 'default'
                  }>终态</Tag>
                )}
              </div>
              {!isPd && !isChangeTerminal(data.status) && (
                <Button danger size="small" disabled={!canCancel} icon={<Ban size={12} />} onClick={() => setCancelOpen(true)}>
                  取消变更
                </Button>
              )}
            </div>

          </DetailSection>}
          {!isPd && brokerData && !isChangeTerminal(brokerData.status) && brokerData.cancellable !== true &&
            <Alert type="warning" showIcon message="暂不可取消" description={cancellationMessage(brokerData)} />}
          {isPd && pdData?.error && <Alert type="error" showIcon message={pdData.error} />}

          {isPd && pdData && <PDChangeDetails data={pdData} />}

          {/* Broker: topology comparison + plans */}
          {!isPd && brokerData && (
            <>
              <MetricStrip items={[
                { label: '当前拓扑', value: brokerData.currentTopology.length, hint: '分区' },
                { label: '期望拓扑', value: brokerData.expectedTopology.length, tone: 'brand', hint: '分区' },
                { label: '原始计划', value: brokerData.plannedChanges.length, hint: '操作' },
                { label: '待执行分区', value: brokerData.pendingChanges.length, tone: 'warning' },
              ]} />
              {/* 拓扑比较 */}
              <Card
                className="shadow-sm"
                title={
                  <div className="flex items-center gap-2">
                    <ArrowRightLeft size={14} className="text-[hsl(var(--primary))]" />
                    <span className="text-sm font-semibold">拓扑比较</span>
                  </div>
                }
              >
                <Row gutter={[16, 16]}>
                  <Col xs={24} lg={12}>
                    <div className="topology-column">
                    <div className="topology-column-header">
                      <Tag color="blue" className="rounded-full text-xs">当前拓扑</Tag>
                      <span className="text-xs text-[hsl(var(--muted-foreground))] ml-1">
                        {brokerData.currentTopology.length} 分区
                      </span>
                    </div>
                    <Table
                      dataSource={brokerData.currentTopology}
                      rowKey="partitionId"
                      size="small"
                      pagination={false}
                      expandable={{expandedRowRender: row => <div className="text-xs space-y-1">
                        <p>目标 Voters：{row.desiredVoters.join(', ') || '—'}</p>
                        <p>Learners：{row.learners.join(', ') || '—'}</p>
                        <p>Desired Primary：{row.desiredPrimary || '—'}</p>
                        <p>Priorities：{JSON.stringify(row.priorities)}</p>
                      </div>}}
                      scroll={{ x: 300 }}
                      columns={[
                        { title: 'Partition', dataIndex: 'partitionId', key: 'partitionId', width: 70 },
                        {
                          title: 'Status',
                          dataIndex: 'status',
                          key: 'status',
                          width: 100,
                          render: (v?: string) => <StatusTag status={v || 'UNKNOWN'} category="partition" className="topology-status-tag" />,
                        },
                        {
                          title: 'Voters',
                          dataIndex: 'currentVoters',
                          key: 'voters',
                          render: (v: string[]) => (
                            <div className="flex flex-wrap gap-0.5">
                              {v.map((id) => <Tag key={id} className="rounded-full text-xs topology-voter-tag">{id}</Tag>)}
                            </div>
                          ),
                        },
                      ]}
                    />
                    </div>
                  </Col>
                  <Col xs={24} lg={12}>
                    <div className="topology-column">
                    <div className="topology-column-header">
                      <Tag color="green" className="rounded-full text-xs">期望拓扑</Tag>
                      <span className="text-xs text-[hsl(var(--muted-foreground))] ml-1">
                        {brokerData.expectedTopology.length} 分区
                      </span>
                    </div>
                    <Table
                      dataSource={brokerData.expectedTopology}
                      rowKey="partitionId"
                      size="small"
                      pagination={false}
                      expandable={{expandedRowRender: row => <div className="text-xs space-y-1">
                        <p>当前 Voters：{row.currentVoters.join(', ') || '—'}</p>
                        <p>Learners：{row.learners.join(', ') || '—'}</p>
                        <p>Desired Primary：{row.desiredPrimary || '—'}</p>
                        <p>Priorities：{JSON.stringify(row.priorities)}</p>
                      </div>}}
                      scroll={{ x: 300 }}
                      columns={[
                        { title: 'Partition', dataIndex: 'partitionId', key: 'partitionId', width: 70 },
                        {
                          title: 'Status',
                          dataIndex: 'status',
                          key: 'status',
                          width: 100,
                          render: (v?: string) => <StatusTag status={v || 'UNKNOWN'} category="partition" className="topology-status-tag" />,
                        },
                        {
                          title: 'Voters',
                          dataIndex: 'desiredVoters',
                          key: 'voters',
                          render: (v: string[]) => (
                            <div className="flex flex-wrap gap-0.5">
                              {v.map((id) => <Tag key={id} className="rounded-full text-xs topology-voter-tag">{id}</Tag>)}
                            </div>
                          ),
                        },
                      ]}
                    />
                    </div>
                  </Col>
                </Row>
              </Card>

              {/* 原始计划 */}
              {brokerData.plannedChanges.length > 0 && (
                <Card
                  className="shadow-sm original-plan-card"
                  title={<div className="flex items-center gap-2">
                    <span className="text-sm font-semibold">原始计划</span>
                    <span className="text-xs font-normal text-[hsl(var(--muted-foreground))]">{brokerData.plannedChanges.length} 项</span>
                  </div>}
                  extra={originalPlanCollapsible && <Button
                    size="small"
                    type="text"
                    aria-expanded={showOriginalPlan}
                    icon={showOriginalPlan ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    onClick={() => setOriginalPlanExpanded(expanded => !expanded)}
                  >
                    {showOriginalPlan ? '收起计划' : '展开计划'}
                  </Button>}
                  styles={{body: {display: showOriginalPlan ? undefined : 'none'}}}
                >
                  {showOriginalPlan && <Table
                    dataSource={brokerData.plannedChanges}
                    rowKey="opId"
                    size="small"
                    pagination={false}
                    scroll={{ x: 620 }}
                    columns={[
                      { title: 'Op ID', dataIndex: 'opId', key: 'opId', render: (v: string) => <CopyableId id={v} maxLength={14} /> },
                      { title: 'Type', dataIndex: 'type', key: 'type', render: (v: string) => <Tag className="rounded-full text-xs">{v}</Tag> },
                      { title: 'Target', dataIndex: 'targetBroker', key: 'targetBroker' },
                      { title: 'Priority', dataIndex: 'priority', key: 'priority' },
                    ]}
                  />}
                </Card>
              )}

              <BrokerChangePartitions data={brokerData} blockedReasons={blockedReasons} />
              {!!brokerData.executionHistory?.length && (
                <Card className="shadow-sm" title={<span className="text-sm font-semibold">执行历史</span>}>
                  <p className="text-xs text-[hsl(var(--muted-foreground))] mb-3">
                    被替代的计划保留原始目标；当前恢复完成后，集群变更仍需满足期望拓扑。
                  </p>
                  <Table dataSource={brokerData.executionHistory} rowKey="changeId" size="small"
                    pagination={false} scroll={{x: 850}}
                    expandable={{expandedRowRender: row => <div className="text-xs space-y-2">
                      <p>原始计划：<CopyableId id={row.rootChangeId || row.changeId || '—'} maxLength={40} /></p>
                      {row.supersedes && <p>替代前序：<Button type="link" size="small" onClick={() =>
                        navigate(`/changes/clusters/${encodeURIComponent(decodedClusterId!)}/${encodeURIComponent(row.supersedes!)}`)}>
                        {row.supersedes}</Button></p>}
                      {row.supersededBy && <p>后继计划：<Button type="link" size="small" onClick={() =>
                        navigate(`/changes/clusters/${encodeURIComponent(decodedClusterId!)}/${encodeURIComponent(row.supersededBy!)}`)}>
                        {row.supersededBy}</Button></p>}
                      <p>权限提交索引：{row.authorityIndex || '0'}</p>
                      <p>等待开始：{row.blockedSince || '—'}</p>
                    </div>}}
                    columns={[
                      {title: '分区', dataIndex: 'partitionId', width: 65},
                      {title: '计划', dataIndex: 'changeId', render: (id: string) => <CopyableId id={id} maxLength={26} />},
                      {title: '代次', dataIndex: 'executionGeneration', width: 70},
                      {title: '状态', dataIndex: 'status', render: (status: string) => <StatusTag status={status} category="change" />},
                      {title: '进度', render: (_, row) => `${row.nextOperationIndex} / ${row.operationCount}`},
                      {title: '等待原因', dataIndex: 'blockedReason', render: (reason: string) => blockedReasons[reason] || reason || '—'},
                    ]} />
                </Card>
              )}
            </>
          )}
        </div>
      ) : !change.error ? (
        <Empty description="未找到变更" image={Empty.PRESENTED_IMAGE_SIMPLE}>
          <p className="text-xs text-[hsl(var(--muted-foreground))] mt-2">
            指定 changeId 可能不存在或已过保留期
          </p>
          <Button size="small" onClick={() => navigate(isPd ? '/changes?tab=pd'
            : `/changes?tab=broker&clusterId=${encodeURIComponent(decodedClusterId!)}`)}>返回变更追踪</Button>
        </Empty>
      ) : <p>记录未找到或读取失败，不能据此判断操作完成。</p>}

      {/* Cancel Confirm */}
      <ConfirmModal
        open={cancelOpen}
        title="取消变更"
        content={
          <div className="space-y-2 text-sm">
            <p>确认取消变更 <strong>{decodedChangeId}</strong>？</p>
            <p className="text-[hsl(var(--warning))] font-medium">取消不回滚已执行操作。</p>
            <p>{brokerData?.changeType === ChangeType.PARTITION
              ? '取消后将停止该计划尚未执行的操作；自动调度仍可能生成新的恢复计划。'
              : '原始配置变更将停止追踪；已进入安全替代的恢复链会继续对账和恢复可用性。'}</p>
          </div>
        }
        confirmLabel="确认取消"
        danger
        disabled={!canCancel}
        onConfirm={handleCancel}
        onCancel={() => setCancelOpen(false)}
      />
    </div>
  );
};
