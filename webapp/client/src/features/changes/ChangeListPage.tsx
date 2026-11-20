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
 * 变更追踪列表页面
 * 按类别展示变更计划，支持表头精确查询
 */

import React, {useCallback, useState} from 'react';
import {Card, Table, Input, Button, Empty, Tabs, Tag} from 'antd';
import type {ColumnType} from 'antd/es/table';
import {useNavigate, useSearchParams} from 'react-router-dom';
import {ListChecks, Filter, Clock, Trash2} from 'lucide-react';
import { StatusTag } from '@/shared/ui/StatusTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import { PageHeader } from '@/shared/ui/DetailPrimitives';
import { formatDateTimeRelative } from '@/shared/lib/format';
import {apiClient, ApiError, activeChange as fetchActiveChange} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState} from '@/shared/ui/RequestState';
import {useBootstrap} from '@/shared/ui/ConsoleBootstrap';
import {readKnownChanges, rememberChange, clearHistory, type KnownChangeRecord} from '@/shared/api/history';
import type { PDChangeDetail, BrokerChangeDetail, ClusterListResponse, JobSubscriptionListResponse, DistributionDetail } from '@/shared/api/types';
import {DistributionStatus} from '@/shared/api/enums';
import {PDChangeDetails} from './PDChangeDetails';

type ChangeTab = 'pd' | 'broker' | 'subscription';
interface ChangePlan {
  scope: KnownChangeRecord['scope'];
  clusterId?: string;
  changeId: string;
  detail: PDChangeDetail | BrokerChangeDetail | DistributionDetail;
}
const tabs = [
  {key: 'pd', label: 'PD 拓扑变更'},
  {key: 'broker', label: 'Broker 拓扑变更'},
  {key: 'subscription', label: '订阅创建/删除'},
];
const planKey = (record: Pick<KnownChangeRecord, 'scope' | 'clusterId' | 'changeId'>) =>
  JSON.stringify([record.scope, record.clusterId, record.changeId]);
const distributionPlan = (detail: DistributionDetail): ChangePlan => ({
  scope: 'subscription', clusterId: detail.sourceClusterId, changeId: detail.distributionKey, detail,
});

function PlanContents({detail}: {detail: ChangePlan['detail']}) {
  if ('sourceEpoch' in detail) return <PDChangeDetails data={detail} />;
  if ('currentTopology' in detail) return <Table className="resource-list-table" size="small"
    dataSource={detail.plannedChanges} rowKey="opId" pagination={false} scroll={{x: 680}}
    locale={{emptyText: '暂无原始操作计划'}}
    columns={[
      {title: '操作 ID', dataIndex: 'opId', render: id => <CopyableId id={id} maxLength={28} />},
      {title: '操作类型', dataIndex: 'type'},
      {title: '目标 Broker', dataIndex: 'targetBroker'},
      {title: '优先级', dataIndex: 'priority'},
    ]} />;
  return <Table className="resource-list-table" size="small" dataSource={detail.targets}
    rowKey="clusterId" pagination={false} scroll={{x: 680}}
    locale={{emptyText: '暂无目标分区'}}
    columns={[
      {title: '目标集群', dataIndex: 'clusterId'},
      {title: '期望分区数', dataIndex: 'expectedPartitions'},
      {title: '已确认分区', dataIndex: 'confirmedPartitionIds', render: (ids: number[]) => ids.join(', ') || '—'},
      {title: '待确认分区', dataIndex: 'pendingPartitions', render: (ids: number[]) => ids.join(', ') || '—'},
      {title: '状态', dataIndex: 'status', render: status => <StatusTag status={status} />},
    ]} />;
}

export const ChangeListPage: React.FC = () => {
  const navigate = useNavigate();
  const {deploymentId} = useBootstrap();
  const [params, setParams] = useSearchParams();
  const activeTab: ChangeTab = params.get('tab') === 'broker' ? 'broker'
    : params.get('tab') === 'subscription' ? 'subscription' : 'pd';
  const changeId = params.get('changeId') || '';
  const clusterId = activeTab === 'pd' ? '' : params.get('clusterId') || '';
  const [knownChanges, setKnownChanges] = useState<KnownChangeRecord[]>(() => readKnownChanges(deploymentId));

  const state = usePolling(useCallback(async (signal: AbortSignal): Promise<ChangePlan[]> => {
    if (activeTab === 'pd') {
      const detail = changeId
        ? await apiClient.get<PDChangeDetail>(`/pd/changes/${encodeURIComponent(changeId)}`, signal)
        : await fetchActiveChange<PDChangeDetail>('/pd/changes/active', signal);
      return detail ? [{scope: 'pd', changeId: detail.changeId, detail}] : [];
    }
    if (activeTab === 'broker') {
      const clusters = clusterId ? [clusterId] : (await apiClient.get<ClusterListResponse>(
        '/broker/clusters', signal)).clusters.map(cluster => cluster.clusterId);
      const plans = await Promise.all(clusters.map(async (id): Promise<ChangePlan | null> => {
        const path = `/broker/clusters/${encodeURIComponent(id)}/changes`;
        let detail: BrokerChangeDetail | null;
        if (changeId) {
          try {
            detail = await apiClient.get<BrokerChangeDetail>(`${path}/${encodeURIComponent(changeId)}`, signal);
          } catch (error) {
            if (error instanceof ApiError && error.status === 404 && error.code === 'CHANGE_NOT_FOUND') return null;
            throw error;
          }
        } else {
          detail = await fetchActiveChange<BrokerChangeDetail>(`${path}/active`, signal);
        }
        return detail ? {scope: 'cluster', clusterId: id, changeId: detail.changeId, detail} : null;
      }));
      return plans.filter((plan): plan is ChangePlan => plan !== null);
    }
    if (changeId) {
      const detail = await apiClient.get<DistributionDetail>(
        `/broker/job-subscriptions/${encodeURIComponent(changeId)}`, signal);
      return !clusterId || detail.sourceClusterId === clusterId ? [distributionPlan(detail)] : [];
    }
    const response = await apiClient.get<JobSubscriptionListResponse>('/broker/job-subscriptions', signal);
    const creations = new Map<string, DistributionDetail>();
    const deletionKeys = new Set<string>();
    for (const resource of response.subscriptions) {
      if (clusterId && resource.sourceClusterId !== clusterId) continue;
      if ([DistributionStatus.DISTRIBUTING, DistributionStatus.COMPLETED].includes(resource.status)) {
        const previous = creations.get(resource.distributionKey);
        creations.set(resource.distributionKey, {...(previous ?? resource),
          status: previous?.status === DistributionStatus.DISTRIBUTING ? previous.status : resource.status,
          targets: [...new Map(
          [...(previous?.targets ?? []), ...resource.targets].map(target => [target.clusterId, target])).values()]});
      }
      if (resource.status === DistributionStatus.DELETING && resource.deletionDistributionKey) {
        deletionKeys.add(resource.deletionDistributionKey);
      }
    }
    const deletions = await Promise.all([...deletionKeys].map(key => apiClient.get<DistributionDetail>(
      `/broker/job-subscriptions/${encodeURIComponent(key)}`, signal)));
    return [...[...creations.values()].filter(detail => detail.status === DistributionStatus.DISTRIBUTING),
      ...deletions].map(distributionPlan);
  }, [activeTab, changeId, clusterId]), {
    interval: 5000, enabled: true, cacheKey: JSON.stringify(['changes', activeTab, changeId, clusterId]),
  });
  const plans = state.data ?? [];
  const recordScope = activeTab === 'broker' ? 'cluster' : activeTab;
  const history = knownChanges.filter(record => record.scope === recordScope &&
    (!clusterId || record.clusterId === clusterId) && (!changeId || record.changeId === changeId));
  const changePath = (record: Pick<KnownChangeRecord, 'scope' | 'clusterId' | 'changeId'>) =>
    record.scope === 'subscription' ? `/changes/subscriptions/${encodeURIComponent(record.changeId)}`
      : record.scope === 'pd' ? `/changes/pd/${encodeURIComponent(record.changeId)}`
        : `/changes/clusters/${encodeURIComponent(record.clusterId!)}/${encodeURIComponent(record.changeId)}`;
  const openPlan = (record: ChangePlan) => {
    rememberChange(deploymentId, {scope: record.scope, clusterId: record.clusterId, changeId: record.changeId});
    setKnownChanges(readKnownChanges(deploymentId));
    navigate(changePath(record));
  };
  const columnFilter = (key: 'changeId' | 'clusterId', label: string): Pick<ColumnType<ChangePlan>,
    'filteredValue' | 'filterIcon' | 'filterDropdown'> => ({
    filteredValue: params.get(key) ? [params.get(key)!] : null,
    filterIcon: active => <Filter size={13} aria-label={`按${label}筛选`}
      style={{color: active ? 'hsl(var(--primary))' : undefined}} />,
    filterDropdown: ({selectedKeys, setSelectedKeys, confirm}) => {
      const apply = (value: string) => {
        const next = new URLSearchParams(params);
        if (value.trim()) next.set(key, value.trim()); else next.delete(key);
        setParams(next);
        confirm();
      };
      return <div className="worker-column-filter" role="group" aria-label={`${label}筛选`}>
        <Input autoFocus aria-label={`按${label}筛选`} placeholder={`输入${label}精确查询`}
          value={String(selectedKeys[0] ?? '')}
          onChange={event => setSelectedKeys(event.target.value ? [event.target.value] : [])}
          onPressEnter={() => apply(String(selectedKeys[0] ?? ''))} />
        <div className="worker-column-filter-actions">
          <Button size="small" aria-label="重置" onClick={() => {setSelectedKeys([]); apply('');}}>重置</Button>
          <Button size="small" type="primary" aria-label="筛选" onClick={() => apply(String(selectedKeys[0] ?? ''))}>筛选</Button>
        </div>
      </div>;
    },
  });
  const planSummary = ({detail}: ChangePlan) => {
    if ('sourceEpoch' in detail) return <div className="space-y-1">
      <div>配置版本 {detail.sourceEpoch} → {detail.targetEpoch} · 目标成员 {detail.targetCount}</div>
      <div>目标 Voters: {detail.targetVoters.join(', ') || '无'}</div>
      <div className="text-xs text-[hsl(var(--muted-foreground))]">
        原始计划 {detail.plannedChanges.length} 项操作 · 待执行 {detail.pendingChanges.length} 项操作
      </div>
    </div>;
    if ('currentTopology' in detail) return <div className="space-y-1">
      <div>分区 {detail.currentTopology.length} → {detail.expectedTopology.length}</div>
      <div className="text-xs text-[hsl(var(--muted-foreground))]">
        原始计划 {detail.plannedChanges.length} 项操作 · 待执行 {detail.pendingChanges.length} 个分区
      </div>
    </div>;
    return <div className="space-y-1">
      <div>{detail.name || detail.jobType} · {detail.tenantId}</div>
      <div className="text-xs text-[hsl(var(--muted-foreground))]">
        {detail.targets.map(target =>
          `${target.clusterId}: ${target.acknowledgedPartitions} / ${target.expectedPartitions} 分区确认`).join(' · ') || '暂无目标分区'}
      </div>
    </div>;
  };
  const handleClearHistory = () => {
    clearHistory(deploymentId);
    setKnownChanges([]);
  };
  const hasFilters = !!changeId || !!clusterId;

  return (
    <div className="resource-list-page space-y-6">
      <PageHeader title="变更追踪" icon={<ListChecks size={22} />}
        subtitle="按类别查看变更计划、执行进度与访问记录"
        actions={<Button aria-label="刷新" onClick={state.refresh}>刷新</Button>} />
      <Tabs activeKey={activeTab} items={tabs} onChange={key => {
        const next = new URLSearchParams(params);
        next.set('tab', key);
        next.delete('changeId');
        next.delete('clusterId');
        setParams(next);
      }} />
      <RequestState state={{...state, loading: false}} />
      <Card className="shadow-sm" title={changeId ? '查询结果' : '当前变更'}
        extra={hasFilters && <Button size="small" onClick={() => {
          const next = new URLSearchParams(params);
          next.delete('changeId');
          next.delete('clusterId');
          setParams(next);
        }}>清除筛选</Button>}>
        <Table<ChangePlan> key={JSON.stringify([activeTab, changeId, clusterId])}
          className="resource-list-table" size="small" pagination={false}
          loading={state.loading} dataSource={plans} rowKey={planKey} scroll={{x: 920}}
          expandable={{expandedRowRender: record => <PlanContents detail={record.detail} />,
            columnTitle: '计划', columnWidth: 54}}
          locale={{emptyText: <Empty description={hasFilters ? '暂无符合条件的变更计划' : '暂无当前变更'}
            image={Empty.PRESENTED_IMAGE_SIMPLE} />}}
          columns={[
            {title: activeTab === 'subscription' ? '分发 Key' : '变更 ID', dataIndex: 'changeId', width: 220,
              ...columnFilter('changeId', activeTab === 'subscription' ? '分发 Key' : '变更 ID'),
              render: (id: string, record) => <CopyableId id={id} maxLength={24} to={changePath(record)} />},
            ...(activeTab !== 'pd' ? [{title: activeTab === 'subscription' ? '源集群' : '所属集群',
              dataIndex: 'clusterId', width: 130, ...columnFilter('clusterId', '集群 ID')}] : []),
            ...(activeTab === 'subscription' ? [{title: '变更类型', key: 'type', width: 100,
              render: (_: unknown, record: ChangePlan) => <Tag className="resource-pill">
                {'valueType' in record.detail && record.detail.valueType === 'JOB_SUBSCRIPTION_DELETION' ? '删除' : '创建'}
              </Tag>}] : []),
            {title: '变更计划', key: 'plan', render: (_, record) => planSummary(record)},
            {title: '状态', key: 'status', width: 120, render: (_, record) =>
              <StatusTag status={record.detail.status} category={record.scope === 'subscription' ? undefined : 'change'} />},
            {title: '操作', key: 'actions', width: 100, render: (_, record) =>
              <Button type="link" size="small" onClick={() => openPlan(record)}>查看详情</Button>},
          ]} />
      </Card>

      {/* 访问记录 */}
      {history.length > 0 && (
        <Card
          className="shadow-sm"
          title={
            <div className="flex items-center justify-between w-full">
              <div className="flex items-center gap-2">
                <Clock size={16} className="text-[hsl(var(--primary))]" />
                <span className="text-sm font-semibold">访问记录 ({history.length})</span>
              </div>
              <Button size="small" icon={<Trash2 size={12} />} onClick={handleClearHistory} danger>
                清空全部记录
              </Button>
            </div>
          }
        >
          <Table
            dataSource={history}
            rowKey={planKey}
            size="small"
            pagination={false}
            scroll={{x: 620}}
            onRow={(record) => ({
              onClick: () => navigate(changePath(record)),
              className: 'cursor-pointer',
            })}
            columns={[
              {
                title: '变更 ID / Key',
                dataIndex: 'changeId',
                key: 'changeId',
                render: (v: string) => <CopyableId id={v} maxLength={20} />,
              },
              {
                title: '范围',
                key: 'scope',
                width: 120,
                render: (_, r) => (
                  <Tag className="rounded-full" color={r.scope === 'pd' ? 'purple' : r.scope === 'subscription' ? 'orange' : 'blue'}>
                    {r.scope === 'pd' ? 'PD' : r.scope === 'subscription' ? `订阅 · ${r.clusterId}` : r.clusterId}
                  </Tag>
                ),
              },
              {
                title: '首次',
                dataIndex: 'firstSeenAt',
                key: 'firstSeenAt',
                width: 120,
                render: (v: string) => formatDateTimeRelative(v),
              },
              {
                title: '最后',
                dataIndex: 'lastSeenAt',
                key: 'lastSeenAt',
                width: 120,
                render: (v: string) => formatDateTimeRelative(v),
              },
            ]}
          />
        </Card>
      )}
    </div>
  );
};
