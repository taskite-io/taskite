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
import {DistributionStatus, JobSubscriptionCreationType} from '@/shared/api/enums';
import React, {useCallback, useState} from 'react';
import {Button, Card, Empty, Input, Progress, Space, Table, Tabs, Tag, Tooltip} from 'antd';
import type {ColumnType} from 'antd/es/table';
import {useNavigate, useSearchParams} from 'react-router-dom';
import {FileCode2, Filter, Plus} from 'lucide-react';
import {apiClient} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {PageHeader, MetricStrip} from '@/shared/ui/DetailPrimitives';
import {CopyableId} from '@/shared/ui/CopyableId';
import {RequestState} from '@/shared/ui/RequestState';
import {StatusTag} from '@/shared/ui/StatusTag';
import {ClusterCountBreakdown} from '@/shared/ui/ClusterCountBreakdown';
import type {DistributionDetail, JobSubscriptionListResponse, TimerSubscriptionListResponse} from '@/shared/api/types';
import {SubscriptionText} from './SubscriptionText';
import {CreateSubscriptionModal} from './CreateSubscriptionModal';
import {TimerSubscriptionList} from './TimerSubscriptionList';
import {DeleteSubscriptionModal} from './DeleteSubscriptionModal';

type FilterKey = 'jobType' | 'name' | 'tenantId' | 'clusterId';
type SubscriptionCounts = {total: number; native: number; mirror: number; completed: number};
const emptyCounts = (): SubscriptionCounts => ({total: 0, native: 0, mirror: 0, completed: 0});

export const JobSubscriptionListPage: React.FC = () => {
  const navigate = useNavigate();
  const [createKind, setCreateKind] = useState<'job' | 'timer' | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DistributionDetail | null>(null);
  const [params, setParams] = useSearchParams();
  const activeTab = params.get('tab') === 'timer' ? 'timer' : 'job';
  const state = usePolling(useCallback((signal: AbortSignal) =>
    apiClient.get<JobSubscriptionListResponse>('/broker/job-subscriptions', signal), []),
    {interval: 5000, enabled: activeTab === 'job', cacheKey: 'job-subscriptions'});
  const timerState = usePolling(useCallback((signal: AbortSignal) =>
    apiClient.get<TimerSubscriptionListResponse>('/broker/timer-subscriptions', signal), []),
    {interval: 5000, enabled: activeTab === 'timer', cacheKey: 'timer-subscriptions'});
  const resources = state.data?.subscriptions ?? [];
  const totals = emptyCounts();
  const countsByCluster = new Map<string, SubscriptionCounts>();
  for (const resource of resources) {
    let counts = countsByCluster.get(resource.clusterId);
    if (!counts) {
      counts = emptyCounts();
      countsByCluster.set(resource.clusterId, counts);
    }
    if (resource.status === DistributionStatus.DELETED) continue;
    const keys: (keyof SubscriptionCounts)[] = ['total'];
    if (resource.creationType === JobSubscriptionCreationType.SUBSCRIPTION) keys.push('native');
    if (resource.creationType === JobSubscriptionCreationType.MIRROR) keys.push('mirror');
    if (resource.status === DistributionStatus.COMPLETED) keys.push('completed');
    for (const key of keys) {
      counts[key]++;
      totals[key]++;
    }
  }
  const clusters = [...countsByCluster.keys()].sort();
  const clusterBreakdown = (key: keyof SubscriptionCounts, tone: 'brand' | 'info' | 'success' = 'brand') =>
    clusters.length > 0 ? <ClusterCountBreakdown tone={tone}
      items={clusters.map(clusterId => ({clusterId, value: countsByCluster.get(clusterId)![key]}))} /> : undefined;
  const canDelete = (resource: DistributionDetail) => resource.creationType === JobSubscriptionCreationType.SUBSCRIPTION &&
    [DistributionStatus.COMPLETED, DistributionStatus.DISTRIBUTING].includes(resource.status) && !state.stale && !state.error;
  const latestDeleteTarget = resources.find(resource => resource.distributionKey === deleteTarget?.distributionKey &&
    resource.clusterId === deleteTarget.clusterId);
  const filtered = resources.filter(resource => resource.status !== DistributionStatus.DELETED &&
    (!params.get('jobType') || resource.jobType === params.get('jobType')!) &&
    (!params.get('name') || resource.name.toLocaleLowerCase().includes(params.get('name')!.toLocaleLowerCase())) &&
    (!params.get('tenantId') || resource.tenantId === params.get('tenantId')) &&
    (!params.get('clusterId') || resource.clusterId === params.get('clusterId')) &&
    (!params.get('sourceClusterId') || resource.sourceClusterId === params.get('sourceClusterId')) &&
    (!params.get('creationType') || resource.creationType === params.get('creationType')));
  const columnFilter = (key: FilterKey, label: string): Pick<ColumnType<DistributionDetail>, 'filteredValue' | 'filterIcon' | 'filterDropdown'> => ({
    filteredValue: params.get(key) ? [params.get(key)!] : null,
    filterIcon: active => <Filter size={13} aria-label={`按${label}筛选`}
      style={{color: active ? 'hsl(var(--primary))' : undefined}} />,
    filterDropdown: ({selectedKeys, setSelectedKeys, confirm}) => {
      const apply = (value: string) => {
        const next = new URLSearchParams(params);
        if (value.trim()) next.set(key, value.trim()); else next.delete(key);
        next.delete('page');
        setParams(next);
        confirm();
      };
      return <div className="worker-column-filter" role="group" aria-label={`${label}筛选`}>
        <Input autoFocus aria-label={`按${label}筛选`}
          placeholder={`输入${label}${key === 'name' ? '关键词' : '精确查询'}`}
          value={String(selectedKeys[0] ?? '')}
          onChange={e => setSelectedKeys(e.target.value ? [e.target.value] : [])}
          onPressEnter={() => apply(String(selectedKeys[0] ?? ''))} />
        <div className="worker-column-filter-actions">
          <Button size="small" aria-label="重置" onClick={() => {setSelectedKeys([]); apply('');}}>重置</Button>
          <Button size="small" type="primary" aria-label="筛选" onClick={() => apply(String(selectedKeys[0] ?? ''))}>筛选</Button>
        </div>
      </div>;
    },
  });
  const resourcePath = (resource: DistributionDetail) => {
    const next = new URLSearchParams(params);
    next.set('assetClusterId', resource.clusterId);
    return `/subscriptions/distributions/${encodeURIComponent(resource.distributionKey)}?${next}`;
  };
  const openResource = (resource: DistributionDetail) => navigate(resourcePath(resource));
  const clusterLink = (id: string) => <Button type="link" className="p-0 max-w-full"
    onClick={() => navigate(`/clusters/${encodeURIComponent(id)}`)}><span className="truncate">{id}</span></Button>;
  const pageSize = [10, 20, 50, 100].includes(Number(params.get('pageSize'))) ? Number(params.get('pageSize')) : 20;
  const requestedPage = Number(params.get('page') || 1);
  const page = Number.isSafeInteger(requestedPage) && requestedPage > 0
    ? Math.min(requestedPage, Math.max(1, Math.ceil(filtered.length / pageSize))) : 1;
  const hasFilters = ['jobType', 'name', 'tenantId', 'clusterId', 'sourceClusterId', 'creationType'].some(key => params.has(key));

  return <div className="resource-list-page space-y-6">
    <PageHeader title="任务订阅" icon={<FileCode2 size={22} />}
      subtitle={activeTab === 'timer' ? '管理定时任务，查看调度计划与关联任务类型' : '管理普通任务订阅，查看所属集群与分区分发进度'}
      actions={<Space wrap>
        <Button aria-label="刷新" onClick={activeTab === 'timer' ? timerState.refresh : state.refresh}>刷新</Button>
        <Button type="primary" icon={<Plus size={14} />} onClick={() => setCreateKind(activeTab)}>
          {activeTab === 'timer' ? '创建定时任务' : '创建普通任务'}
        </Button>
      </Space>} />
    <Tabs activeKey={activeTab} items={[{key: 'job', label: '普通任务'}, {key: 'timer', label: '定时任务'}]}
      onChange={key => {
        const next = new URLSearchParams(params);
        if (key === 'timer') next.set('tab', 'timer'); else next.delete('tab');
        for (const field of ['page', 'name', 'jobType', 'sourceClusterId', 'creationType']) next.delete(field);
        setParams(next);
      }} />
    {activeTab === 'timer' ? <TimerSubscriptionList state={timerState} /> : <>
    <RequestState state={{...state, loading: false}} />
    <section className="resource-statistics" aria-label="任务订阅统计">
      <MetricStrip items={[
        {label: '任务订阅', value: state.data ? totals.total : '—', tone: 'brand', hint: clusterBreakdown('total')},
        {label: '原生订阅', value: state.data ? totals.native : '—', tone: 'brand', hint: clusterBreakdown('native')},
        {label: 'Mirror 订阅', value: state.data ? totals.mirror : '—', tone: 'info', hint: clusterBreakdown('mirror', 'info')},
        {label: '分发完成', value: state.data ? totals.completed : '—', tone: 'success', hint: clusterBreakdown('completed', 'success')},
      ]} />
    </section>
    <Card className="shadow-sm" title={`任务订阅 · ${filtered.length}`}
      extra={hasFilters && <Button size="small" onClick={() => setParams({})}>清除筛选</Button>}>
      <Table<DistributionDetail> className="resource-list-table" rowKey={r => JSON.stringify([r.clusterId, r.distributionKey])}
        dataSource={filtered} loading={state.loading} size="small" scroll={{x: 1570}}
        locale={{emptyText: <Empty description="暂无符合条件的任务订阅" image={Empty.PRESENTED_IMAGE_SIMPLE} />}}
        pagination={{current: page, pageSize, total: filtered.length, showSizeChanger: true,
          pageSizeOptions: [10, 20, 50, 100], showTotal: total => `共 ${total} 个订阅`}}
        onChange={(pagination, _filters, _sorter, extra) => {
          if (extra.action !== 'paginate') return;
          const next = new URLSearchParams(params);
          next.set('page', String(pagination.pageSize === pageSize ? pagination.current ?? 1 : 1));
          next.set('pageSize', String(pagination.pageSize ?? pageSize));
          setParams(next);
        }}
        columns={[
          {title: '分发 Key', dataIndex: 'distributionKey', width: 205, fixed: 'left',
            render: (key, resource) => <CopyableId id={key} maxLength={24} to={resourcePath(resource)} />},
          {title: '任务订阅名称', dataIndex: 'name', width: 190, ...columnFilter('name', '任务订阅名称'),
            render: (name, resource) => <Button type="link" className="p-0 h-auto max-w-full" title={name}
              onClick={() => openResource(resource)}><span className="truncate">{name || '—'}</span></Button>},
          {title: '任务类型', dataIndex: 'jobType', width: 220, ...columnFilter('jobType', '任务类型'),
            render: type => <SubscriptionText value={type} tone="job" />},
          {title: '所属租户', dataIndex: 'tenantId', width: 140, ...columnFilter('tenantId', '所属租户'),
            render: tenant => <SubscriptionText value={tenant} tone="tenant" />},
          {title: '所属集群', dataIndex: 'clusterId', width: 130, ...columnFilter('clusterId', '所属集群'), render: clusterLink},
          {title: '订阅来源', dataIndex: 'creationType', width: 100, render: type => <Tag className="resource-pill">{type === JobSubscriptionCreationType.SUBSCRIPTION ? '创建' : type === JobSubscriptionCreationType.MIRROR ? 'Mirror' : type}</Tag>},
          {title: '源集群', dataIndex: 'sourceClusterId', width: 130, render: clusterLink},
          {title: '状态', dataIndex: 'status', width: 90, render: status => <StatusTag status={status} />},
          {title: '分区确认', key: 'progress', width: 205, className: 'resource-confirmation-cell', render: (_, resource) => {
            if (resource.status === DistributionStatus.DELETING && resource.deletionDistributionKey) {
              return <Button type="link" size="small" onClick={() => navigate(
                `/changes/subscriptions/${encodeURIComponent(resource.deletionDistributionKey!)}`)}>查看删除进度</Button>;
            }
            const expected = resource.targets.reduce((sum, t) => sum + t.expectedPartitions, 0);
            const ack = resource.targets.reduce((sum, t) => sum + t.acknowledgedPartitions, 0);
            return <div><div className="flex justify-between text-xs"><span>{resource.clusterId}</span><span>{ack} / {expected}</span></div>
              <Progress size="small" percent={expected ? Math.min(100, Math.round(ack / expected * 100)) : 0} showInfo={false}
                trailColor="hsl(var(--border))"
                strokeColor={expected > 0 && ack === expected ? 'hsl(var(--success))' : 'hsl(var(--primary))'} /></div>;
          }},
          {title: '操作', key: 'actions', width: 160, fixed: 'right', render: (_, resource) => <Space size={4}>
            <Button type="link" onClick={() => openResource(resource)}>详情</Button>
            <Tooltip title={resource.creationType === JobSubscriptionCreationType.MIRROR ? 'Mirror 订阅请在源集群删除' : undefined}>
              <Button type="link" danger disabled={!canDelete(resource)} onClick={() => setDeleteTarget(resource)}>删除</Button>
            </Tooltip>
          </Space>},
        ]} />
    </Card>
    </>}
    <DeleteSubscriptionModal target={deleteTarget ? {kind: 'job', subscription: deleteTarget} : null}
      disabled={!latestDeleteTarget || !canDelete(latestDeleteTarget)}
      onClose={() => setDeleteTarget(null)} onDeleted={() => {setDeleteTarget(null); state.refresh();}} />
    {createKind && <CreateSubscriptionModal kind={createKind}
      initialClusterId={params.get('clusterId') ?? undefined}
      initialTenantId={params.get('tenantId') ?? undefined}
      onClose={() => {setCreateKind(null); (activeTab === 'timer' ? timerState : state).refresh();}} />}
  </div>;
};
