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
import React, {useState} from 'react';
import {Button, Card, Descriptions, Drawer, Empty, Input, Select, Space, Table, Tag, Tooltip} from 'antd';
import {useNavigate, useSearchParams} from 'react-router-dom';
import type {TimerSubscription, TimerSubscriptionListResponse} from '@/shared/api/types';
import {usePolling} from '@/shared/hooks/usePolling';
import {formatDateTimeUTC} from '@/shared/lib/format';
import {CopyableId} from '@/shared/ui/CopyableId';
import {MetricStrip} from '@/shared/ui/DetailPrimitives';
import {RequestState} from '@/shared/ui/RequestState';
import {SubscriptionText} from './SubscriptionText';
import {JsonEditor} from '@/shared/ui/JsonEditor';
import {DeleteSubscriptionModal} from './DeleteSubscriptionModal';

export function TimerSubscriptionList({state}: {
  state: ReturnType<typeof usePolling<TimerSubscriptionListResponse>>;
}) {
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<TimerSubscription | null>(null);
  const subscriptions = state.data?.subscriptions ?? [];
  const selected = subscriptions.find(item => item.timerSubscriptionKey === selectedKey);
  const filtered = subscriptions.filter(item =>
    (!params.get('name') || item.name.toLocaleLowerCase().includes(params.get('name')!.toLocaleLowerCase())) &&
    (!params.get('clusterId') || item.clusterId === params.get('clusterId')) &&
    (!params.get('tenantId') || item.tenantId === params.get('tenantId')) &&
    (!params.get('jobType') || item.jobTypes.includes(params.get('jobType')!)));
  const filter = (key: string, value?: string) => {
    const next = new URLSearchParams(params);
    if (value?.trim()) next.set(key, value.trim()); else next.delete(key);
    next.delete('page');
    setParams(next);
  };
  const options = (values: string[]) => [...new Set(values)].sort().map(value => ({value, label: value}));
  const pageSize = [10, 20, 50, 100].includes(Number(params.get('pageSize'))) ? Number(params.get('pageSize')) : 20;
  const requested = Number(params.get('page') || 1);
  const page = Number.isSafeInteger(requested) && requested > 0
    ? Math.min(requested, Math.max(1, Math.ceil(filtered.length / pageSize))) : 1;
  const types = (item: TimerSubscription) => <Space wrap size={4}>
    {item.jobTypes.map(type => <SubscriptionText key={type} value={type} tone="job" />)}
  </Space>;

  return <>
    <RequestState state={{...state, loading: false}} />
    <section className="resource-statistics" aria-label="定时任务统计">
      <MetricStrip items={[
        {label: '定时任务', value: state.data ? subscriptions.length : '—', tone: 'brand'},
        {label: '单次任务', value: state.data ? subscriptions.filter(item => item.type === 'DURATION').length : '—'},
        {label: '循环任务', value: state.data ? subscriptions.filter(item => item.type === 'CYCLE').length : '—', tone: 'info'},
        {label: 'Cron 任务', value: state.data ? subscriptions.filter(item => item.type === 'CRON').length : '—', tone: 'success'},
      ]} />
    </section>
    <Card className="shadow-sm" title={`定时任务 · ${filtered.length}`}>
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-3 mb-4" role="group" aria-label="定时任务筛选">
        <Input aria-label="按定时任务名称筛选" placeholder="按名称筛选" allowClear value={params.get('name') ?? ''}
          onChange={event => filter('name', event.target.value)} />
        <Select aria-label="按所属集群筛选" placeholder="所属集群" allowClear showSearch optionFilterProp="label"
          value={params.get('clusterId') ?? undefined} onChange={value => filter('clusterId', value)}
          options={options(subscriptions.map(item => item.clusterId))} />
        <Select aria-label="按所属租户筛选" placeholder="所属租户" allowClear showSearch optionFilterProp="label"
          value={params.get('tenantId') ?? undefined} onChange={value => filter('tenantId', value)}
          options={options(subscriptions.map(item => item.tenantId))} />
        <Select aria-label="按任务类型筛选" placeholder="任务类型" allowClear showSearch optionFilterProp="label"
          value={params.get('jobType') ?? undefined} onChange={value => filter('jobType', value)}
          options={options(subscriptions.flatMap(item => item.jobTypes))} />
      </div>
      <Table<TimerSubscription> className="resource-list-table" rowKey="timerSubscriptionKey" size="small"
        loading={state.loading} dataSource={filtered} scroll={{x: 1540}}
        locale={{emptyText: <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无符合条件的定时任务" />}}
        pagination={{current: page, pageSize, total: filtered.length, showSizeChanger: true,
          pageSizeOptions: [10, 20, 50, 100], showTotal: total => `共 ${total} 个定时任务`}}
        onChange={(pagination, _filters, _sorter, extra) => {
          if (extra.action !== 'paginate') return;
          const next = new URLSearchParams(params);
          next.set('page', String(pagination.pageSize === pageSize ? pagination.current ?? 1 : 1));
          next.set('pageSize', String(pagination.pageSize ?? pageSize));
          setParams(next);
        }}
        columns={[
          {title: '定时订阅 Key', dataIndex: 'timerSubscriptionKey', width: 205, fixed: 'left',
            render: key => <CopyableId id={key} maxLength={24} />},
          {title: '任务名称', dataIndex: 'name', width: 180, render: (name, item) =>
            <Button type="link" className="p-0 max-w-full" onClick={() => setSelectedKey(item.timerSubscriptionKey)}>
              <span className="truncate">{name}</span>
            </Button>},
          {title: '任务类型', width: 220, render: (_, item) => types(item)},
          {title: '所属租户', dataIndex: 'tenantId', width: 140,
            render: value => <SubscriptionText value={value} tone="tenant" />},
          {title: '所属集群', dataIndex: 'clusterId', width: 130, render: value =>
            <Button type="link" className="p-0" onClick={() => navigate(`/clusters/${encodeURIComponent(value)}`)}>{value}</Button>},
          {title: '定时类型', dataIndex: 'type', width: 100, render: value => <Tag>{value}</Tag>},
          {title: '定时表达式', dataIndex: 'expression', width: 220, ellipsis: {showTitle: false},
            render: value => <Tooltip title={value}><code>{value}</code></Tooltip>},
          {title: '下次计划执行（UTC）', dataIndex: 'dueDate', width: 210,
            render: value => formatDateTimeUTC(Number(value))},
          {title: '操作', width: 160, fixed: 'right', render: (_, item) => <Space size={4}>
            <Button type="link" onClick={() => setSelectedKey(item.timerSubscriptionKey)}>详情</Button>
            <Button type="link" danger disabled={state.stale || !!state.error} onClick={() => setDeleteTarget(item)}>删除</Button>
          </Space>},
        ]} />
    </Card>
    <Drawer className="console-drawer" width={640} open={!!selectedKey} title="定时任务详情" onClose={() => setSelectedKey(null)}>
      {selected ? <Descriptions column={1} size="small">
        <Descriptions.Item label="定时订阅 Key"><CopyableId id={selected.timerSubscriptionKey} maxLength={24} /></Descriptions.Item>
        <Descriptions.Item label="名称">{selected.name}</Descriptions.Item>
        <Descriptions.Item label="所属集群">{selected.clusterId}</Descriptions.Item>
        <Descriptions.Item label="所属租户">{selected.tenantId}</Descriptions.Item>
        <Descriptions.Item label="任务类型">{types(selected)}</Descriptions.Item>
        <Descriptions.Item label="定时类型">{selected.type}</Descriptions.Item>
        <Descriptions.Item label="定时表达式"><code className="break-all">{selected.expression}</code></Descriptions.Item>
        <Descriptions.Item label="调度时区">{selected.timeZone}</Descriptions.Item>
        <Descriptions.Item label="下次计划执行（UTC）">{formatDateTimeUTC(Number(selected.dueDate))}</Descriptions.Item>
        <Descriptions.Item label="当前执行序号">{selected.occurrence}</Descriptions.Item>
        <Descriptions.Item label="剩余计划执行次数">{selected.repetitions < 0 ? '无限循环' : selected.repetitions}</Descriptions.Item>
        <Descriptions.Item label="任务变量"><JsonEditor value={selected.variables} readOnly label="任务变量（JSON）" /></Descriptions.Item>
      </Descriptions> : <Empty description="该定时任务已执行完成或已删除" />}
    </Drawer>
    <DeleteSubscriptionModal target={deleteTarget ? {kind: 'timer', subscription: deleteTarget} : null}
      disabled={state.stale || !!state.error || !subscriptions.some(item => item.timerSubscriptionKey === deleteTarget?.timerSubscriptionKey)}
      onClose={() => setDeleteTarget(null)}
      onDeleted={() => {setDeleteTarget(null); setSelectedKey(null); state.refresh();}} />
  </>;
}
