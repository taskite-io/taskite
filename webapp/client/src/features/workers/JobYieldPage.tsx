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
import React, {useCallback} from 'react';
import {Alert, Button, Card, Descriptions, Empty, Input, Progress, Table, Tag, Tooltip} from 'antd';
import type {ColumnType} from 'antd/es/table';
import {useNavigate, useParams, useSearchParams} from 'react-router-dom';
import {Filter, RotateCcw} from 'lucide-react';
import {JobYieldReason, JobYieldStatus} from '@/shared/api/enums';
import type {Int64String, JobYield, JobYieldResponse} from '@/shared/api/types';
import {apiClient} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {CopyableId} from '@/shared/ui/CopyableId';
import {PageHeader, DetailSection, MetricStrip} from '@/shared/ui/DetailPrimitives';
import {RequestState} from '@/shared/ui/RequestState';
import {formatDateTime} from '@/shared/lib/format';

const reasons: Record<JobYieldReason, string> = {
  [JobYieldReason.WORKER_LEASE_EXPIRED]: '租约到期',
  [JobYieldReason.WORKER_DEREGISTERED]: '主动注销',
  [JobYieldReason.WORKER_SESSION_REPLACED]: '会话替换',
};
const statuses: Record<JobYieldStatus, {color: string; label: string}> = {
  [JobYieldStatus.PENDING]: {color: 'processing', label: '等待释放'},
  [JobYieldStatus.COMPLETED]: {color: 'success', label: '已完成'},
  [JobYieldStatus.RETIRED]: {color: 'default', label: '分区已退役'},
};
const YieldStatus: React.FC<{status: JobYieldStatus}> = ({status}) => {
  const info = statuses[status];
  return <Tag color={info?.color ?? 'default'}>{info?.label ?? status}</Tag>;
};
const timestamp = (value: Int64String) => {
  const millis = Number(value);
  return millis > 0 && Number.isFinite(millis) && millis <= 8640000000000000
    ? formatDateTime(new Date(millis).toISOString()) : '—';
};
const text = (value: string) => <Tooltip title={value} trigger={['hover', 'focus']}><span tabIndex={0} className="block truncate">{value || '—'}</span></Tooltip>;
const filterKeys = ['cluster', 'tenant', 'workerId', 'workerLeaseToken'] as const;

export const JobYieldListPage: React.FC = () => {
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();
  const query = new URLSearchParams();
  filterKeys.forEach(key => {const value = params.get(key); if (value) query.set(key, value);});
  const queryString = query.toString();
  const selectedStatuses = params.getAll('status').filter(value =>
    value === JobYieldStatus.PENDING || value === JobYieldStatus.COMPLETED);
  const navigationQuery = params.toString();
  const state = usePolling(useCallback(signal => apiClient.get<JobYieldResponse>(
    `/workers/yields${queryString ? `?${queryString}` : ''}`, signal), [queryString]),
    {interval: 5000, enabled: true, cacheKey: JSON.stringify(['job-yields', queryString])});
  const columnFilter = (key: typeof filterKeys[number], label: string): Pick<ColumnType<JobYield>, 'filteredValue' | 'filterIcon' | 'filterDropdown'> => {
    const apply = (value: string) => {
      const next = new URLSearchParams(params);
      if (value.trim()) next.set(key, value.trim());
      else next.delete(key);
      setParams(next);
    };
    return {
      filteredValue: params.get(key) ? [params.get(key)!] : null,
      filterIcon: filtered => <Filter size={13} aria-label={`按${label}筛选`} style={{color: filtered ? 'hsl(var(--primary))' : undefined}}/>,
      filterDropdown: ({selectedKeys, setSelectedKeys, confirm}) => {
        const value = String(selectedKeys[0] ?? '');
        const invalid = key === 'workerLeaseToken' && !!value.trim() && !/^[1-9]\d*$/.test(value.trim());
        const submit = () => {if (!invalid) {apply(value); confirm();}};
        return <div className="worker-column-filter" role="group" aria-label={`${label}筛选`}>
          <Input autoFocus aria-label={`按${label}筛选`} placeholder={`输入${label}精确查询`}
            value={value} status={invalid ? 'error' : undefined} aria-invalid={invalid}
            onChange={e => setSelectedKeys(e.target.value ? [e.target.value] : [])}
            onPressEnter={submit}/>
          {invalid && <div role="alert" className="text-xs text-[hsl(var(--destructive))]">请输入正整数 Token</div>}
          <div className="worker-column-filter-actions">
            <Button size="small" aria-label="重置" onClick={() => {setSelectedKeys([]); apply(''); confirm();}}>重置</Button>
            <Button size="small" type="primary" aria-label="筛选" onClick={submit}>筛选</Button>
          </div>
        </div>;
      },
    };
  };
  return <div className="space-y-6">
    <PageHeader title="任务释放记录" icon={<RotateCcw size={22}/>}
      subtitle="跟踪 Worker 会话结束后，任务恢复为可领取状态的进度与分区确认"
      onBack={() => navigate('/workers')}
      actions={<Button onClick={state.refresh}>刷新</Button>}/>
    <RequestState state={{...state, loading: false}}/>
    <MetricStrip items={[
      {label: '释放记录', value: state.data?.total ?? '—', tone: 'brand'},
      {label: '等待释放', value: state.data?.yields.filter(plan => plan.status === JobYieldStatus.PENDING).length ?? '—', tone: 'warning'},
      {label: '已完成', value: state.data?.yields.filter(plan => plan.status === JobYieldStatus.COMPLETED).length ?? '—', tone: 'success'},
    ]}/>
    <p className="text-xs text-[hsl(var(--muted-foreground))]">最多展示最近 200 条匹配记录，可在表头筛选。已释放任务可由其他 Worker 重新领取；Worker 离线保留期结束后，释放记录随注册记录一并删除。</p>
    <Card><Table<JobYield> rowKey="yieldId" size="small" tableLayout="fixed" scroll={{x: 1545}}
      loading={state.loading} dataSource={state.data?.yields ?? []}
      locale={{filterConfirm: '筛选', filterReset: '重置', emptyText: <Empty description="暂无匹配的任务释放记录" image={Empty.PRESENTED_IMAGE_SIMPLE}/>}}
      pagination={{pageSize: 20, showSizeChanger: true}}
      onChange={(_, filters) => {
        const values = (filters.status ?? []).map(String);
        if (values.length === selectedStatuses.length && values.every((value, index) => value === selectedStatuses[index])) return;
        const next = new URLSearchParams(params);
        next.delete('status');
        values.forEach(value => next.append('status', value));
        setParams(next);
      }}
      columns={[
        {title: 'Yield ID', dataIndex: 'yieldId', width: 165, render: (id: string) => <CopyableId id={id} to={`/workers/yields/${encodeURIComponent(id)}${navigationQuery ? `?${navigationQuery}` : ''}`}/>},
        {title: 'Worker', dataIndex: 'workerName', width: 140, render: text},
        {title: 'Worker ID', dataIndex: 'workerId', width: 160, ...columnFilter('workerId', 'Worker ID'), render: id => <CopyableId id={id} maxLength={16}/>},
        {title: 'Token', dataIndex: 'workerLeaseToken', width: 160, ...columnFilter('workerLeaseToken', 'Token'), render: (token: string) => <CopyableId id={token} maxLength={18}/>},
        {title: '集群', dataIndex: 'clusterId', width: 130, ...columnFilter('cluster', '集群'), render: text},
        {title: '租户', dataIndex: 'tenantId', width: 120, ...columnFilter('tenant', '租户'), render: text},
        {title: 'Job Type', dataIndex: 'jobType', width: 140, render: text},
        {title: '原因', dataIndex: 'reason', width: 90, render: reason => reasons[reason as JobYieldReason] ?? reason},
        {title: '状态', dataIndex: 'status', width: 100, filters: Object.entries(statuses).filter(([value]) => value !== JobYieldStatus.RETIRED).map(([value, info]) => ({text: info.label, value})),
          filteredValue: selectedStatuses.length ? selectedStatuses : null,
          filterIcon: filtered => <Filter size={13} aria-label="按状态筛选" style={{color: filtered ? 'hsl(var(--primary))' : undefined}}/>,
          onFilter: (value, row) => row.status === value, render: status => <YieldStatus status={status}/>},
        {title: '分区确认', width: 90, render: (_, row) => `${row.acknowledgedPartitions}/${row.totalPartitions}`},
        {title: '已释放', dataIndex: 'releasedJobCount', width: 90},
        {title: '退出时间', dataIndex: 'createdAtEpochMillis', width: 160, render: timestamp},
      ]}/></Card>
  </div>;
};

export const JobYieldDetailPage: React.FC = () => {
  const navigate = useNavigate();
  const {yieldId = ''} = useParams();
  const [params] = useSearchParams();
  const state = usePolling(useCallback(signal => apiClient.get<JobYield>(
    `/workers/yields/${encodeURIComponent(yieldId)}`, signal), [yieldId]),
    {interval: 3000, enabled: !!yieldId, cacheKey: JSON.stringify(['job-yield', yieldId]),
      isTerminal: plan => plan.status === JobYieldStatus.COMPLETED});
  const plan = state.data;
  return <div className="space-y-6">
    <PageHeader title="任务释放详情" icon={<RotateCcw size={22}/>}
      subtitle={<CopyableId id={yieldId}/>}
      onBack={() => navigate(`/workers/yields${params.size ? `?${params}` : ''}`)}
      badges={plan && <YieldStatus status={plan.status}/>} actions={<Button onClick={state.refresh}>刷新</Button>}/>
    <RequestState state={state}/>
    {plan && <>
      {plan.status === JobYieldStatus.PENDING && <Alert type="info" showIcon message="尚有分区未确认，旧会话的任务将按分区逐步释放"
        description="任务释放能力关闭或分区不可达时，记录会保留并重试。分区已确认后，旧会话无法再次领取或提交任务。"/>}
      <MetricStrip items={[
        {label: '目标分区', value: plan.totalPartitions, tone: 'brand'},
        {label: '已确认分区', value: plan.acknowledgedPartitions, tone: 'success'},
        {label: '已释放任务', value: plan.releasedJobCount},
      ]}/>
      <DetailSection title="Worker 会话快照" description="Worker 退出时的身份和消费信息，离线保留期结束后随注册记录一并删除">
        <Descriptions size="small" column={{xs: 1, md: 2, lg: 3}}>
          <Descriptions.Item label="名称">{text(plan.workerName)}</Descriptions.Item>
          <Descriptions.Item label="Worker ID"><CopyableId id={plan.workerId}/></Descriptions.Item>
          <Descriptions.Item label="Worker Lease Token"><CopyableId id={plan.workerLeaseToken} maxLength={24}/></Descriptions.Item>
          <Descriptions.Item label="集群">{text(plan.clusterId)}</Descriptions.Item>
          <Descriptions.Item label="租户">{text(plan.tenantId)}</Descriptions.Item>
          <Descriptions.Item label="Job Type">{text(plan.jobType)}</Descriptions.Item>
          <Descriptions.Item label="注册时间">{formatDateTime(plan.workerSnapshot.registeredAt)}</Descriptions.Item>
          <Descriptions.Item label="最后续约">{formatDateTime(plan.workerSnapshot.lastRenewal)}</Descriptions.Item>
          <Descriptions.Item label="退出原因">{reasons[plan.reason] ?? plan.reason}</Descriptions.Item>
          <Descriptions.Item label="退出时间">{timestamp(plan.createdAtEpochMillis)}</Descriptions.Item>
          <Descriptions.Item label="完成时间">{timestamp(plan.completedAtEpochMillis)}</Descriptions.Item>
          <Descriptions.Item label="版本">{text(plan.workerSnapshot.clientVersion)}</Descriptions.Item>
        </Descriptions>
      </DetailSection>
      <DetailSection title="目标分区确认" description="每个分区在会话封禁与任务释放完成后确认">
        <Progress percent={plan.totalPartitions ? Math.round(plan.acknowledgedPartitions * 100 / plan.totalPartitions) : 100} size="small"/>
        <Table rowKey="partitionId" size="small" tableLayout="fixed" scroll={{x: 1030}} dataSource={plan.partitions} pagination={{pageSize: 20}}
          columns={[
            {title: 'Partition', dataIndex: 'partitionId', width: 90},
            {title: '状态', dataIndex: 'status', width: 100, render: status => <YieldStatus status={status}/>},
            {title: '已释放', dataIndex: 'releasedJobCount', width: 90},
            {title: '尝试次数', dataIndex: 'attempts', width: 90},
            {title: '最后尝试', dataIndex: 'lastAttemptAtEpochMillis', width: 170, render: timestamp},
            {title: '下次重试', width: 170, render: (_, target) => target.status === JobYieldStatus.PENDING ? timestamp(target.nextAttemptAtEpochMillis) : '—'},
            {title: '确认时间', dataIndex: 'acknowledgedAtEpochMillis', width: 170, render: timestamp},
            {title: '等待原因 / 最近错误', width: 170, render: (_, target) => text(target.pauseReason || target.lastError)},
          ]}/>
      </DetailSection>
    </>}
  </div>;
};
