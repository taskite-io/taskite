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
 * Worker 查看页面
 * 完整注册列表 + 全字段表格 + Drawer 详情
 */

import {WorkerStatus} from '@/shared/api/enums';
import React, { useCallback } from 'react';
import { Alert, Card, Table, Tag, Tooltip, Button, Drawer, Empty, Descriptions, Progress, Input } from 'antd';
import type {ColumnType} from 'antd/es/table';
import {Link, useNavigate, useSearchParams} from 'react-router-dom';
import { ArrowUpRight, Cpu, Filter } from 'lucide-react';
import { StatusTag } from '@/shared/ui/StatusTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import { PageHeader, DrawerTitle, DetailSection, MetricStrip } from '@/shared/ui/DetailPrimitives';
import { formatDateTime, formatDateTimeRelative, encodeWorkerKey } from '@/shared/lib/format';
import {apiClient} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState} from '@/shared/ui/RequestState';
import type { WorkerListResponse, WorkerRegistration } from '@/shared/api/types';

const JobTypeText: React.FC<{value: string; detail?: boolean}> = ({value, detail = false}) => (
  <Tooltip title={value} trigger={['hover', 'focus']} overlayClassName="worker-job-type-tooltip">
    <span tabIndex={0} className={`worker-job-type-text${detail ? ' worker-job-type-detail' : ''}`}>{value}</span>
  </Tooltip>
);

export const WorkerListPage: React.FC = () => {
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();
  const cluster = params.get('cluster') || '';
  const tenant = params.get('tenant') || '';
  const name = params.get('name') || '';
  const state = usePolling(useCallback((signal: AbortSignal) => {
    const query = new URLSearchParams();
    if (cluster) query.set('cluster', cluster);
    if (tenant) query.set('tenant', tenant);
    if (name) query.set('name', name);
    return apiClient.get<WorkerListResponse>(`/workers${query.size ? `?${query}` : ''}`, signal);
  }, [cluster, tenant, name]), {interval: 5000, enabled: true, cacheKey: JSON.stringify(['workers', cluster, tenant, name])});
  const workers = state.data;
  const columnFilter = (key: 'cluster' | 'tenant' | 'name', label: string): Pick<ColumnType<WorkerRegistration>, 'filteredValue' | 'filterIcon' | 'filterDropdown'> => {
    const apply = (value: string) => {
      const next = new URLSearchParams(params);
      if (value.trim()) next.set(key, value.trim());
      else next.delete(key);
      setParams(next);
    };
    return {
      filteredValue: params.get(key) ? [params.get(key)!] : null,
      filterIcon: filtered => <Filter size={13} aria-label={`按${label}筛选`} style={{color: filtered ? 'hsl(var(--primary))' : undefined}} />,
      filterDropdown: ({selectedKeys, setSelectedKeys, confirm}) => (
        <div className="worker-column-filter" role="group" aria-label={`${label}筛选`}>
          <Input autoFocus aria-label={`按${label}筛选`} placeholder={`输入${label}精确查询`}
            value={String(selectedKeys[0] ?? '')}
            onChange={e => setSelectedKeys(e.target.value ? [e.target.value] : [])}
            onPressEnter={() => {apply(String(selectedKeys[0] ?? '')); confirm();}} />
          <div className="worker-column-filter-actions">
            <Button size="small" aria-label="重置" onClick={() => {setSelectedKeys([]); apply(''); confirm();}}>重置</Button>
            <Button size="small" type="primary" aria-label="筛选" onClick={() => {apply(String(selectedKeys[0] ?? '')); confirm();}}>筛选</Button>
          </div>
        </div>
      ),
    };
  };
  const workerKey = params.get('worker');
  const selectedWorker = workers?.workers.find(w => encodeWorkerKey(w.clusterId, w.tenantId, w.name, w.workerId) === workerKey) ?? null;
  const setSelectedWorker = (worker: WorkerRegistration | null) => {
    const next = new URLSearchParams(params);
    if (worker) next.set('worker', encodeWorkerKey(worker.clusterId, worker.tenantId, worker.name, worker.workerId));
    else next.delete('worker');
    setParams(next);
  };

  return (
    <div className="space-y-6">
      {/* 页头 + 统计 */}
      <PageHeader title="Worker" icon={<Cpu size={21} />}
        subtitle={workers ? `注册记录快照 · ${formatDateTime(workers.generatedAt)}` : '注册记录与消费配置快照'}
        actions={<><Button onClick={() => navigate(`/workers/yields?${new URLSearchParams({...(cluster ? {cluster} : {}), ...(tenant ? {tenant} : {})})}`)}>任务释放记录</Button><Button aria-label="刷新" onClick={state.refresh}>刷新</Button></>} />
      <RequestState state={{...state, loading: false}} />
      <p className="text-xs text-[hsl(var(--muted-foreground))]">活动任务和消费配置来自最后一次心跳快照。</p>
      {workerKey && workers && !selectedWorker && <Alert type="info" message="该注册记录已不在当前查询结果中" />}
      <MetricStrip items={[
        { label: '注册总数', value: workers?.total ?? '—', tone: 'brand' },
        { label: '在线 Worker', value: workers?.online ?? '—', tone: 'success' },
        { label: '离线 Worker', value: workers?.offline ?? '—', tone: 'warning' },
        { label: '所属集群', value: workers ? new Set(workers.workers.map((worker) => worker.clusterId)).size : '—' },
      ]} />

      {/* Worker 表格 */}
      <Card className="shadow-sm">
          <Table
            dataSource={workers?.workers ?? []}
            loading={state.loading}
            locale={{emptyText: <Empty description={cluster || tenant || name ? '暂无符合筛选条件的 Worker，可在表头重置筛选' : '暂无 Worker 注册记录'} image={Empty.PRESENTED_IMAGE_SIMPLE} />}}
            rowKey={(r) => encodeWorkerKey(r.clusterId, r.tenantId, r.name, r.workerId)}
            size="small"
            tableLayout="fixed"
            pagination={{ pageSize: 20, showSizeChanger: true, showTotal: (t) => `共 ${t} 条` }}
            scroll={{ x: 1000 }}
            onRow={(record) => ({
              onClick: () => setSelectedWorker(record),
              className: 'cursor-pointer',
            })}
            columns={[
              { title: 'Name', dataIndex: 'name', key: 'name', width: 160,
                ...columnFilter('name', '名称'),
                render: (value: string, record: WorkerRegistration) => <Button type="link" size="small" className="p-0" onClick={(event) => { event.stopPropagation(); setSelectedWorker(record); }}>{value}</Button> },
              {
                title: 'Worker ID',
                dataIndex: 'workerId',
                key: 'workerId',
                width: 130,
                render: (v: string) => <CopyableId id={v} maxLength={14} />,
              },
              { title: 'Cluster', dataIndex: 'clusterId', key: 'clusterId', width: 120, ...columnFilter('cluster', '集群') },
              { title: 'Tenant', dataIndex: 'tenantId', key: 'tenantId', width: 90, ...columnFilter('tenant', '租户') },
              {
                title: 'Job Type',
                dataIndex: 'jobType',
                key: 'jobType',
                width: 160,
                render: (v: string) => <JobTypeText value={v} />,
              },
              {
                title: 'Status',
                dataIndex: 'status',
                key: 'status',
                width: 80,
                render: (v: string) => <StatusTag status={v} category="worker" />,
              },
              {
                title: '活动/容量',
                key: 'jobs',
                width: 90,
                render: (_, r: WorkerRegistration) => (
                  <span className="text-sm font-medium">
                    {r.activeJobs}
                    <span className="text-[hsl(var(--muted-foreground))] font-normal">/{r.maxJobsActive}</span>
                  </span>
                ),
              },
              { title: '并发', dataIndex: 'concurrency', key: 'concurrency', width: 60 },
              {
                title: '最后续约',
                dataIndex: 'lastRenewal',
                key: 'lastRenewal',
                width: 110,
                render: (v: string) => formatDateTimeRelative(v),
              },
              { title: '版本', dataIndex: 'clientVersion', key: 'clientVersion', width: 80 },
            ]}
          />
      </Card>

      {/* Worker Drawer */}
      <Drawer
        rootClassName="console-drawer"
        open={!!selectedWorker}
        onClose={() => setSelectedWorker(null)}
        title={<DrawerTitle title={selectedWorker?.name} eyebrow="WORKER / 注册详情" icon={<Cpu size={22} />} />}
        width="min(640px, 100vw)"
        footer="注册信息与消费配置来自最近一次上报"
      >
        {selectedWorker && (
          <div className="space-y-4">
            {selectedWorker.status === WorkerStatus.OFFLINE && (
              <Alert
                message="离线，数值来自最后一次上报的快照"
                type="warning"
                showIcon
              />
            )}
            <div className="object-summary">
              <CopyableId id={selectedWorker.workerId} maxLength={36} />
              <StatusTag status={selectedWorker.status} category="worker" />
            </div>
            <MetricStrip items={[
              { label: '活动任务', value: selectedWorker.activeJobs, tone: 'brand' },
              { label: '任务容量', value: selectedWorker.maxJobsActive },
              { label: '并发数', value: selectedWorker.concurrency },
            ]} />
            <DetailSection title="注册信息" description="Worker 的身份、所属集群与任务类型">
            <Descriptions size="small" column={1} className="worker-detail-fields worker-registration-details">
              <Descriptions.Item label="名称">{selectedWorker.name}</Descriptions.Item>
              <Descriptions.Item label="Worker ID">
                <CopyableId id={selectedWorker.workerId} maxLength={30} />
              </Descriptions.Item>
              <Descriptions.Item label="Worker Lease Token">
                <CopyableId id={selectedWorker.workerLeaseToken} maxLength={24} />
              </Descriptions.Item>
              {selectedWorker.status === WorkerStatus.OFFLINE && (
                <Descriptions.Item label="任务释放">
                  <Link className="worker-yield-link"
                    to={`/workers/yields?${new URLSearchParams({
                      cluster: selectedWorker.clusterId, tenant: selectedWorker.tenantId,
                      workerId: selectedWorker.workerId, workerLeaseToken: selectedWorker.workerLeaseToken,
                    })}`}>
                    <span>查看释放记录</span><ArrowUpRight size={13} aria-hidden="true" />
                  </Link>
                </Descriptions.Item>
              )}
              <Descriptions.Item label="所属集群">{selectedWorker.clusterId}</Descriptions.Item>
              <Descriptions.Item label="租户">{selectedWorker.tenantId}</Descriptions.Item>
              <Descriptions.Item label="Job Type">
                <JobTypeText value={selectedWorker.jobType} detail />
              </Descriptions.Item>
              <Descriptions.Item label="Client Version">
                <Tag className="rounded-full text-xs">{selectedWorker.clientVersion}</Tag>
              </Descriptions.Item>
            </Descriptions>
            </DetailSection>

            <DetailSection title="时间与续约">
            <Descriptions size="small" column={1} className="worker-detail-fields">
              <Descriptions.Item label="注册时间">{formatDateTime(selectedWorker.registeredAt)}</Descriptions.Item>
              <Descriptions.Item label="最后续约">{formatDateTime(selectedWorker.lastRenewal)}</Descriptions.Item>
              <Descriptions.Item label="租约到期">{Number(selectedWorker.leaseDeadlineEpochMillis) > 0 ? formatDateTime(new Date(Number(selectedWorker.leaseDeadlineEpochMillis)).toISOString()) : '—'}</Descriptions.Item>
              <Descriptions.Item label="状态变更时间">{formatDateTime(selectedWorker.statusChangedAt)}</Descriptions.Item>
              <Descriptions.Item label="续约超时">{selectedWorker.renewalTimeoutMillis.toLocaleString()} ms</Descriptions.Item>
            </Descriptions>
            </DetailSection>

            <DetailSection title="消费配置">
            <div className="mb-4">
              <div className="metric-label">容量使用率</div>
              <Progress percent={selectedWorker.maxJobsActive ? Math.round(selectedWorker.activeJobs / selectedWorker.maxJobsActive * 100) : 0} size="small" />
            </div>
            <Descriptions size="small" column={1} className="worker-detail-fields">
              <Descriptions.Item label="任务超时">{selectedWorker.timeoutMillis.toLocaleString()} ms</Descriptions.Item>
              <Descriptions.Item label="请求超时">{selectedWorker.requestTimeoutMillis.toLocaleString()} ms</Descriptions.Item>
              <Descriptions.Item label="轮询间隔">{selectedWorker.pollIntervalMillis.toLocaleString()} ms</Descriptions.Item>
            </Descriptions>
            </DetailSection>

            <DetailSection title="拉取变量" description="任务激活时需要读取的变量">
            <div>
              {selectedWorker.fetchVariables.length > 0
                ? selectedWorker.fetchVariables.map((v) => (
                  <Tag key={v} className="rounded-full text-xs mb-1" color="blue">{v}</Tag>
                ))
                : <span className="text-[hsl(var(--muted-foreground))] text-sm">未指定变量</span>}
            </div>
            </DetailSection>
          </div>
        )}
      </Drawer>
    </div>
  );
};
