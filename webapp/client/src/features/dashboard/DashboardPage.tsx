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
 * Dashboard 总览页
 * 三块摘要卡片：PD 节点健康、Broker 集群状态、Worker 注册快照
 */

import React, { useCallback } from 'react';
import { Card, Row, Col, Statistic, Table, Empty, Button, Tag, Tooltip, Progress, Badge } from 'antd';
import {
  Server,
  Boxes,
  Cpu,
  HardDrive,
  GitBranch,
  ArrowRight,
  Activity,
  Wifi,
  CheckCircle2,
  AlertCircle,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { CopyableId } from '@/shared/ui/CopyableId';
import {NetworkEndpoint} from '@/shared/ui/NetworkEndpoint';
import {formatEndpoint} from '@/shared/lib/format';
import {RaftRoleTag} from '@/shared/ui/RaftRoleTag';
import { PageHeader } from '@/shared/ui/DetailPrimitives';
import {ClusterCountBreakdown} from '@/shared/ui/ClusterCountBreakdown';
import {apiClient} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState} from '@/shared/ui/RequestState';
import type { PDTopology, PDMember, PDHealth, ClusterListResponse, WorkerListResponse, ClusterSummary } from '@/shared/api/types';

async function readDashboardData(signal: AbortSignal) {
  const health = await apiClient.get<PDHealth>('/pd/health', signal);
  const pdMembers = await apiClient.get<PDTopology>('/pd/topology', signal);
  const clusters = await apiClient.get<ClusterListResponse>('/broker/clusters', signal);
  const workers = await apiClient.get<WorkerListResponse>('/workers', signal);
  return {
    health, pdMembers, clusters, workers,
  };
}

export const DashboardPage: React.FC = () => {
  const navigate = useNavigate();
  const state = usePolling(useCallback(readDashboardData, []), {interval: 5000, enabled: true, cacheKey: 'dashboard'});
  const data = state.data;
  if (!data) return <RequestState state={state} />;

  const totalBrokers = data.clusters.clusters.reduce((sum, c) => sum + c.registeredBrokerCount, 0);
  const onlineBrokers = data.clusters.clusters.reduce((sum, c) => sum + c.onlineBrokerCount, 0);
  const workerOnlinePercent = data.workers.total > 0
    ? Math.round((data.workers.online / data.workers.total) * 100)
    : 0;

  return (
    <div className="space-y-6">
      <RequestState state={state} />
      {/* 页头 */}
      <PageHeader title="集群总览" icon={<Activity size={22} />}
        subtitle="控制平面、Broker 集群与 Worker 的运行快照" />

      {/* === 第一行：三个核心统计卡片 === */}
      <Row gutter={[16, 16]} className="dashboard-summary-row">
        {/* PD 健康卡片 */}
        <Col xs={24} sm={12} lg={8}>
          <Card
            className="cursor-pointer hover:shadow-md transition-all duration-200 border-l-4 border-l-[hsl(var(--primary))]"
            onClick={() => navigate('/pd')}
          >
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-[hsl(var(--primary-muted))] flex items-center justify-center">
                  <Server size={20} className="text-[hsl(var(--primary))]" />
                </div>
                <div>
                  <div className="text-sm font-semibold text-[hsl(var(--foreground))]">PD 节点</div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
                    {data.health.nodeId} · {data.health.role}
                  </div>
                </div>
              </div>
              <ArrowRight size={16} className="text-[hsl(var(--muted-foreground))] mt-1" />
            </div>

            <Row gutter={16} className="mt-4 pt-4 border-t border-[hsl(var(--border))]">
              <Col span={8}>
                <Statistic
                  title="成员数"
                  value={data.pdMembers.members.length}
                  valueStyle={{ fontSize: 22, fontWeight: 600, color: 'hsl(var(--foreground))' }}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="Term"
                  value={data.health.term}
                  valueStyle={{ fontSize: 22, fontWeight: 600, color: 'hsl(var(--foreground))' }}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="Quorum"
                  value={`${data.health.quorum}/${data.health.peerCount}`}
                  valueStyle={{
                    fontSize: 22,
                    fontWeight: 600,
                    color: 'hsl(var(--foreground))',
                  }}
                />
              </Col>
            </Row>

            <div className="mt-3 flex items-start gap-1.5">
              <span className={cn(
                'inline-flex items-center gap-1 text-xs px-2 py-0.5 rounded-full',
                ['healthy', 'UP'].includes(data.health.status)
                  ? 'bg-[hsl(var(--success-muted))] text-[hsl(var(--success))]'
                  : 'bg-[hsl(var(--destructive))/0.1] text-[hsl(var(--destructive))]'
              )}>
                <Activity size={10} />
                {['healthy', 'UP'].includes(data.health.status) ? '健康' : data.health.status}
              </span>
              <span className="min-w-0 text-xs text-[hsl(var(--muted-foreground))]">
                <NetworkEndpoint value={formatEndpoint(data.health.host, data.health.port)} />
              </span>
            </div>
          </Card>
        </Col>

        {/* Broker 集群卡片 */}
        <Col xs={24} sm={12} lg={8}>
          <Card
            className="dashboard-broker-summary cursor-pointer hover:shadow-md transition-all duration-200 border-l-4 border-l-[hsl(var(--success))]"
            onClick={() => navigate('/clusters')}
          >
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-[hsl(var(--success-muted))] flex items-center justify-center">
                  <Boxes size={20} className="text-[hsl(var(--success))]" />
                </div>
                <div>
                  <div className="text-sm font-semibold text-[hsl(var(--foreground))]">Broker 集群</div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
                    {data.clusters.count} 个集群
                  </div>
                </div>
              </div>
              <ArrowRight size={16} className="text-[hsl(var(--muted-foreground))] mt-1" />
            </div>

            <Row gutter={16} className="mt-4 pt-4 border-t border-[hsl(var(--border))]">
              <Col span={12}>
                <Statistic
                  title="Broker 总数"
                  value={totalBrokers}
                  valueStyle={{ fontSize: 22, fontWeight: 600, color: 'hsl(var(--foreground))' }}
                />
              </Col>
              <Col span={12}>
                <Statistic
                  title="在线 / 已注册"
                  value={`${onlineBrokers} / ${totalBrokers}`}
                  valueStyle={{
                    fontSize: 22,
                    fontWeight: 600,
                    color: onlineBrokers === totalBrokers
                      ? 'hsl(var(--success))'
                      : 'hsl(var(--warning))',
                  }}
                />
              </Col>
            </Row>

            {data.clusters.clusters.length > 0 && <div className="mt-3">
              <ClusterCountBreakdown items={[...data.clusters.clusters]
                .sort((left, right) => left.clusterId.localeCompare(right.clusterId))
                .map(cluster => ({clusterId: cluster.clusterId, value: cluster.registeredBrokerCount}))} />
            </div>}
          </Card>
        </Col>

        {/* Worker 卡片 */}
        <Col xs={24} sm={12} lg={8}>
          <Card
            className="cursor-pointer hover:shadow-md transition-all duration-200 border-l-4 border-l-[hsl(var(--info))]"
            onClick={() => navigate('/workers')}
          >
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 rounded-xl bg-[hsl(var(--info-muted))] flex items-center justify-center">
                  <Cpu size={20} className="text-[hsl(var(--info))]" />
                </div>
                <div>
                  <div className="text-sm font-semibold text-[hsl(var(--foreground))]">Worker 注册</div>
                  <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">
                    {data.workers.total} 条注册记录
                  </div>
                </div>
              </div>
              <ArrowRight size={16} className="text-[hsl(var(--muted-foreground))] mt-1" />
            </div>

            <Row gutter={16} className="mt-4 pt-4 border-t border-[hsl(var(--border))]">
              <Col span={6}>
                <Statistic
                  title="总计"
                  value={data.workers.total}
                  valueStyle={{ fontSize: 22, fontWeight: 600, color: 'hsl(var(--foreground))' }}
                />
              </Col>
              <Col span={9}>
                <div className="flex items-end gap-1.5 h-full pb-1">
                  <CheckCircle2 size={14} className="text-[hsl(var(--success))]" />
                  <Statistic
                    title="在线"
                    value={data.workers.online}
                    valueStyle={{ fontSize: 22, fontWeight: 600, color: 'hsl(var(--success))' }}
                  />
                </div>
              </Col>
              <Col span={9}>
                <div className="flex items-end gap-1.5 h-full pb-1">
                  <AlertCircle size={14} className={data.workers.offline > 0 ? 'text-[hsl(var(--destructive))]' : 'text-[hsl(var(--muted-foreground))]'} />
                  <Statistic
                    title="离线"
                    value={data.workers.offline}
                    valueStyle={{
                      fontSize: 22,
                      fontWeight: 600,
                      color: data.workers.offline > 0 ? 'hsl(var(--destructive))' : 'hsl(var(--foreground))',
                    }}
                  />
                </div>
              </Col>
            </Row>

            <div className="worker-registration-progress mt-3">
              <Progress
                percent={workerOnlinePercent}
                size="small"
                strokeColor="hsl(var(--success))"
                trailColor="hsl(var(--border))"
                showInfo={false}
              />
              <span className="worker-registration-progress-label">{workerOnlinePercent}% 在线</span>
            </div>
          </Card>
        </Col>
      </Row>

      {/* === 第二行：PD 成员概览 + Broker 集群列表 === */}
      <Row gutter={[16, 16]}>
        {/* PD 成员快速预览 */}
        <Col xs={24} lg={12}>
          <Card
            className="dashboard-table-panel shadow-sm hover:shadow-md transition-all duration-200"
            title={
              <div className="flex items-center gap-2">
                <Server size={16} className="text-[hsl(var(--primary))]" />
                <span className="text-sm font-semibold">PD 成员</span>
              </div>
            }
            extra={
              <Button size="small" type="link" onClick={() => navigate('/pd')}>
                管理 <ArrowRight size={12} className="inline ml-1" />
              </Button>
            }
          >
            <Table
              dataSource={data.pdMembers.members}
              rowKey="memberId"
              size="small"
              pagination={false}
              scroll={{ y: 220 }}
              onRow={() => ({ onClick: () => navigate('/pd'), className: 'cursor-pointer' })}
              columns={[
                {
                  title: '节点',
                  dataIndex: 'memberId',
                  key: 'memberId',
                  render: (v: string) => <CopyableId id={v} maxLength={14} />,
                },
                {
                  title: '角色',
                  dataIndex: 'role',
                  key: 'role',
                  width: 100,
                  render: (v: PDMember['role'], r: PDMember) => (
                    <div className="flex items-center gap-1">
                      <RaftRoleTag role={v} />
                      {r.self && <Badge status="processing" />}
                    </div>
                  ),
                },
                {
                  title: '状态',
                  dataIndex: 'status',
                  key: 'status',
                  width: 70,
                  render: (v: string) => (
                    <span className="flex items-center gap-1 text-xs">
                      <span className={cn(
                        'w-1.5 h-1.5 rounded-full',
                        v?.toLowerCase() === 'online' ? 'bg-[hsl(var(--success))]' : 'bg-[hsl(var(--muted-foreground))]'
                      )} />
                      {v?.toLowerCase() === 'online' ? '在线' : v || '未知'}
                    </span>
                  ),
                },
                {
                  title: 'Commit',
                  dataIndex: 'commitIndex',
                  key: 'commitIndex',
                  width: 90,
                  className: 'font-mono-id text-xs',
                  render: (v: string) => v || '-',
                },
              ]}
            />
          </Card>
        </Col>

        {/* Broker 集群快速预览 */}
        <Col xs={24} lg={12}>
          <Card
            className="dashboard-table-panel shadow-sm hover:shadow-md transition-all duration-200"
            title={
              <div className="flex items-center gap-2">
                <Boxes size={16} className="text-[hsl(var(--success))]" />
                <span className="text-sm font-semibold">Broker 集群</span>
              </div>
            }
            extra={
              <Button size="small" type="link" onClick={() => navigate('/clusters')}>
                管理 <ArrowRight size={12} className="inline ml-1" />
              </Button>
            }
          >
            {data.clusters.clusters.length > 0 ? (
              <Table
                dataSource={data.clusters.clusters}
                rowKey="clusterId"
                size="small"
                pagination={false}
                scroll={{ y: 220 }}
                onRow={(record) => ({
                  onClick: () => navigate(`/clusters/${encodeURIComponent(record.clusterId)}`),
                  className: 'cursor-pointer',
                })}
                columns={[
                  {
                    title: '集群 ID',
                    dataIndex: 'clusterId',
                    key: 'clusterId',
                    render: (v: string) => <CopyableId id={v} maxLength={18} />,
                  },
                  {
                    title: '在线/已注册',
                    key: 'brokers',
                    width: 100,
                    render: (_, r: ClusterSummary) => (
                      <span className={cn(
                        'text-sm font-medium',
                        r.onlineBrokerCount < r.registeredBrokerCount
                          ? 'text-[hsl(var(--warning))]'
                          : 'text-[hsl(var(--success))]'
                      )}>
                        {r.onlineBrokerCount}/{r.registeredBrokerCount}
                      </span>
                    ),
                  },
                  {
                    title: '分区',
                    dataIndex: 'partitionSize',
                    key: 'partitionSize',
                    width: 60,
                  },
                  {
                    title: '副本',
                    dataIndex: 'replicationFactor',
                    key: 'replicationFactor',
                    width: 60,
                    render: (v: number) => (
                      <Tag className="rounded-full text-xs" color="purple">{v}</Tag>
                    ),
                  },
                ]}
              />
            ) : (
              <Empty description="暂无集群" image={Empty.PRESENTED_IMAGE_SIMPLE}>
                <Button size="small" type="primary" onClick={() => navigate('/clusters')}>
                  创建集群
                </Button>
              </Empty>
            )}
          </Card>
        </Col>
      </Row>

      
    </div>
  );
};

function cn(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(' ');
}
