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
 * Broker 集群列表页面
 * 表格展示所有集群，支持创建、Rebalance 与删除
 */

import React, { useState, useCallback } from 'react';
import {ChangeAcceptanceOutcome} from '@/shared/api/enums';
import { Card, Table, Button, Input, InputNumber, Form, Modal, Empty, Space, Tag, Badge } from 'antd';
import { useNavigate } from 'react-router-dom';
import { Plus, ArrowRight, RefreshCw, Boxes, Trash2 } from 'lucide-react';
import { CopyableId } from '@/shared/ui/CopyableId';
import { ConfirmModal } from '@/shared/ui/ConfirmModal';
import { PageHeader, MetricStrip } from '@/shared/ui/DetailPrimitives';
import {apiClient, clusterPath, ApiError} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState, MutationError} from '@/shared/ui/RequestState';
import type { ClusterListResponse, ClusterSummary, CreateClusterRequest, CreateClusterResponse, RebalanceResponse } from '@/shared/api/types';

export const ClusterListPage: React.FC = () => {
  const navigate = useNavigate();
  const [createOpen, setCreateOpen] = useState(false);
  const [rebalanceTarget, setRebalanceTarget] = useState<ClusterSummary | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ClusterSummary | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const state = usePolling(useCallback((signal: AbortSignal) => apiClient.get<ClusterListResponse>('/broker/clusters', signal), []), {interval: 5000, enabled: true, cacheKey: 'clusters'});
  const clusterList = state.data;
  const [createError, setCreateError] = useState<unknown>(null);
  const [notice, setNotice] = useState('');
  const [form] = Form.useForm();

  const handleCreate = async (values: CreateClusterRequest) => {
    if (submitting || (createError instanceof ApiError && createError.outcomeUnknown)) return;
    setSubmitting(true);
    setCreateError(null);
    try {
      const result = await apiClient.post<CreateClusterResponse>('/broker/clusters', values);
      setCreateOpen(false);
      form.resetFields();
      navigate(`/clusters/${encodeURIComponent(result.clusterId)}`);
    } catch (error) { setCreateError(error); }
    finally { setSubmitting(false); }
  };

  const handleRebalance = async () => {
    if (!rebalanceTarget) return;
    const result = await apiClient.post<RebalanceResponse>(`${clusterPath(rebalanceTarget.clusterId)}/rebalance`);
    const id = rebalanceTarget.clusterId;
    setRebalanceTarget(null);
    if (result.changeId && result.changeId !== ChangeAcceptanceOutcome.NO_CHANGES) navigate(`/changes/clusters/${encodeURIComponent(id)}/${encodeURIComponent(result.changeId)}`);
    else { setNotice('无需变更，已刷新配置'); state.refresh(); }
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    const current = await apiClient.get<ClusterListResponse>('/broker/clusters');
    const target = current.clusters.find(c => c.clusterId === deleteTarget.clusterId);
    if (target && target.onlineBrokerCount > 0) throw new ApiError('请先停止集群全部 Broker，再删除集群', 409, 'BROKERS_ONLINE');
    await apiClient.delete(clusterPath(deleteTarget.clusterId));
    setDeleteTarget(null);
    state.refresh();
  };
  if (!clusterList) return <RequestState state={state} />;

  const totalRegistered = clusterList.clusters.reduce((s, c) => s + c.registeredBrokerCount, 0);
  const totalOnline = clusterList.clusters.reduce((s, c) => s + c.onlineBrokerCount, 0);
  const totalPartitions = clusterList.clusters.reduce((s, c) => s + c.partitionSize, 0);

  return (
    <div className="space-y-6">
      <RequestState state={state} />
      {notice && <p>{notice}</p>}
      {/* 页头 + 统计概览 */}
      <PageHeader title="Broker 集群" icon={<Boxes size={22} />} subtitle="管理集群配置、Broker 注册与分区分布"
        actions={<Button type="primary" icon={<Plus size={14} />} onClick={() => {setCreateError(null); setCreateOpen(true);}}>
          创建集群
        </Button>} />
      <MetricStrip items={[
        { label: '集群数', value: clusterList.count, tone: 'brand' },
        { label: '已注册 Broker', value: totalRegistered },
        { label: '在线 Broker', value: totalOnline, tone: 'success' },
        { label: '分区总数', value: totalPartitions },
      ]} />

      {/* 集群表格 */}
      <Card className="shadow-sm">
        {clusterList.clusters.length > 0 ? (
          <Table
            className="cluster-list-table"
            dataSource={clusterList.clusters}
            rowKey="clusterId"
            size="small"
            tableLayout="fixed"
            scroll={{x: 1100}}
            onRow={(record) => ({
              onClick: () => navigate(`/clusters/${encodeURIComponent(record.clusterId)}`),
              className: 'cursor-pointer',
            })}
            columns={[
              {
                title: '集群 ID',
                dataIndex: 'clusterId',
                key: 'clusterId',
                width: '18%',
                align: 'left',
                render: (v: string) => <CopyableId id={v} maxLength={24} />,
              },
              {
                title: '目标 Broker',
                dataIndex: 'clusterSize',
                key: 'clusterSize',
                width: '16%',
                align: 'center',
                render: (v: number) => <Tag className="cluster-count-tag" color="blue">{v}</Tag>,
              },
              {
                title: '在线 / 已注册',
                key: 'brokers',
                width: '20%',
                align: 'center',
                render: (_, r: ClusterSummary) => {
                  const healthy = r.onlineBrokerCount === r.registeredBrokerCount;
                  return (
                    <Tag className="cluster-count-tag" color={healthy ? 'success' : 'warning'}>
                      {r.onlineBrokerCount} / {r.registeredBrokerCount}
                    </Tag>
                  );
                },
              },
              {
                title: '分区',
                dataIndex: 'partitionSize',
                key: 'partitionSize',
                width: '12%',
                align: 'center',
                render: (v: number) => <Tag className="cluster-count-tag" color="cyan">{v}</Tag>,
              },
              {
                title: '副本因子',
                dataIndex: 'replicationFactor',
                key: 'replicationFactor',
                width: '14%',
                align: 'center',
                render: (v: number) => <Tag className="cluster-count-tag" color="purple">{v}</Tag>,
              },
              {
                title: '操作',
                key: 'action',
                width: '20%',
                align: 'center',
                render: (_, record: ClusterSummary) => (
                  <Space size={8}>
                    <Button
                      size="small"
                      type="primary"
                      ghost
                      className="cluster-rebalance-button"
                      icon={<RefreshCw size={12} />}
                      disabled={record.registeredBrokerCount === 0}
                      onClick={(e) => {
                        e.stopPropagation();
                        setRebalanceTarget(record);
                      }}
                    >
                      Rebalance
                    </Button>
                    <Button
                      size="small"
                      danger
                      icon={<Trash2 size={12} />}
                      disabled={record.onlineBrokerCount > 0}
                      onClick={(e) => {
                        e.stopPropagation();
                        setDeleteTarget(record);
                      }}
                    >
                      删除
                    </Button>
                  </Space>
                ),
              },
            ]}
          />
        ) : (
          <Empty
            description="暂无集群"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          >
            <Button size="small" type="primary" onClick={() => {setCreateError(null); setCreateOpen(true);}}>
              创建集群
            </Button>
          </Empty>
        )}
      </Card>

      {/* Create Modal */}
      <Modal
        className="console-modal"
        width={560}
        open={createOpen}
        title={<div className="flex items-center gap-3"><span className="object-icon"><Boxes size={22} /></span><span>创建 Broker 集群</span></div>}
        onCancel={() => { if (!submitting) {setCreateOpen(false); form.resetFields();} }}
        onOk={() => form.submit()}
        confirmLoading={submitting}
        okButtonProps={{'aria-label': '创建', disabled: createError instanceof ApiError && createError.outcomeUnknown}}
        okText="创建"
        cancelText="取消"
        destroyOnClose
      >
        <MutationError error={createError} />
        <p className="text-xs text-[hsl(var(--muted-foreground))] mb-5">设置集群标识与目标容量，完成后可查看 Broker 与分区详情。</p>
        <Form form={form} layout="vertical" onFinish={handleCreate} className="pt-2">
          <Form.Item
            name="clusterId"
            label="Cluster ID"
            rules={[
              { required: true, message: '请输入 Cluster ID' },
              { pattern: /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/, message: '格式不正确，最多128字符' },
            ]}
          >
            <Input placeholder="如: broker-cluster-01" />
          </Form.Item>
          <Form.Item
            name="clusterSize"
            label="目标 Broker 数"
            initialValue={3}
            rules={[{ required: true, type: 'number', min: 1, message: '正整数' }]}
          >
            <InputNumber min={1} className="w-full" />
          </Form.Item>
          <Form.Item
            name="partitionSize"
            label="分区数"
            initialValue={5}
            rules={[{ required: true, type: 'number', min: 1, max: 511, message: '1-511' }]}
          >
            <InputNumber min={1} max={511} className="w-full" />
          </Form.Item>
          <Form.Item
            name="replicationFactor"
            label="副本因子"
            initialValue={3}
            rules={[
              { required: true, type: 'number', min: 1, message: '正整数' },
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value || value <= getFieldValue('clusterSize')) return Promise.resolve();
                  return Promise.reject(new Error('副本因子不能超过目标 Broker 数'));
                },
              }),
            ]}
          >
            <InputNumber min={1} className="w-full" />
          </Form.Item>
        </Form>
      </Modal>

      {/* Rebalance Confirm */}
      <ConfirmModal
        open={!!rebalanceTarget}
        title="确认 Rebalance"
        content={
          <p>确认对集群 <strong>{rebalanceTarget?.clusterId}</strong> 执行 Rebalance？此操作将重新平衡分区分布。</p>
        }
        confirmLabel="确认执行"
        onConfirm={handleRebalance}
        onCancel={() => setRebalanceTarget(null)}
      />

      {/* Delete Confirm */}
      <ConfirmModal
        open={!!deleteTarget}
        title={`删除集群 ${deleteTarget?.clusterId}`}
        content={
          <div className="space-y-2 text-sm">
            <p>此操作将清除 PD 的集群配置、注册、Worker、拓扑、计划与分发状态。</p>
            <p>该集群作为 Source 或 Target 的所有 Mirror 关系也将一并删除。</p>
            <p className="text-[hsl(var(--destructive))] font-medium">删除不停止 Broker，也不删除物理数据目录。</p>
            <p>请确认该集群所有 Broker 均已 OFFLINE。</p>
          </div>
        }
        confirmText={deleteTarget?.clusterId}
        confirmLabel="确认删除"
        danger
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
};

function cn(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(' ');
}
