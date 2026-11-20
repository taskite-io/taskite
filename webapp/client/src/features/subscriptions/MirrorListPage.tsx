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
 * Mirror 列表页面
 * 展示完整 Mirror 关系，支持创建与删除
 */

import React, { useState, useCallback, useEffect } from 'react';
import { Card, Table, Input, Button, Form, Modal, Empty, Tag, Select, Alert } from 'antd';
import { useNavigate } from 'react-router-dom';
import { ArrowRight, Plus, Trash2, HardDrive } from 'lucide-react';
import { StatusTag } from '@/shared/ui/StatusTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import { ConfirmModal } from '@/shared/ui/ConfirmModal';
import { PageHeader } from '@/shared/ui/DetailPrimitives';
import {apiClient, ApiError} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState, MutationError} from '@/shared/ui/RequestState';
import {useSearchParams} from 'react-router-dom';
import type { MirrorListResponse, MirrorItem, CreateMirrorRequest, ClusterListResponse } from '@/shared/api/types';

export const MirrorListPage: React.FC = () => {
  const navigate = useNavigate();
  const [createOpen, setCreateOpen] = useState(false);
  const [deleteTarget, setDeleteTarget] = useState<MirrorItem | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [params, setParams] = useSearchParams();
  const source = params.get('sourceClusterId') || '';
  const target = params.get('targetClusterId') || '';
  const [sourceInput, setSourceInput] = useState(source);
  const [targetInput, setTargetInput] = useState(target);
  const state = usePolling(useCallback((signal: AbortSignal) => {
    const query = new URLSearchParams();
    if (source) query.set('sourceClusterId', source);
    if (target) query.set('targetClusterId', target);
    return apiClient.get<MirrorListResponse>(`/broker/job-subscription-mirrors${query.size ? `?${query}` : ''}`, signal);
  }, [source, target]), {interval: 5000, enabled: true, cacheKey: JSON.stringify(['mirrors', source, target])});
  const mirrorList = state.data;
  const [createError, setCreateError] = useState<unknown>(null);
  const [form] = Form.useForm();
  const clusterState = usePolling(useCallback((signal: AbortSignal) =>
    apiClient.get<ClusterListResponse>('/broker/clusters', signal), []), {interval: 5000, enabled: createOpen});
  const sourceClusterId = Form.useWatch('sourceClusterId', form);
  const targetClusterId = Form.useWatch('targetClusterId', form);
  const clusters = clusterState.data?.clusters ?? [];
  const clusterOptions = clusters.map(cluster => ({value: cluster.clusterId, label: cluster.clusterId}));
  const clustersUnavailable = !clusterState.data || clusterState.loading || clusterState.stale || !!clusterState.error;
  const validSelection = !clustersUnavailable && sourceClusterId && targetClusterId
    && sourceClusterId !== targetClusterId
    && clusters.some(cluster => cluster.clusterId === sourceClusterId)
    && clusters.some(cluster => cluster.clusterId === targetClusterId);
  useEffect(() => {
    if (!createOpen || !clusterState.data || clusterState.stale || clusterState.error) return;
    for (const name of ['sourceClusterId', 'targetClusterId']) {
      const value = form.getFieldValue(name);
      if (value && !clusterState.data.clusters.some(cluster => cluster.clusterId === value)) {
        form.setFields([{name, value: undefined, errors: ['所选集群已不存在，请重新选择']}]);
      }
    }
  }, [createOpen, clusterState.data, clusterState.stale, clusterState.error, form]);

  const handleCreate = async (values: CreateMirrorRequest) => {
    if (submitting || (createError instanceof ApiError && createError.outcomeUnknown)) return;
    setSubmitting(true);
    setCreateError(null);
    try {
      const latest = await apiClient.get<ClusterListResponse>('/broker/clusters');
      if (values.sourceClusterId === values.targetClusterId
        || !latest.clusters.some(cluster => cluster.clusterId === values.sourceClusterId)
        || !latest.clusters.some(cluster => cluster.clusterId === values.targetClusterId)) {
        clusterState.refresh();
        throw new ApiError('请选择两个不同的现有集群，所选集群可能已被删除', 409, 'INVALID_CLUSTER_SELECTION');
      }
      const result = await apiClient.post<MirrorItem>('/broker/job-subscription-mirrors', values);
      setCreateOpen(false);
      form.resetFields();
      navigate(`/subscriptions/mirrors/${encodeURIComponent(result.mirrorKey)}`);
    } catch (error) {setCreateError(error);}
    finally {setSubmitting(false);}
  };

  const handleDelete = async () => {
    if (!deleteTarget) return;
    await apiClient.delete(`/broker/job-subscription-mirrors/${encodeURIComponent(deleteTarget.mirrorKey)}`);
    setDeleteTarget(null);
    state.refresh();
  };
  if (!mirrorList) return <RequestState state={state} />;

  return (
    <div className="space-y-6">
      <RequestState state={state} />
      <PageHeader title="Mirror 列表" icon={<HardDrive size={22} />}
        subtitle={`${mirrorList.mirrors.length} 条任务订阅同步关系`}
        actions={<Button type="primary" icon={<Plus size={14} />} onClick={() => {setCreateError(null); setCreateOpen(true);}}>
          创建 Mirror
        </Button>} />

      <Card className="shadow-sm">
        <div className="filter-toolbar mb-4">
          <Input aria-label="筛选源集群" placeholder="源集群 ID" value={sourceInput} onChange={e => setSourceInput(e.target.value)} />
          <Input aria-label="筛选目标集群" placeholder="目标集群 ID" value={targetInput} onChange={e => setTargetInput(e.target.value)} />
          <Button onClick={() => {const next = new URLSearchParams(); if (sourceInput.trim()) next.set('sourceClusterId', sourceInput.trim()); if (targetInput.trim()) next.set('targetClusterId', targetInput.trim()); setParams(next);}}>筛选</Button>
        </div>
        {mirrorList.mirrors.length > 0 ? (
          <Table
            dataSource={mirrorList.mirrors}
            rowKey="mirrorKey"
            size="small"
            scroll={{ x: 720 }}
            onRow={(record) => ({
              onClick: () => navigate(`/subscriptions/mirrors/${record.mirrorKey}`),
              className: 'cursor-pointer',
            })}
            columns={[
              {
                title: 'Mirror Key',
                dataIndex: 'mirrorKey',
                key: 'mirrorKey',
                render: (v: string) => <CopyableId id={v} maxLength={24} />,
              },
              {
                title: '源 → 目标',
                key: 'direction',
                render: (_, r: MirrorItem) => (
                  <div className="flex items-center gap-2 text-sm">
                    <Tag className="rounded-full text-xs" color="blue">{r.sourceClusterId}</Tag>
                    <ArrowRight size={12} className="text-[hsl(var(--muted-foreground))]" />
                    <Tag className="rounded-full text-xs" color="green">{r.targetClusterId}</Tag>
                  </div>
                ),
              },
              {
                title: '状态',
                dataIndex: 'status',
                key: 'status',
                width: 100,
                render: (v: string) => <StatusTag status={v} />,
              },
              {
                title: '创建',
                dataIndex: 'created',
                key: 'created',
                width: 80,
                render: (v?: boolean) =>
                  v === true ? <Tag color="success" className="rounded-full text-xs">新建</Tag>
                    : v === false ? <Tag className="rounded-full text-xs">已存在</Tag>
                    : '-',
              },
              {
                title: '',
                key: 'action',
                width: 80,
                render: (_, record: MirrorItem) => (
                  <Button
                    size="small"
                    danger
                    icon={<Trash2 size={12} />}
                    onClick={(e) => {
                      e.stopPropagation();
                      setDeleteTarget(record);
                    }}
                  >
                    删除
                  </Button>
                ),
              },
            ]}
          />
        ) : (
          <Empty description="暂无 Mirror 关系" image={Empty.PRESENTED_IMAGE_SIMPLE}>
            <Button size="small" type="primary" onClick={() => {setCreateError(null); setCreateOpen(true);}}>创建 Mirror</Button>
          </Empty>
        )}
      </Card>

      {/* Create Modal */}
      <Modal
        className="console-modal"
        width={560}
        open={createOpen}
        title={
          <div className="flex items-center gap-2">
            <span className="object-icon"><HardDrive size={22} /></span>
            <span>创建 Mirror</span>
          </div>
        }
        onCancel={() => { if (!submitting) {setCreateOpen(false); form.resetFields();} }}
        onOk={() => form.submit()}
        confirmLoading={submitting}
        okButtonProps={{'aria-label': '创建', disabled: !validSelection || (createError instanceof ApiError && createError.outcomeUnknown)}}
        okText="创建"
        cancelText="取消"
        destroyOnClose
      >
        <MutationError error={createError} />
        <p className="text-xs text-[hsl(var(--muted-foreground))] mb-5">从当前已有集群中选择源与目标，建立单向任务订阅同步关系。</p>
        <RequestState state={clusterState} />
        {!clustersUnavailable && clusters.length < 2 && <Alert className="mb-4" type="info" showIcon
          message="至少需要两个现有集群才能创建 Mirror" />}
        <Form form={form} layout="vertical" onFinish={handleCreate} className="pt-2">
          <Form.Item
            name="sourceClusterId"
            label="源集群 ID"
            rules={[{ required: true, message: '请选择源集群' }]}
          >
            <Select aria-label="源集群 ID" showSearch allowClear optionFilterProp="label" placeholder="选择源集群"
              disabled={submitting || clustersUnavailable}
              options={clusterOptions.filter(option => option.value !== targetClusterId)}
              notFoundContent={<Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="没有可选的源集群" />} />
          </Form.Item>
          <Form.Item
            name="targetClusterId"
            label="目标集群 ID"
            rules={[
              { required: true, message: '请选择目标集群' },
              ({ getFieldValue }) => ({
                validator(_, value) {
                  if (!value || value !== getFieldValue('sourceClusterId')) return Promise.resolve();
                  return Promise.reject(new Error('源和目标不能相同'));
                },
              }),
            ]}
          >
            <Select aria-label="目标集群 ID" showSearch allowClear optionFilterProp="label" placeholder="选择目标集群"
              disabled={submitting || clustersUnavailable}
              options={clusterOptions.filter(option => option.value !== sourceClusterId)}
              notFoundContent={<Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="没有可选的目标集群" />} />
          </Form.Item>
        </Form>
      </Modal>

      {/* Delete Confirm */}
      <ConfirmModal
        open={!!deleteTarget}
        title={`删除 Mirror ${deleteTarget?.mirrorKey}`}
        content={
          <div className="space-y-2 text-sm">
            <p>确认删除以下 Mirror 关系？</p>
            <div className="flex items-center gap-2 p-2 rounded bg-[hsl(var(--muted))]">
              <strong>{deleteTarget?.sourceClusterId}</strong>
              <span className="text-[hsl(var(--muted-foreground))]">→</span>
              <strong>{deleteTarget?.targetClusterId}</strong>
            </div>
            <p className="text-[hsl(var(--warning))]">删除只删除关系，不撤回已分发的任务订阅。</p>
          </div>
        }
        confirmLabel="确认删除"
        danger
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
};
