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
 * Broker 集群详情页面
 * 页签：概览 / Broker / 分区；配置通过右侧抽屉编辑
 * 支持 Quorum Recovery 与配置 PATCH
 */

import {ChangeAcceptanceOutcome, RegistrationStatus, MemberRole} from '@/shared/api/enums';
import React, { useEffect, useState, useCallback } from 'react';
import {
  Card, Descriptions, Table, Tabs, Tag, Button, Form, Input, InputNumber,
  Alert, Empty, Drawer, Modal, Badge, Row, Col, Collapse, Select, Tooltip,
} from 'antd';
import { useParams, useNavigate, useSearchParams } from 'react-router-dom';
import {
  ArrowLeft, Boxes, ShieldAlert, Activity, ListChecks, Columns,
  CheckCircle2, AlertCircle, Settings, PlusCircle, MinusCircle,
} from 'lucide-react';
import { StatusTag } from '@/shared/ui/StatusTag';
import {RaftRoleTag} from '@/shared/ui/RaftRoleTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import {NetworkEndpoint} from '@/shared/ui/NetworkEndpoint';
import { ConfirmModal } from '@/shared/ui/ConfirmModal';
import {ConfigurationChangeSummary, type ConfigurationChange} from '@/shared/ui/ConfigurationChangeSummary';
import { PageHeader, DrawerTitle, DetailSection, MetricStrip } from '@/shared/ui/DetailPrimitives';
import { formatInt64, formatDateTime, formatDateTimeRelative, formatEndpoint } from '@/shared/lib/format';
import {apiClient, activeChange, isNoBrokerRegistration, clusterPath, ApiError} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState, MutationError} from '@/shared/ui/RequestState';
import { PartitionOwnershipMatrix } from './PartitionOwnershipMatrix';
import {validateConfiguration, type ConfigurationDraft} from './configurationValidation';
import type {
  ClusterConfiguration, BrokerTopology, BrokerChangeDetail, BrokerInfo,
  PartitionPlacement, QuorumRecoveryRequest, QuorumRecoveryResponse, BrokerPartitionReport, RebalanceResponse, ClusterPatchRequest,
} from '@/shared/api/types';

function cn(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(' ');
}

function addableBrokers(topology: BrokerTopology | null): BrokerInfo[] {
  const voters = new Set(topology?.partitions.flatMap(partition => [partition.leaderId, ...partition.replicas]) ?? []);
  return topology?.brokers.filter(broker => broker.status === RegistrationStatus.ONLINE
    && broker.partitions.every(partition => partition.role === MemberRole.LEARNER)
    && !voters.has(broker.brokerId)) ?? [];
}

function configurationPatch(draft: ConfigurationDraft, current: ClusterConfiguration): ClusterPatchRequest {
  const brokers: NonNullable<ClusterPatchRequest['brokers']> = {};
  const partitions: NonNullable<ClusterPatchRequest['partitions']> = {};
  if (draft.brokersCount != null && draft.brokersCount !== current.clusterSize) brokers.count = draft.brokersCount;
  if (draft.brokersAdd?.length) brokers.add = draft.brokersAdd;
  if (draft.brokersRemove?.length) brokers.remove = draft.brokersRemove;
  if (draft.partitionCount != null && draft.partitionCount !== current.partitionSize) partitions.count = draft.partitionCount;
  if (draft.replicationFactor != null && draft.replicationFactor !== current.replicationFactor) partitions.replicationFactor = draft.replicationFactor;
  return {
    ...(Object.keys(brokers).length ? {brokers} : {}),
    ...(Object.keys(partitions).length ? {partitions} : {}),
  };
}

async function readClusterDetailData(clusterId: string, signal: AbortSignal) {
  const path = clusterPath(clusterId);
  const config = await apiClient.get<ClusterConfiguration>(`${path}/configuration`, signal);
  let topology: BrokerTopology | null;
  try { topology = await apiClient.get<BrokerTopology>(`${path}/topology`, signal); }
  catch (error) { if (isNoBrokerRegistration(error)) topology = null; else throw error; }
  return {
    config, topology,
    activeChange: await activeChange<BrokerChangeDetail>(`${path}/changes/active`, signal),
  };
}

export const ClusterDetailPage: React.FC = () => {
  const { clusterId } = useParams<{ clusterId: string }>();
  const navigate = useNavigate();
  const decodedClusterId = clusterId || '';

  const state = usePolling(useCallback((signal: AbortSignal) => readClusterDetailData(decodedClusterId, signal), [decodedClusterId]), {interval: 5000, enabled: !!decodedClusterId, cacheKey: JSON.stringify(['cluster', decodedClusterId])});
  const detailData = state.data;
  const [recoveryError, setRecoveryError] = useState<unknown>(null);
  const [selectedBroker, setSelectedBroker] = useState<BrokerInfo | null>(null);
  const [selectedPartition, setSelectedPartition] = useState<PartitionPlacement | null>(null);
  const [recoveryOpen, setRecoveryOpen] = useState(false);
  const [recoveryForm, setRecoveryForm] = useState<Partial<QuorumRecoveryRequest>>({});
  const [patchForm, setPatchForm] = useState<ConfigurationDraft>({});
  const [brokerCountInput, setBrokerCountInput] = useState<string | null>(null);
  const [replicaInput, setReplicaInput] = useState<string | null>(null);
  const [partitionInput, setPartitionInput] = useState<string | null>(null);
  const [partitionInputError, setPartitionInputError] = useState('');
  const [partitionInputRevision, setPartitionInputRevision] = useState(0);
  const [editorOpen, setEditorOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [params, setParams] = useSearchParams();
  const requestedTab = params.get('tab');
  const activeTab = requestedTab === 'brokers' || requestedTab === 'partitions' ? requestedTab : 'overview';
  const setActiveTab = (tab: string) => setParams({tab});
  const [recoveryConfirmation, setRecoveryConfirmation] = useState('');
  useEffect(() => {
    setSelectedBroker(null);
    setSelectedPartition(null);
    setRecoveryOpen(false);
    setPatchForm({});
    setBrokerCountInput(null);
    setReplicaInput(null);
    setPartitionInput(null);
    setPartitionInputError('');
    setEditorOpen(false);
    setConfirmOpen(false);
  }, [decodedClusterId]);

  const handlePatch = async () => {
    const latest = await readClusterDetailData(decodedClusterId, new AbortController().signal);
    if (!latest.topology?.brokers.length || latest.activeChange) {
      state.refresh();
      throw new ApiError('请等待 Broker 注册或当前变更完成后再提交', 409, 'INVALID_CONFIGURATION');
    }
    const add = patchForm.brokersAdd ?? [];
    if (add.length) {
      const candidates = new Set(addableBrokers(latest.topology).map(broker => broker.brokerId));
      if (add.some(id => !candidates.has(id))) {
        state.refresh();
        throw new ApiError('所选 Broker 已不满足在线备用节点的条件，请返回编辑重新选择', 409, 'INVALID_BROKER_SELECTION');
      }
    }
    const validation = validateConfiguration(patchForm, latest.config, latest.topology);
    if (validation.errors.length) {
      state.refresh();
      throw new ApiError(validation.errors.join('；'), 409, 'INVALID_CONFIGURATION');
    }
    if (patchForm.partitionCount != null && patchForm.partitionCount < latest.config.partitionSize) throw new ApiError('分区数只允许增加', 400, 'INVALID_CONFIGURATION');
    const body = configurationPatch(patchForm, latest.config);
    if (!Object.keys(body).length) throw new ApiError('配置与当前状态一致，无需提交变更', 400, 'NO_CONFIGURATION_CHANGES');
    const result = await apiClient.patch<RebalanceResponse>(`${clusterPath(decodedClusterId)}/configuration`, body);
    setConfirmOpen(false);
    setPatchForm({});
    setBrokerCountInput(null);
    setReplicaInput(null);
    setPartitionInput(null);
    setPartitionInputError('');
    if (result.changeId && result.changeId !== ChangeAcceptanceOutcome.NO_CHANGES) navigate(`/changes/clusters/${encodeURIComponent(decodedClusterId)}/${encodeURIComponent(result.changeId)}`);
    else state.refresh();
  };

  const handleRecovery = async () => {
    if (submitting || (recoveryError instanceof ApiError && recoveryError.outcomeUnknown)) return;
    if (!recoveryForm.fencingProof?.trim() || !selectedPartition ||
      recoveryConfirmation !== `${decodedClusterId}/${selectedPartition.partitionId}`) return;
    setSubmitting(true);
    setRecoveryError(null);
    try {
      const result = await apiClient.post<QuorumRecoveryResponse>(`${clusterPath(decodedClusterId)}/partitions/${selectedPartition.partitionId}/quorum-recovery`, {
        confirm: 'true', fencingProof: recoveryForm.fencingProof.trim(),
        ...(recoveryForm.survivor?.trim() ? {survivor: recoveryForm.survivor.trim()} : {}),
      });
      setRecoveryOpen(false);
      setRecoveryForm({});
      navigate(`/changes/clusters/${encodeURIComponent(decodedClusterId)}/${encodeURIComponent(result.changeId)}`, {
        state: {recovery: {partitionId: result.partitionId, survivor: result.survivor, recoveryEpoch: result.recoveryEpoch}},
      });
    } catch (error) {setRecoveryError(error);}
    finally {setSubmitting(false);}
  };

  if (!detailData) return <RequestState state={state} />;
  const noBrokerRegistered = !detailData.topology || detailData.topology.brokers.length === 0;
  const topology = detailData.topology;
  const config = detailData.config;
  // Empty collections render the waiting view; unknown topology metadata stays absent.
  const brokers = topology?.brokers ?? [];
  const partitions = topology?.partitions ?? [];
  const addList = patchForm.brokersAdd || [];
  const removeList = patchForm.brokersRemove || [];
  const brokerCandidates = addableBrokers(topology);
  const invalidAddList = addList.some(id => !brokerCandidates.some(broker => broker.brokerId === id));
  const addSelectionUnavailable = addList.length > 0 && (invalidAddList || state.stale || !!state.error);
  const enteredNumber = (raw: string | null, value: number | undefined) => raw == null ? value
    : raw.trim() === '' ? undefined : Number(raw);
  const validation = validateConfiguration({
    ...patchForm,
    brokersCount: enteredNumber(brokerCountInput, patchForm.brokersCount),
    replicationFactor: enteredNumber(replicaInput, patchForm.replicationFactor),
  }, config, topology);
  const enteredPartitionCount = partitionInput != null && partitionInput.trim() !== '' ? Number(partitionInput) : patchForm.partitionCount;
  const invalidPartitionCount = enteredPartitionCount != null && (!Number.isInteger(enteredPartitionCount)
    || enteredPartitionCount < config.partitionSize || enteredPartitionCount > 511);
  const patch = configurationPatch(patchForm, config);
  const hasPatch = Object.keys(patch).length > 0;
  const snapshotUnavailable = hasPatch && (state.stale || !!state.error);
  const cannotPreview = !hasPatch || validation.errors.length > 0 || addSelectionUnavailable || snapshotUnavailable
    || invalidPartitionCount || noBrokerRegistered || !!detailData.activeChange;
  const reducingBrokers = patch.brokers?.count != null && patch.brokers.count < config.clusterSize;
  const reducingReplicas = patch.partitions?.replicationFactor != null && patch.partitions.replicationFactor < config.replicationFactor;
  const dangerousChange = removeList.length > 0 || reducingBrokers || reducingReplicas;
  const riskDescription = [
    (removeList.length > 0 || reducingBrokers) && '移除 Broker 或缩减节点数量可能触发分区迁移与 Leader 切换，期间请求可能出现延迟。',
    reducingReplicas && '降低副本因子会减少数据副本，降低故障容忍能力。',
    '请确认剩余在线节点能够承载分区，并满足副本和多数派要求。',
  ].filter(Boolean).join('');
  const changes: ConfigurationChange[] = [];
  if (addList.length) changes.push({kind: 'add', label: 'Broker', members: addList});
  if (removeList.length) changes.push({kind: 'remove', label: 'Broker', members: removeList});
  if (patch.brokers?.count != null) changes.push({kind: 'update', label: 'Broker 数量',
    before: config.clusterSize, after: patch.brokers.count, danger: reducingBrokers});
  if (patch.partitions?.count != null) changes.push({kind: 'update', label: '分区数',
    before: config.partitionSize, after: patch.partitions.count});
  if (patch.partitions?.replicationFactor != null) changes.push({kind: 'update', label: '副本因子',
    before: config.replicationFactor, after: patch.partitions.replicationFactor, danger: reducingReplicas});
  const commitPartitionInput = () => {
    if (enteredPartitionCount != null && enteredPartitionCount < config.partitionSize) {
      setPatchForm((p) => ({...p, partitionCount: config.partitionSize}));
      setPartitionInput(null);
      setPartitionInputError(`分区数只能增加，已恢复为当前配置（${config.partitionSize}）`);
      setPartitionInputRevision((revision) => revision + 1);
    }
  };

  return (
    <div className="space-y-6">
      <RequestState state={state} />
      {noBrokerRegistered && <Alert message="等待 Broker 注册" description="集群配置已创建，请启动并注册 Broker。" type="info" showIcon />}
      {/* 页头 */}
      <PageHeader title={decodedClusterId} icon={<Boxes size={22} />}
        subtitle={`Broker 集群 · ${topology?.version ?? '等待拓扑'}`}
        onBack={() => navigate('/clusters')}
        badges={<Tag color="blue">Broker 集群</Tag>}
        actions={<Tooltip title={noBrokerRegistered ? '等待 Broker 注册后可修改配置' : detailData.activeChange ? '请等待当前变更完成' : undefined}>
          <span><Button type="primary" icon={<Settings size={14} />} disabled={noBrokerRegistered || !!detailData.activeChange}
            onClick={() => setEditorOpen(true)}>修改配置</Button></span>
        </Tooltip>} />
      <MetricStrip items={[
        { label: '在线 Broker', value: `${brokers.filter((b) => b.status.toLowerCase() === 'online').length} / ${brokers.length}`, tone: 'success', hint: `目标数量 ${config.clusterSize}` },
        { label: '分区数', value: config.partitionSize, tone: 'brand' },
        { label: '副本因子', value: config.replicationFactor },
        { label: '当前变更', value: detailData.activeChange ? <StatusTag status={detailData.activeChange.status} category="change" /> : '无' },
      ]} />

      {/* Tabs */}
      <Tabs
        className="detail-tabs"
        activeKey={activeTab}
        onChange={setActiveTab}
        size="large"
        items={[
          {
            key: 'overview',
            label: (
              <span className="flex items-center gap-1.5">
                <Activity size={14} /> 概览
              </span>
            ),
            children: (
              <div className="space-y-4">
                {/* 配置概览 */}
                <DetailSection title="集群信息" description="配置与最近一次拓扑快照" className="cluster-information">
                  <Descriptions size="small" column={{ xs: 1, sm: 2, md: 2, lg: 4, xl: 4, xxl: 4 }} layout="horizontal">
                    <Descriptions.Item label="Cluster ID">
                      <CopyableId id={config.clusterId} maxLength={20} />
                    </Descriptions.Item>
                    <Descriptions.Item label="Cluster Key">{config.clusterKey}</Descriptions.Item>
                    <Descriptions.Item label="目标 Broker">{config.clusterSize}</Descriptions.Item>
                    <Descriptions.Item label="在线 Broker">
                      <span className={cn(
                        brokers.filter((b) => b.status.toLowerCase() === 'online').length === brokers.length
                          ? 'text-[hsl(var(--success))]'
                          : 'text-[hsl(var(--warning))]'
                      )}>
                        {brokers.filter((b) => b.status.toLowerCase() === 'online').length}
                      </span>
                    </Descriptions.Item>
                    <Descriptions.Item label="分区数">{config.partitionSize}</Descriptions.Item>
                    <Descriptions.Item label="副本因子">{config.replicationFactor}</Descriptions.Item>
                    <Descriptions.Item label="Topology Version">
                      <span className="font-mono-id text-xs">{topology?.version ?? '—'}</span>
                    </Descriptions.Item>
                    <Descriptions.Item label="最后更新">
                      {topology ? formatDateTimeRelative(topology.updatedAt) : '—'}
                    </Descriptions.Item>
                  </Descriptions>
                </DetailSection>

                {/* 当前变更 */}
                {detailData.activeChange && (
                  <DetailSection title="当前变更" description="查看调度计划与执行进度">
                    <div className="object-summary">
                      <div className="flex items-center gap-3 flex-wrap">
                        <ListChecks size={16} className="text-[hsl(var(--primary))]" />
                        <span className="text-sm font-semibold">当前变更</span>
                        <CopyableId id={detailData.activeChange.changeId} maxLength={16} />
                        <StatusTag status={detailData.activeChange.status} category="change" />
                      </div>
                      <Button
                        size="small"
                        onClick={() =>
                          navigate(`/changes/clusters/${encodeURIComponent(decodedClusterId)}/${encodeURIComponent(detailData.activeChange!.changeId)}`)
                        }
                      >
                        查看详情
                      </Button>
                    </div>
                    <div className="mt-3 grid grid-cols-2 md:grid-cols-4 gap-3 text-sm">
                      <div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">当前拓扑</div>
                        <div className="font-semibold">{detailData.activeChange.currentTopology.length} 分区</div>
                      </div>
                      <div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">期望拓扑</div>
                        <div className="font-semibold">{detailData.activeChange.expectedTopology.length} 分区</div>
                      </div>
                      <div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">计划操作</div>
                        <div className="font-semibold">{detailData.activeChange.plannedChanges.length}</div>
                      </div>
                      <div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">待执行</div>
                        <div className="font-semibold">{detailData.activeChange.pendingChanges.length} 分区</div>
                      </div>
                    </div>
                  </DetailSection>
                )}

              </div>
            ),
          },
          {
            key: 'brokers',
            label: (
              <span className="flex items-center gap-1.5">
                <Boxes size={14} /> Broker
              </span>
            ),
            children: (
              <Card className="shadow-sm">
                {noBrokerRegistered ? (
                  <Empty description="等待 Broker 注册" image={Empty.PRESENTED_IMAGE_SIMPLE}>
                    <p className="text-xs text-[hsl(var(--muted-foreground))] mt-2">
                      集群配置已创建，请启动并注册 Broker
                    </p>
                  </Empty>
                ) : (
                  <Table
                    className="broker-list-table"
                    dataSource={brokers}
                    rowKey="brokerId"
                    size="small"
                    pagination={false}
                    tableLayout="fixed"
                    scroll={{ x: 980, y: 400 }}
                    columns={[
                      {
                        title: 'Broker ID',
                        dataIndex: 'brokerId',
                        key: 'brokerId',
                        width: 240,
                        render: (v: string) => <CopyableId id={v} maxLength={20} />,
                      },
                      {
                        title: '状态',
                        dataIndex: 'status',
                        key: 'status',
                        width: 100,
                        align: 'center',
                        render: (v: string) => <StatusTag status={v} />,
                      },
                      {
                        title: 'Host:Port',
                        key: 'host',
                        width: 220,
                        render: (_, r: BrokerInfo) => <NetworkEndpoint value={formatEndpoint(r.host, r.port)} />,
                      },
                      { title: 'Raft 端口', dataIndex: 'raftPort', key: 'raftPort', width: 100, align: 'center',
                        render: (v: number) => <Tag className="number-tag font-mono-id">{v}</Tag> },
                      {
                        title: '分区数',
                        key: 'partitions',
                        width: 90,
                        align: 'center',
                        render: (_, r: BrokerInfo) => <Tag className="number-tag">{r.partitions.length}</Tag>,
                      },
                      {
                        title: '最后续约',
                        dataIndex: 'lastRenewal',
                        key: 'lastRenewal',
                        width: 140,
                        align: 'center',
                        render: (v: string) => formatDateTimeRelative(v),
                      },
                      {
                        title: '操作',
                        key: 'action',
                        width: 90,
                        align: 'center',
                        render: (_, r: BrokerInfo) => (
                          <Button size="small" type="link" onClick={() => setSelectedBroker(r)}>
                            详情
                          </Button>
                        ),
                      },
                    ]}
                  />
                )}
              </Card>
            ),
          },
          {
            key: 'partitions',
            label: (
              <span className="flex items-center gap-1.5">
                <Columns size={14} /> 分区
              </span>
            ),
            children: (
              <div className="space-y-4">
              <DetailSection title="分区归属" description="按节点查看各分区的角色，分区 ID 按数值升序排列">
                {topology ? <PartitionOwnershipMatrix topology={topology} onBrokerSelect={setSelectedBroker} /> : <Empty description="等待 Broker 注册" image={Empty.PRESENTED_IMAGE_SIMPLE} />}
              </DetailSection>
              <Collapse className="partition-details-collapse" items={[{
                key: 'details',
                label: '分区详情与高级操作',
                children: (
              <Card className="shadow-sm">
                {noBrokerRegistered ? (
                  <Empty description="等待 Broker 注册" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                ) : (
                  <Table
                    dataSource={[...partitions].sort((a, b) => a.partitionId - b.partitionId)}
                    rowKey="partitionId"
                    size="small"
                    pagination={false}
                    scroll={{ x: 800, y: 400 }}
                    columns={[
                      {
                        title: 'Partition',
                        dataIndex: 'partitionId',
                        key: 'partitionId',
                        width: 80,
                      },
                      {
                        title: 'Leader',
                        dataIndex: 'leaderId',
                        key: 'leaderId',
                        render: (v: string) => <CopyableId id={v} maxLength={16} />,
                      },
                      {
                        title: 'Term',
                        dataIndex: 'term',
                        key: 'term',
                        width: 60,
                        render: (v: string) => <span className="font-mono-id text-xs">{formatInt64(v)}</span>,
                      },
                      {
                        title: 'Replicas',
                        dataIndex: 'replicas',
                        key: 'replicas',
                        render: (v: string[]) => (
                          <div className="flex flex-wrap gap-1">
                            {v.map((id) => <Tag key={id} className="rounded-full text-xs">{id}</Tag>)}
                          </div>
                        ),
                      },
                      {
                        title: 'Learners',
                        dataIndex: 'learners',
                        key: 'learners',
                        width: 120,
                        render: (v: string[]) =>
                          v.length > 0 ? (
                            <div className="flex flex-wrap gap-1">
                              {v.map((id) => <Tag key={id} className="rounded-full text-xs" color="orange">{id}</Tag>)}
                            </div>
                          ) : <span className="text-[hsl(var(--muted-foreground))]">-</span>,
                      },
                      {
                        title: '',
                        key: 'action',
                        width: 100,
                        render: (_, r: PartitionPlacement) => (
                          <Button
                            size="small"
                            onClick={() => { setSelectedPartition(r); setRecoveryConfirmation(''); setRecoveryForm({}); setRecoveryError(null); setRecoveryOpen(true); }}
                          >
                            高级操作
                          </Button>
                        ),
                      },
                    ]}
                  />
                )}
              </Card>
                ),
              }]} />
              </div>
            ),
          },
        ]}
      />

      <Drawer
        rootClassName="console-drawer broker-config-editor"
        width="min(480px, 100vw)"
        open={editorOpen}
        title={<DrawerTitle title="修改配置" eyebrow="BROKER CONFIGURATION" icon={<Settings size={20} />} />}
        onClose={() => setEditorOpen(false)}
        footer={<div className="pd-editor-footer">
          <Button onClick={() => setEditorOpen(false)}>取消</Button>
          <Button type="primary" disabled={cannotPreview}
            onClick={() => { setEditorOpen(false); setConfirmOpen(true); }}>预览变更</Button>
        </div>}
      >
        <aside className="configuration-aside" aria-label="当前 Broker 配置">
          <h3>当前配置</h3>
          <div className="broker-config-snapshot">
            <div><span>Broker 数量</span><Tag className="broker-value-pill">{config.clusterSize}</Tag></div>
            <div><span>分区数</span><Tag className="broker-value-pill">{config.partitionSize}</Tag></div>
            <div><span>副本因子</span><Tag className="broker-value-pill">{config.replicationFactor}</Tag></div>
          </div>
          <p>选择添加 / 移除成员后自动推导目标数量；填写目标数量时需与成员选择一致。分区数只能增加。</p>
        </aside>
        <Alert message="提交后可在变更详情中查看执行进度" type="info" showIcon className="mb-6" />
        <Form layout="vertical" disabled={!!detailData.activeChange || noBrokerRegistered}>
          <Row gutter={12}>
            <Col span={12}>
              <Form.Item label="目标 Broker 数量">
                <InputNumber aria-label="目标 Broker 数量" min={1} precision={0} changeOnBlur={false}
                  placeholder={`当前 ${config.clusterSize}`} value={patchForm.brokersCount}
                  onInput={setBrokerCountInput}
                  onChange={(v) => {setPatchForm((p) => ({...p, brokersCount: v ?? undefined})); setBrokerCountInput(null);}} className="w-full" />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="分区数" validateStatus={invalidPartitionCount || partitionInputError ? 'error' : undefined}
                help={partitionInputError || (invalidPartitionCount ? `分区数只能增加，至少为 ${config.partitionSize}，最多为 511` : undefined)}>
                <InputNumber key={partitionInputRevision} aria-label="分区数" min={config.partitionSize} max={511} precision={0}
                  changeOnBlur={false}
                  placeholder={`当前 ${config.partitionSize}`} value={patchForm.partitionCount}
                  onInput={(raw) => { setPartitionInput(raw); setPartitionInputError(''); }}
                  onBlur={commitPartitionInput} onPressEnter={commitPartitionInput}
                  onChange={(v) => {
                    setPatchForm((p) => ({...p, partitionCount: v ?? undefined}));
                    setPartitionInput(null);
                    setPartitionInputError('');
                  }} className="w-full" />
              </Form.Item>
            </Col>
          </Row>
          <Form.Item label={<span className="configuration-field-label configuration-field-add"><PlusCircle size={15} />添加 Broker</span>}
            extra="可选择在线空闲节点或所有已报告分区均为 Learner 的节点，支持多选"
            validateStatus={addSelectionUnavailable ? 'error' : undefined}
            help={invalidAddList ? '所选 Broker 已不满足添加条件，请移除后重新选择'
              : addSelectionUnavailable ? '节点状态读取失败或已过时，请刷新后再提交' : undefined}>
            <Select aria-label="添加 Broker" mode="multiple" showSearch optionFilterProp="label"
              options={brokerCandidates.map(broker => ({value: broker.brokerId, label: broker.brokerId}))}
              disabled={state.stale || !!state.error}
              notFoundContent={<Empty image={Empty.PRESENTED_IMAGE_SIMPLE}
                description={brokerCandidates.length ? '没有匹配的节点' : '暂无可添加的在线备用节点'} />}
              placeholder="选择要添加的在线备用节点" value={addList}
              onChange={(values: string[]) => setPatchForm((p) => ({...p, brokersAdd: values}))} />
          </Form.Item>
          <Form.Item label={<span className="configuration-field-label configuration-field-remove"><MinusCircle size={15} />移除 Broker</span>}
            extra="选择已有节点，或输入 Broker ID">
            <Select className="configuration-removal-select" aria-label="移除 Broker" mode="tags" tokenSeparators={[',', '，']}
              options={brokers.map((broker) => ({value: broker.brokerId, label: broker.brokerId}))}
              placeholder="选择要移除的节点" value={removeList}
              onChange={(values: string[]) => setPatchForm((p) => ({...p, brokersRemove: [...new Set(values.map((id) => id.trim()).filter(Boolean))]}))} />
          </Form.Item>
          <Form.Item label="副本因子">
            <InputNumber aria-label="副本因子" min={1} precision={0} changeOnBlur={false}
              placeholder={`当前 ${config.replicationFactor}`} value={patchForm.replicationFactor}
              onInput={setReplicaInput}
              onChange={(v) => {setPatchForm((p) => ({...p, replicationFactor: v ?? undefined})); setReplicaInput(null);}} className="w-full" />
          </Form.Item>
          <div aria-live="polite" className="mb-3">
            <p className="text-xs text-[hsl(var(--muted-foreground))]">
              变更后目标 Broker：{Number.isFinite(validation.targetCount) ? validation.targetCount : '—'} · 副本因子：{Number.isFinite(validation.replicationFactor) ? validation.replicationFactor : '—'}
            </p>
            {validation.errors.length > 0 && <Alert className="configuration-validation-error" type="error" showIcon
              message="当前配置不满足变更条件"
              description={<ul className="m-0 pl-4 list-disc">{validation.errors.map(error => <li key={error}>{error}</li>)}</ul>} />}
            {snapshotUnavailable && <Alert type="error" showIcon message="配置或节点状态读取失败或已过时，请刷新后再提交" />}
          </div>
          {dangerousChange && validation.errors.length === 0 && <Alert className="configuration-editor-risk" type="warning" showIcon
            message="此变更包含高危操作" description={riskDescription} />}
          {(noBrokerRegistered || detailData.activeChange) && <Alert
            message={noBrokerRegistered ? '等待 Broker 注册后可修改配置' : '请等待当前变更完成'} type="warning" showIcon className="mb-3" />}
        </Form>
      </Drawer>

      {/* Broker Drawer */}
      <Drawer
        rootClassName="console-drawer"
        open={!!selectedBroker}
        onClose={() => setSelectedBroker(null)}
        title={<DrawerTitle title={selectedBroker?.brokerId} eyebrow="BROKER / 节点详情" icon={<Boxes size={22} />} />}
        width="min(720px, 100vw)"
        footer={`所属集群 · ${decodedClusterId}`}
      >
        {selectedBroker && (
          <div className="space-y-4">
            <div className="object-summary">
              <NetworkEndpoint value={formatEndpoint(selectedBroker.host, selectedBroker.port)} />
              <StatusTag status={selectedBroker.status} />
            </div>
            <MetricStrip items={[
              { label: '承载分区', value: selectedBroker.partitions.length, tone: 'brand' },
              { label: 'Leader 分区', value: selectedBroker.partitions.filter((p) => p.role === MemberRole.LEADER).length, tone: 'warning' },
              { label: '服务就绪', value: selectedBroker.partitions.filter((p) => p.election?.serviceReady).length, tone: 'success' },
            ]} />
            <DetailSection title="节点信息" description="连接端点、版本与注册时间">
            <Descriptions size="small" column={{ xs: 1, sm: 2 }} layout="vertical">
              <Descriptions.Item label="Broker ID">
                <CopyableId id={selectedBroker.brokerId} maxLength={30} />
              </Descriptions.Item>
              <Descriptions.Item label="Version">
                <Tag className="rounded-full text-xs">{selectedBroker.version}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="Host">{selectedBroker.host}</Descriptions.Item>
              <Descriptions.Item label="Port">{selectedBroker.port}</Descriptions.Item>
              <Descriptions.Item label="Raft Port">{selectedBroker.raftPort}</Descriptions.Item>
              <Descriptions.Item label="Registered">
                {formatDateTime(selectedBroker.registeredAt)}
              </Descriptions.Item>
              <Descriptions.Item label="Last Renewal">
                {formatDateTimeRelative(selectedBroker.lastRenewal)}
              </Descriptions.Item>
              <Descriptions.Item label="Renewal Timeout">
                {selectedBroker.renewalTimeoutMillis}ms
              </Descriptions.Item>
            </Descriptions>
            </DetailSection>

            <DetailSection title={`分区报告 · ${selectedBroker.partitions.length}`} description="角色、复制延迟与服务就绪状态">
            <Table
              dataSource={selectedBroker.partitions}
              rowKey="partitionId"
              size="small"
              pagination={false}
              scroll={{ x: 600 }}
              columns={[
                { title: 'Partition', dataIndex: 'partitionId', key: 'partitionId', width: 70 },
                {
                  title: 'Role',
                  dataIndex: 'role',
                  key: 'role',
                  width: 80,
                  render: (v: MemberRole) => (
                    <RaftRoleTag role={v} />
                  ),
                },
                {
                  title: 'Lag',
                  dataIndex: 'lag',
                  key: 'lag',
                  width: 60,
                  render: (v: string) => (
                    <span className={BigInt(v) > 0n ? 'text-[hsl(var(--warning))]' : 'text-[hsl(var(--success))]'}>
                      {v}
                    </span>
                  ),
                },
                {
                  title: 'Ready',
                  dataIndex: 'readyForPromotion',
                  key: 'readyForPromotion',
                  width: 60,
                  render: (v: boolean) =>
                    v ? <CheckCircle2 size={14} className="text-[hsl(var(--success))]" /> : <AlertCircle size={14} className="text-[hsl(var(--muted-foreground))]" />,
                },
                { title: 'Priority', dataIndex: 'priority', key: 'priority', width: 60 },
                { title: 'State', dataIndex: 'lifecycleState', key: 'lifecycleState', width: 80 },
                {
                  title: 'Service Ready',
                  key: 'serviceReady',
                  width: 80,
                  render: (_, r: BrokerPartitionReport) =>
                    r.election?.serviceReady ? (
                      <Tag color="success" className="rounded-full text-xs">就绪</Tag>
                    ) : (
                      <Tag className="rounded-full text-xs">未就绪</Tag>
                    ),
                },
              ]}
            />
            </DetailSection>
          </div>
        )}
      </Drawer>

      {/* Quorum Recovery Modal */}
      <Modal
        className="console-modal"
        width={560}
        open={recoveryOpen}
        title={
          <div className="flex items-center gap-2">
            <ShieldAlert size={16} className="text-[hsl(var(--destructive))]" />
            <span>分区 {selectedPartition?.partitionId} - Quorum Recovery</span>
          </div>
        }
        onCancel={() => { if (!submitting) {setRecoveryOpen(false); setRecoveryForm({}); setRecoveryConfirmation('');} }}
        onOk={handleRecovery}
        confirmLoading={submitting}
        okText="提交恢复"
        okButtonProps={{ danger: true, disabled: (recoveryError instanceof ApiError && recoveryError.outcomeUnknown) || !recoveryForm.fencingProof?.trim() || recoveryConfirmation !== `${decodedClusterId}/${selectedPartition?.partitionId}` }}
        cancelText="取消"
        destroyOnClose
      >
        <div className="space-y-4 pt-2">
          <MutationError error={recoveryError} />
          <Alert
            message="此操作用于已失 quorum 的场景"
            description="请确认已完成旧副本隔离，再提交 fencing proof。"
            type="warning"
            showIcon
          />

          {selectedPartition && (
            <div className="p-3 rounded-lg bg-[hsl(var(--muted))] text-sm">
              <div className="text-xs text-[hsl(var(--muted-foreground))] mb-1">分区信息</div>
              <div>Leader: <span className="font-mono-id">{selectedPartition.leaderId}</span></div>
              <div>Replicas: {selectedPartition.replicas.join(', ')}</div>
              {selectedPartition.learners.length > 0 && (
                <div>Learners: {selectedPartition.learners.join(', ')}</div>
              )}
            </div>
          )}

          <Form layout="vertical">
          <Form.Item label="Fencing Proof" required>
            <Input
              aria-label="Fencing Proof"
              placeholder="隔离凭证"
              value={recoveryForm.fencingProof || ''}
              onChange={(e) => setRecoveryForm((f) => ({ ...f, fencingProof: e.target.value }))}
            />
          </Form.Item>

          <Form.Item label="Survivor (可选)">
            <Input
              aria-label="Survivor"
              placeholder="指定幸存者 brokerId，留空由 PD 自动选择"
              value={recoveryForm.survivor || ''}
              onChange={(e) => setRecoveryForm((f) => ({ ...f, survivor: e.target.value }))}
            />
          </Form.Item>

          <Form.Item label="二次确认" required>
            <Input
              aria-label="二次确认"
              placeholder={`输入 ${decodedClusterId}/${selectedPartition?.partitionId} 确认`}
              value={recoveryConfirmation}
              onChange={(event) => setRecoveryConfirmation(event.target.value)}
            />
          </Form.Item>
          </Form>
        </div>
      </Modal>

      {/* Config Confirm */}
      <ConfirmModal
        open={confirmOpen}
        title="确认配置变更"
        content={
          <div className="space-y-3">
            <ConfigurationChangeSummary changes={changes} />
            {validation.errors.length > 0 && <Alert type="error" showIcon
              message="当前配置不满足变更条件，请返回编辑"
              description={validation.errors.join('；')} />}
            {snapshotUnavailable && <Alert type="error" showIcon message="配置或节点状态读取失败或已过时，请刷新后再提交" />}
            {addSelectionUnavailable && <Alert type="error" showIcon
              message={invalidAddList ? '所选 Broker 已不满足添加条件，请返回编辑重新选择' : '节点状态读取失败或已过时，请刷新后再提交'} />}
          </div>
        }
        confirmLabel={removeList.length ? '确认移除并提交' : dangerousChange ? '确认高危变更' : '确认提交'}
        danger={dangerousChange}
        warning={dangerousChange ? riskDescription : undefined}
        disabled={cannotPreview}
        onConfirm={handlePatch}
        onCancel={() => { setConfirmOpen(false); setEditorOpen(true); }}
      />
    </div>
  );
};
