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
 * PD 管理页面
 * 全宽成员表 + 目标/实际配置对照 + 当前变更
 * 成员变更通过独立抽屉编辑与确认
 */

import {ChangeAcceptanceOutcome, ChangeStatus, MemberOperationType} from '@/shared/api/enums';
import React, { useState, useCallback } from 'react';
import {
  Table, Tag, Button, Form, Select, InputNumber, Drawer,
  Alert, Empty,
} from 'antd';
import { useNavigate } from 'react-router-dom';
import {
  Server, Settings, ArrowRight, PlusCircle, MinusCircle,
} from 'lucide-react';
import { StatusTag } from '@/shared/ui/StatusTag';
import {RaftRoleTag} from '@/shared/ui/RaftRoleTag';
import { CopyableId } from '@/shared/ui/CopyableId';
import {NetworkEndpoint} from '@/shared/ui/NetworkEndpoint';
import { ConfirmModal } from '@/shared/ui/ConfirmModal';
import {ConfigurationChangeSummary, type ConfigurationChange} from '@/shared/ui/ConfigurationChangeSummary';
import { PageHeader, DetailSection, MetricStrip, DrawerTitle } from '@/shared/ui/DetailPrimitives';
import { formatInt64, formatEndpoint } from '@/shared/lib/format';
import {apiClient, activeChange} from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState} from '@/shared/ui/RequestState';
import type { PDTopology, PDConfiguration, PDChangeAccepted, PDChangeDetail, PDMutationRequest, PDMember } from '@/shared/api/types';

function cn(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(' ');
}

async function readPDData(signal: AbortSignal) {
  return {
    topology: await apiClient.get<PDTopology>('/pd/topology', signal),
    config: await apiClient.get<PDConfiguration>('/pd/configuration', signal),
    activeChange: await activeChange<PDChangeDetail>('/pd/changes/active', signal),
  };
}

export const PDPage: React.FC = () => {
  const navigate = useNavigate();
  const state = usePolling(useCallback(readPDData, []), {interval: 5000, enabled: true, cacheKey: 'pd'});
  const pdData = state.data;
  const [notice, setNotice] = useState('');
  const [memberForm, setMemberForm] = useState<PDMutationRequest>({});
  const [editorOpen, setEditorOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);

  const handleSubmit = async () => {
    if (!memberForm.add?.length && !memberForm.remove?.length && memberForm.count == null) return;
      if (memberForm.add?.length) {
        const config = await apiClient.get<PDConfiguration>('/pd/configuration');
        const unavailable = memberForm.add.filter((id) =>
          !config.actual.learners.includes(id) || config.actual.voters.includes(id));
        if (unavailable.length) {
          state.refresh();
          throw new Error(`所选成员已不在 Learner 列表，请重新选择：${unavailable.join('、')}`);
        }
      }
      const result = await apiClient.patch<PDChangeAccepted>('/pd/configuration', {
        ...memberForm, add: [...new Set(memberForm.add)], remove: [...new Set(memberForm.remove)],
      });
      if (result.changeId && result.changeId !== ChangeAcceptanceOutcome.NO_CHANGES) {
        setConfirmOpen(false);
        setEditorOpen(false);
        setMemberForm({});
        navigate(`/changes/pd/${encodeURIComponent(result.changeId)}`);
      } else {
        setConfirmOpen(false);
        setEditorOpen(false);
        setNotice('无需变更，已刷新配置');
        state.refresh();
      }
  };

  const addList = (memberForm.add || []).filter(Boolean);
  const removeList = (memberForm.remove || []).filter(Boolean);
  const hasIntersection = addList.some((id) => removeList.includes(id));
  const learnerIds = [...new Set(pdData?.config.actual.learners || [])]
    .filter((id) => !pdData?.config.actual.voters.includes(id));
  const unavailableAdds = addList.filter((id) => !learnerIds.includes(id));
  const invalidAddMessage = unavailableAdds.length
    ? `所选成员已不在 Learner 列表，请重新选择：${unavailableAdds.join('、')}` : undefined;
  if (!pdData) return <RequestState state={state} />;
  const reducingCount = memberForm.count != null && memberForm.count < pdData.config.target.count;
  const dangerousChange = removeList.length > 0 || reducingCount;
  const riskDescription = '移除成员或缩减成员数量可能触发 Leader 切换；在线投票成员不足时，PD 控制平面将无法正常提供服务。请确认剩余投票成员在线，并满足多数派要求。';
  const changes: ConfigurationChange[] = [];
  if (addList.length) changes.push({kind: 'add', label: 'PD 成员', members: addList});
  if (removeList.length) changes.push({kind: 'remove', label: 'PD 成员', members: removeList});
  if (memberForm.count != null) changes.push({kind: 'update', label: '目标成员数量',
    before: pdData.config.target.count, after: memberForm.count, danger: reducingCount});

  return (
    <div className="pd-page space-y-6">
      <RequestState state={state} />
      {notice && <Alert message={notice} type="info" />}
      {/* 页头 */}
      <PageHeader title="PD 管理" icon={<Server size={22} />}
        subtitle={`控制平面 · Leader ${pdData.topology.leaderId}`}
        actions={<Button type="primary" disabled={!!pdData.activeChange} icon={<Settings size={14} />} onClick={() => setEditorOpen(true)}>修改配置</Button>} />
      <MetricStrip items={[
        { label: '成员数', value: pdData.config.actual.voters.length, tone: 'brand' },
        { label: '在线成员', value: pdData.topology.members.filter((member) => member.status?.toLowerCase() === 'online').length, tone: 'success' },
        { label: 'Raft Term', value: pdData.config.actual.term },
        { label: '配置 Epoch', value: pdData.config.epoch },
      ]} />

      <DetailSection
        className="pd-members-panel"
        title="成员列表"
        description="节点角色、连接地址与 Raft 复制状态"
        extra={<Tag>{pdData.topology.members.length} 个成员</Tag>}
      >
            <Table
              dataSource={pdData.topology.members}
              rowKey="memberId"
              size="small"
              pagination={false}
              scroll={{ x: 940 }}
              columns={[
                {
                  title: '成员节点',
                  dataIndex: 'memberId',
                  key: 'memberId',
                  width: 180,
                  render: (v: string, member: PDMember) => (
                    <div className="pd-member-identity">
                      <CopyableId id={v} maxLength={24} />
                      {member.self && <span className="pd-observer-label">当前节点</span>}
                    </div>
                  ),
                },
                {
                  title: '角色',
                  dataIndex: 'role',
                  key: 'role',
                  width: 100,
                  render: (v: PDMember['role']) => <RaftRoleTag role={v} />,
                },
                {
                  title: '状态',
                  dataIndex: 'status',
                  key: 'status',
                  width: 90,
                  render: (v: string) => (
                    <span className="flex items-center gap-1 text-xs">
                      <span className={cn(
                        'w-1.5 h-1.5 rounded-full',
                        v?.toLowerCase() === 'online' ? 'bg-[hsl(var(--success))] status-pulse' : 'bg-[hsl(var(--muted-foreground))]'
                      )} />
                      {v?.toLowerCase() === 'online' ? '在线' : v || '未知'}
                    </span>
                  ),
                },
                {
                  title: '连接地址',
                  dataIndex: 'httpPort',
                  key: 'endpoint',
                  width: 230,
                  render: (v: number, r: PDMember) => (
                    <div className="pd-node-endpoint">
                      <NetworkEndpoint value={r.host && v ? formatEndpoint(r.host, v) : r.endpoint || '—'} />
                      <span className="pd-secondary-line">HTTP · gRPC <span className="font-mono-id">{r.grpcPort}</span> · Raft <span className="font-mono-id">{r.raftPort}</span></span>
                    </div>
                  ),
                },
                {
                  title: 'Raft 复制进度',
                  key: 'progress',
                  width: 220,
                  render: (_, r: PDMember) => (
                    <div className="pd-replication-progress">
                      <span><span className="pd-secondary-line">Commit</span><span className="font-mono-id">{formatInt64(r.commitIndex)}</span></span>
                      <span><span className="pd-secondary-line">Applied</span><span className="font-mono-id">{formatInt64(r.lastApplied)}</span></span>
                    </div>
                  ),
                },
                {
                  title: '提升状态',
                  key: 'ready',
                  width: 120,
                  render: (_, member: PDMember) => {
                    // Promotion only applies to Learners. The heartbeat boolean is not
                    // maintained by the PD executor; use actual membership and change progress.
                    if (pdData.config.actual.voters.includes(member.memberId)) {
                      return <Tag className="pd-promotion-pill pd-promotion-complete" title="已是 Voter，无需从 Learner 提升">无需提升</Tag>;
                    }
                    if (!pdData.config.actual.learners.includes(member.memberId)) return '—';
                    const change = pdData.activeChange;
                    const operation = change?.currentOperation;
                    if (change && operation?.memberId === member.memberId && operation.type === MemberOperationType.MEMBER_PROMOTE_VOTER) {
                      const catchingUp = change.status === ChangeStatus.WAITING_CATCH_UP;
                      return <Tag className={`pd-promotion-pill ${catchingUp ? 'pd-promotion-catching-up' : 'pd-promotion-progress'}`}>
                        {catchingUp ? '追平中' : '提升中'}
                      </Tag>;
                    }
                    return <Tag className="pd-promotion-pill pd-promotion-waiting" title="当前是 Learner，尚未执行提升操作">等待提升</Tag>;
                  },
                },
              ]}
            />
      </DetailSection>

      <div className="pd-context-grid">
          <DetailSection title="配置详情" description="对照目标成员配置与当前 Raft 状态" extra={<Tag>Epoch {pdData.config.epoch}</Tag>}>
            <div className="pd-configuration-compare">
              <section className="pd-config-snapshot" aria-label="目标配置">
                <div className="pd-snapshot-heading"><span className="pd-snapshot-marker" /><h3>目标配置</h3></div>
                <dl className="pd-key-values">
                  <div><dt>成员数量</dt><dd>{pdData.config.target.count}</dd></div>
                  <div><dt>Voters</dt><dd className="pd-member-tags">{pdData.config.target.voters.map((id) => <Tag key={id} color="blue">{id}</Tag>)}</dd></div>
                </dl>
              </section>
              <section className="pd-config-snapshot" aria-label="实际状态">
                <div className="pd-snapshot-heading"><span className="pd-snapshot-marker pd-snapshot-actual" /><h3>实际状态</h3></div>
                <dl className="pd-key-values">
                  <div><dt>Leader / Term</dt><dd className="pd-leader-term"><Tag className="leader-tag">{pdData.config.actual.leaderId}</Tag><span className="font-mono-id">{pdData.config.actual.term}</span></dd></div>
                  <div><dt>Voters</dt><dd className="pd-member-tags">{pdData.config.actual.voters.map((id) => <Tag key={id} color="success">{id}</Tag>)}</dd></div>
                  <div><dt>Learners</dt><dd className="pd-member-tags">{pdData.config.actual.learners.length
                    ? pdData.config.actual.learners.map((id) => <Tag key={id}>{id}</Tag>)
                    : <span className="pd-secondary-line">无</span>}</dd></div>
                </dl>
              </section>
            </div>
            <div className="pd-panel-footer">
              <span>关联变更</span>
              {pdData.config.activeChangeId
                ? <Button type="link" size="small" aria-label="查看当前变更" onClick={() => navigate(`/changes/pd/${pdData.config.activeChangeId}`)}>{pdData.config.activeChangeId}<ArrowRight size={13} /></Button>
                : <span className="pd-secondary-line">无</span>}
            </div>
          </DetailSection>

          <DetailSection title="当前变更" description="成员调整的执行进度与配置演进"
            extra={pdData.activeChange && <StatusTag status={pdData.activeChange.status} category="change" />}>
            {pdData.activeChange ? (
              <div className="pd-change-content">
                <div className="pd-change-identity">
                  <CopyableId id={pdData.activeChange.changeId} maxLength={40} />
                  <Button type="link" size="small" onClick={() => navigate(`/changes/pd/${pdData.activeChange!.changeId}`)}>查看详情<ArrowRight size={13} /></Button>
                </div>
                <div className="pd-epoch-flow" aria-label="配置版本演进">
                  <div><span>Source Epoch</span><strong className="font-mono-id">{pdData.activeChange.sourceEpoch}</strong></div>
                  <ArrowRight size={16} />
                  <div><span>Target Epoch</span><strong className="font-mono-id">{pdData.activeChange.targetEpoch}</strong></div>
                </div>

                {pdData.activeChange.currentOperation && (
                  <div className="pd-current-operation">
                    <span className="pd-secondary-line">当前操作</span>
                    <div className="pd-operation-summary">
                      <Tag>{pdData.activeChange.currentOperation.type}</Tag>
                      <span className="font-mono-id">{pdData.activeChange.currentOperation.memberId}</span>
                      <StatusTag status={pdData.activeChange.currentOperation.status} />
                    </div>
                  </div>
                )}

                <dl className="pd-key-values pd-change-members">
                  <div><dt>目标 Voters</dt><dd className="pd-member-tags">{pdData.activeChange.targetVoters.map((id) => <Tag key={id} color="blue">{id}</Tag>)}</dd></div>
                  <div><dt>实际 Voters</dt><dd className="pd-member-tags">{pdData.activeChange.actualVoters.map((id) => <Tag key={id} color="success">{id}</Tag>)}</dd></div>
                  <div><dt>Learners</dt><dd className="pd-member-tags">{pdData.activeChange.actualLearners.length
                    ? pdData.activeChange.actualLearners.map((id) => <Tag key={id}>{id}</Tag>)
                    : <span className="pd-secondary-line">无</span>}</dd></div>
                </dl>
              </div>
            ) : (
              <Empty description="暂无当前变更" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </DetailSection>
      </div>

      <Drawer
        className="console-drawer pd-member-editor"
        width={480}
        open={editorOpen}
        title={<DrawerTitle title="修改配置" eyebrow="PD CONFIGURATION" icon={<Settings size={20} />} />}
        onClose={() => setEditorOpen(false)}
        footer={<div className="pd-editor-footer">
          <Button onClick={() => setEditorOpen(false)}>取消</Button>
          <Button type="primary" disabled={hasIntersection || !!invalidAddMessage || (!addList.length && !removeList.length && memberForm.count == null)}
            onClick={() => { setEditorOpen(false); setConfirmOpen(true); }}>预览变更</Button>
        </div>}
      >
          <Alert message="提交后可在变更详情中查看执行进度" type="info" showIcon className="mb-6" />
            <Form layout="vertical">
              <Form.Item
                label={<span className="configuration-field-label configuration-field-add"><PlusCircle size={15} />添加成员</span>}
                extra={learnerIds.length ? '选择 Learner 成员，提交后提升为 Voter' : '请先启动新节点，等待其加入 Learner 列表'}
                validateStatus={invalidAddMessage ? 'error' : undefined}
                help={invalidAddMessage}
              >
                <Select
                  aria-label="添加成员"
                  mode="multiple"
                  showSearch
                  allowClear
                  optionFilterProp="label"
                  options={learnerIds.map((id) => ({value: id, label: id}))}
                  disabled={!learnerIds.length && !addList.length}
                  placeholder={learnerIds.length ? '选择要添加的 Learner' : '暂无可添加的 Learner'}
                  value={memberForm.add || []}
                  onChange={(values: string[]) => setMemberForm((p) => ({ ...p, add: values }))}
                />
              </Form.Item>
              <Form.Item
                label={<span className="configuration-field-label configuration-field-remove"><MinusCircle size={15} />移除成员</span>}
                extra="选择已有成员，或输入成员 ID"
              >
                <Select
                  className="configuration-removal-select"
                  aria-label="移除成员"
                  mode="tags"
                  tokenSeparators={[',', '，']}
                  options={pdData.topology.members.map((member) => ({ value: member.memberId, label: member.memberId }))}
                  placeholder="选择要移除的成员"
                  value={memberForm.remove || []}
                  onChange={(values: string[]) => setMemberForm((p) => ({ ...p, remove: values.map((id) => id.trim()).filter(Boolean) }))}
                />
              </Form.Item>
              <Form.Item label={<span className="text-xs">目标数量</span>}>
                <InputNumber
                  min={1}
                  precision={0}
                  aria-label="目标数量"
                  placeholder="正整数"
                  value={memberForm.count}
                  onChange={(v) => setMemberForm((p) => ({ ...p, count: v || undefined }))}
                  className="w-full"
                />
              </Form.Item>

              {hasIntersection && (
                <Alert
                  message="添加和移除列表存在交集"
                  type="error"
                  showIcon
                  className="mb-3 text-xs"
                />
              )}
              {dangerousChange && <Alert className="configuration-editor-risk" type="warning" showIcon
                message="此变更包含高危操作" description={riskDescription} />}
            </Form>
      </Drawer>

      {/* 确认弹窗 */}
      <ConfirmModal
        open={confirmOpen}
        title="确认 PD 配置变更"
        content={<>
          <ConfigurationChangeSummary changes={changes} />
          {invalidAddMessage && <Alert className="mt-3" type="error" showIcon message={invalidAddMessage} />}
        </>}
        confirmLabel={removeList.length ? '确认移除并提交' : dangerousChange ? '确认高危变更' : '确认提交'}
        danger={dangerousChange}
        warning={dangerousChange ? riskDescription : undefined}
        disabled={hasIntersection || !!invalidAddMessage}
        onConfirm={handleSubmit}
        onCancel={() => { setConfirmOpen(false); setEditorOpen(true); }}
      />
    </div>
  );
};
