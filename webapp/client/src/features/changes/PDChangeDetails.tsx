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
import React from 'react';
import {Alert, Empty, Table, Tag} from 'antd';
import {ArrowRight} from 'lucide-react';
import {CopyableId} from '@/shared/ui/CopyableId';
import {DetailSection} from '@/shared/ui/DetailPrimitives';
import {StatusTag} from '@/shared/ui/StatusTag';
import {formatInt64} from '@/shared/lib/format';
import type {PDChangeDetail} from '@/shared/api/types';

function OperationTag({type}: {type: string}) {
  const presentation = ({
    MEMBER_ADD_LEARNER: {label: '添加 Learner', color: 'blue'},
    MEMBER_PROMOTE_VOTER: {label: '提升为 Voter', color: 'success'},
    MEMBER_REMOVE: {label: '移除成员', color: 'error'},
  } as Record<string, {label: string; color: string}>)[type];
  return <Tag title={type} color={presentation?.color} className="pd-operation-type">
    {presentation?.label ?? type}
  </Tag>;
}

function MemberTags({members}: {members: string[]}) {
  return <div className="pd-member-tags">
    {members.length ? members.map(id => <Tag key={id} className="pd-change-member"><CopyableId id={id} maxLength={28} /></Tag>)
      : <span className="pd-secondary-line">无</span>}
  </div>;
}

export function PDChangeDetails({data}: {data: PDChangeDetail}) {
  const current = data.currentOperation;
  return <div className="pd-change-details space-y-4">
    <DetailSection title="变更信息" className="pd-change-information">
      <dl className="pd-change-metadata">
        <div><dt>变更 ID</dt><dd><CopyableId id={data.changeId} maxLength={48} /></dd></div>
        <div><dt>配置版本</dt><dd className="pd-change-epoch" aria-label="配置版本演进">
          <span><span>Source Epoch</span><Tag className="pd-change-value">{formatInt64(data.sourceEpoch)}</Tag></span>
          <ArrowRight size={16} aria-hidden="true" />
          <span><span>Target Epoch</span><Tag className="pd-change-value">{formatInt64(data.targetEpoch)}</Tag></span>
        </dd></div>
      </dl>
    </DetailSection>

    <div className={`pd-change-context${current ? ' has-current-operation' : ''}`}>
      <DetailSection title="成员配置对比" description="目标配置保留原始计划，实际状态随执行更新" className="pd-change-membership">
        <div className="pd-change-snapshots">
          <section className="pd-change-snapshot" aria-label="目标成员配置">
            <div className="pd-snapshot-heading"><span className="pd-snapshot-marker" /><h3>目标配置</h3><span>原始计划</span></div>
            <dl className="pd-change-fields">
              <div><dt>成员数量</dt><dd><Tag className="pd-change-value">{data.targetCount}</Tag></dd></div>
              <div><dt>Voters</dt><dd><MemberTags members={data.targetVoters} /></dd></div>
            </dl>
          </section>
          <section className="pd-change-snapshot" aria-label="实际成员状态">
            <div className="pd-snapshot-heading"><span className="pd-snapshot-marker" /><h3>实际状态</h3><span>执行快照</span></div>
            <dl className="pd-change-fields">
              <div><dt>Voters <span className="pd-change-role-count">{data.actualVoters.length}</span></dt><dd><MemberTags members={data.actualVoters} /></dd></div>
              <div><dt>Learners <span className="pd-change-role-count">{data.actualLearners.length}</span></dt><dd><MemberTags members={data.actualLearners} /></dd></div>
            </dl>
          </section>
        </div>
      </DetailSection>

      {current && <DetailSection title="当前操作" description="执行器当前处理的成员操作" className="pd-change-current">
        <dl className="pd-change-fields">
          <div><dt>操作类型</dt><dd><OperationTag type={current.type} /></dd></div>
          <div><dt>成员节点</dt><dd><CopyableId id={current.memberId} maxLength={28} /></dd></div>
          <div><dt>执行状态</dt><dd><StatusTag status={current.status} category="change" /></dd></div>
          <div><dt>追平屏障</dt><dd>{current.barrierIndex != null
            ? <Tag className="pd-change-value">{formatInt64(current.barrierIndex)}</Tag>
            : <span className="pd-secondary-line">—</span>}</dd></div>
        </dl>
        {current.error && <Alert className="pd-change-operation-error" type="error" showIcon message={current.error} />}
      </DetailSection>}
    </div>

    <DetailSection title="操作计划" description="成员操作顺序与各步骤的执行状态" className="pd-change-operations"
      extra={<span className="pd-change-plan-count"><Tag className="pd-change-value">{data.operations.length}</Tag>项操作</span>}>
      {data.operations.length ? <Table
        dataSource={data.operations}
        rowKey={row => `${row.type}-${row.memberId}`}
        size="small"
        pagination={false}
        tableLayout="fixed"
        scroll={{x: 800}}
        columns={[
          {title: '操作类型', dataIndex: 'type', key: 'type', width: 180,
            render: (value: string) => <OperationTag type={value} />},
          {title: '成员节点', dataIndex: 'memberId', key: 'memberId', width: 220,
            render: (value: string) => <CopyableId id={value} maxLength={28} />},
          {title: '执行状态', dataIndex: 'status', key: 'status', width: 140, align: 'center',
            render: (value: string) => <StatusTag status={value} category="change" />},
          {title: '追平屏障', dataIndex: 'barrierIndex', key: 'barrierIndex', width: 150, align: 'center',
            render: (value?: string) => value != null ? <Tag className="pd-change-value">{formatInt64(value)}</Tag> : '—'},
          {title: '错误信息', dataIndex: 'error', key: 'error', width: 220,
            render: (value?: string) => value ? <span className="pd-change-error-text">{value}</span> : '—'},
        ]}
      /> : <Empty description="暂无操作计划" image={Empty.PRESENTED_IMAGE_SIMPLE} />}
    </DetailSection>
  </div>;
}
