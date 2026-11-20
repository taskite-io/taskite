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
import {Descriptions} from 'antd';
import {apiClient, ApiError} from '@/shared/api/client';
import type {DistributionDetail, TimerSubscription} from '@/shared/api/types';
import {ConfirmModal} from '@/shared/ui/ConfirmModal';
import {CopyableId} from '@/shared/ui/CopyableId';
import {useNavigate} from 'react-router-dom';
import {useBootstrap} from '@/shared/ui/ConsoleBootstrap';
import {rememberChange} from '@/shared/api/history';

type Target = {kind: 'job'; subscription: DistributionDetail} | {kind: 'timer'; subscription: TimerSubscription};

export function DeleteSubscriptionModal({target, disabled, onClose, onDeleted}: {
  target: Target | null; disabled: boolean; onClose: () => void; onDeleted: () => void;
}) {
  const navigate = useNavigate();
  const {deploymentId} = useBootstrap();
  if (!target) return null;
  const subscription = target.subscription;
  const key = target.kind === 'job' ? target.subscription.distributionKey : target.subscription.timerSubscriptionKey;
  const confirm = async () => {
    const scope = new URLSearchParams({clusterId: subscription.clusterId, tenantId: subscription.tenantId});
    try {
      const accepted = await apiClient.delete<{distributionKey: string}>(
        `/broker/${target.kind === 'job' ? 'job' : 'timer'}-subscriptions/${encodeURIComponent(key)}?${scope}`);
      if (target.kind === 'job') {
        rememberChange(deploymentId, {scope: 'subscription', clusterId: subscription.clusterId,
          changeId: accepted.distributionKey});
      }
      onDeleted();
      if (target.kind === 'job') navigate(`/changes/subscriptions/${encodeURIComponent(accepted.distributionKey)}`);
    } catch (error) {
      if (error instanceof ApiError && error.status >= 500) {
        throw new ApiError('删除结果尚未确认，请关闭弹窗并刷新任务列表后确认实际状态。',
          error.status, error.code, [], error.leaderEndpoint, true);
      }
      throw error;
    }
  };
  return <ConfirmModal open danger disabled={disabled}
    title={`删除${target.kind === 'job' ? '普通' : '定时'}任务`}
    confirmLabel="确认删除" onConfirm={confirm} onCancel={onClose}
    content={<div className="space-y-3">
      <Descriptions size="small" column={1}>
        <Descriptions.Item label="名称">{subscription.name || (target.kind === 'job' ? target.subscription.jobType : '—')}</Descriptions.Item>
        <Descriptions.Item label="订阅 Key"><CopyableId id={key} maxLength={24} /></Descriptions.Item>
        <Descriptions.Item label="所属集群">{subscription.clusterId}</Descriptions.Item>
        <Descriptions.Item label="所属租户">{subscription.tenantId}</Descriptions.Item>
      </Descriptions>
      <p>{target.kind === 'job'
        ? '删除将异步分发到源集群及 Mirror 目标集群，关联的 Job、Incident 和变量也会清理。'
        : '删除将停止后续调度，已创建或已发出的 Job 不会撤回。'}</p>
    </div>} />;
}
