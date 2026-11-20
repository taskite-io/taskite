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
 * 任务订阅详情与持久化分发进度
 */

import {DistributionStatus} from '@/shared/api/enums';
import React, { useState, useCallback, useEffect } from 'react';
import { Button, Descriptions, Tag, Empty, Skeleton, Alert, Progress, App } from 'antd';
import { useParams, useNavigate, useSearchParams } from 'react-router-dom';
import { Route, RefreshCw, CheckCircle2, Info } from 'lucide-react';
import { CopyableId } from '@/shared/ui/CopyableId';
import { PageHeader, DetailSection, MetricStrip } from '@/shared/ui/DetailPrimitives';
import { StatusTag } from '@/shared/ui/StatusTag';
import { apiClient, ApiError } from '@/shared/api/client';
import {usePolling} from '@/shared/hooks/usePolling';
import {RequestState} from '@/shared/ui/RequestState';
import {ConfirmModal} from '@/shared/ui/ConfirmModal';
import type { DistributionDetail, JobSubscriptionRedistributionAccepted } from '@/shared/api/types';
import {SubscriptionText} from './SubscriptionText';
import {copyText} from '@/shared/lib/clipboard';
import {useBootstrap} from '@/shared/ui/ConsoleBootstrap';
import {rememberChange} from '@/shared/api/history';

export const DistributionPage: React.FC<{change?: boolean}> = ({change = false}) => {
  const { distributionKey } = useParams<{ distributionKey: string }>();
  const decodedKey = distributionKey || '';
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const {message} = App.useApp();
  const {deploymentId} = useBootstrap();
  const [confirmation, setConfirmation] = useState<DistributionDetail | null>(null);
  const state = usePolling(useCallback((signal: AbortSignal) => apiClient.get<DistributionDetail>(`/broker/job-subscriptions/${encodeURIComponent(decodedKey)}`, signal), [decodedKey]), {
    interval: 3000, enabled: !!decodedKey,
    cacheKey: JSON.stringify(['distribution', decodedKey]),
  });
  const {data: detail, loading} = state;
  const isDeletion = detail?.valueType === 'JOB_SUBSCRIPTION_DELETION';
  useEffect(() => {
    if (detail && (isDeletion || change) && !state.stale && !state.error) {
      rememberChange(deploymentId, {scope: 'subscription', clusterId: detail.sourceClusterId,
        changeId: detail.distributionKey});
    }
  }, [deploymentId, isDeletion, change, detail?.distributionKey, detail?.sourceClusterId, state.stale, state.error]);
  const requestedCluster = params.get('assetClusterId');
  const assetClusterId = detail && requestedCluster &&
    (requestedCluster === detail.sourceClusterId || detail.targets.some(t => t.clusterId === requestedCluster))
    ? requestedCluster : detail?.clusterId;
  const backParams = new URLSearchParams(params);
  backParams.delete('assetClusterId');
  const subscriptionContent = detail?.subscription ? JSON.stringify(detail.subscription, null, 2) : '';
  const canRedistribute = !change && detail?.valueType === 'JOB_SUBSCRIPTION' &&
    [DistributionStatus.COMPLETED, DistributionStatus.DISTRIBUTING].includes(detail.status) && detail.targets.length > 0 && !state.stale && !state.error;
  const redistribute = async () => {
    if (!confirmation || !canRedistribute || detail?.generation !== confirmation.generation) return;
    try {
      const accepted = await apiClient.post<JobSubscriptionRedistributionAccepted>(
        `/broker/job-subscriptions/${encodeURIComponent(decodedKey)}/redistribute`,
        {expectedGeneration: confirmation.generation});
      setConfirmation(null);
      message.success(`已受理重新下发 · 第 ${accepted.generation} 轮，请查看分区确认进度`);
    } catch (error) {
      if (error instanceof ApiError && error.outcomeUnknown) {
        throw new ApiError('重新下发结果未知，请关闭确认框并刷新任务订阅轮次与分区进度后再决定是否重试。',
          error.status, error.code, [], error.leaderEndpoint, true);
      }
      throw error;
    } finally {
      state.refresh();
    }
  };

  return (
    <div className="resource-detail-page space-y-6">
      <PageHeader title={isDeletion ? '删除任务订阅' : change ? '创建任务订阅' : '任务订阅详情'} icon={<Route size={22} />}
        subtitle={detail?.name || `分发 Key · ${decodedKey}`}
        badges={detail && <StatusTag status={detail.status} />}
        onBack={() => navigate(change || isDeletion ? '/changes?tab=subscription'
          : `/subscriptions/distributions${backParams.size ? `?${backParams}` : ''}`)}
        actions={<><Button aria-label="刷新" onClick={state.refresh}>刷新</Button>
          {!isDeletion && !change && <Button type="primary" icon={<RefreshCw size={14} />} disabled={!canRedistribute}
            onClick={() => setConfirmation(detail)}>重新下发</Button>}</>} />
      <RequestState state={{...state, loading: false}} />
      {detail && !isDeletion && [DistributionStatus.DELETING, DistributionStatus.DELETED].includes(detail.status) &&
        <Alert className="distribution-notice" type="info" showIcon icon={<Info size={16} />}
          message="该任务订阅已删除或正在删除，以下为创建分发记录。"
          action={detail.deletionDistributionKey && <Button size="small" onClick={() =>
            navigate(`/changes/subscriptions/${encodeURIComponent(detail.deletionDistributionKey!)}`)}>查看删除进度</Button>} />}
      {isDeletion && <Alert className="distribution-notice"
        type={detail?.status === DistributionStatus.COMPLETED ? 'success' : 'info'} showIcon
        icon={detail?.status === DistributionStatus.COMPLETED ? <CheckCircle2 size={16} /> : <Info size={16} />}
        message={detail?.status === DistributionStatus.COMPLETED ? '删除分发已完成，源集群及 Mirror 目标分区均已确认。'
          : '正在向源集群及 Mirror 目标分区分发删除命令，等待分区确认。'} />}

      {loading && !detail ? (
        <Skeleton active paragraph={{ rows: 3 }} />
      ) : detail ? (
        <div className="space-y-4">
          {/* 基本信息 */}
          <MetricStrip items={[
            { label: '目标集群', value: detail.targets.length, tone: 'brand' },
            { label: '已完成目标', value: detail.targets.filter((target) => target.status === DistributionStatus.COMPLETED).length, tone: 'success' },
            { label: '已确认分区', value: detail.targets.reduce((sum, target) => sum + target.acknowledgedPartitions, 0) },
            { label: '待确认分区', value: detail.targets.reduce((sum, target) => sum + target.pendingPartitions.length, 0), tone: 'warning' },
          ]} />
          <DetailSection title="任务订阅信息" className="resource-information">
            <Descriptions size="small" column={{ xs: 1, sm: 1, md: 2, xl: 3 }} layout="horizontal">
              <Descriptions.Item label={isDeletion ? '变更 Key' : '分发 Key'}>
                <CopyableId id={detail.distributionKey} maxLength={30} />
              </Descriptions.Item>
              {isDeletion && detail.subscriptionKey && <Descriptions.Item label="原任务订阅">
                <CopyableId id={detail.subscriptionKey} maxLength={30}
                  to={`/subscriptions/distributions/${encodeURIComponent(detail.subscriptionKey)}`} />
              </Descriptions.Item>}
              <Descriptions.Item label="状态">
                <StatusTag status={detail.status} />
              </Descriptions.Item>
              <Descriptions.Item label="所属集群">
                <Button type="link" className="resource-cluster-link p-0" title={assetClusterId}
                  onClick={() => navigate(`/clusters/${encodeURIComponent(assetClusterId!)}`)}><span className="truncate">{assetClusterId}</span></Button>
              </Descriptions.Item>
              <Descriptions.Item label="订阅来源">
                <Tag className="resource-pill">
                  {assetClusterId === detail.sourceClusterId ? '创建' : 'Mirror'}
                </Tag>
              </Descriptions.Item>
              <Descriptions.Item label="源集群"><SubscriptionText value={detail.sourceClusterId} /></Descriptions.Item>
              <Descriptions.Item label="任务订阅类型">
                <Tag className="resource-pill">普通任务订阅</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="任务订阅名称"><SubscriptionText value={detail.name} /></Descriptions.Item>
              <Descriptions.Item label="任务类型">
                <SubscriptionText value={detail.jobType} tone="job" />
              </Descriptions.Item>
              <Descriptions.Item label="租户"><SubscriptionText value={detail.tenantId} tone="tenant" /></Descriptions.Item>
              <Descriptions.Item label="分发轮次"><CopyableId id={detail.generation} /></Descriptions.Item>
              {!isDeletion && <Descriptions.Item label="最近重新下发">
                {detail.lastRedistributedAt === '0' ? '尚未重新下发' : new Date(Number(detail.lastRedistributedAt)).toLocaleString()}
              </Descriptions.Item>}
            </Descriptions>
          </DetailSection>

          {/* 目标 ACK */}
          <DetailSection title={`${isDeletion ? '删除确认状态' : '目标确认状态'} · ${detail.targets.length}`}
            description={isDeletion ? '每个目标集群独立记录删除命令的分区 ACK 与待确认分区'
              : '每个目标集群独立记录创建分发的分区 ACK 与待确认分区'}>
            {detail.targets.length > 0 ? (
              <div className="space-y-4">
                {detail.targets.map((t) => {
                  const inconsistent = t.acknowledgedPartitions > t.expectedPartitions || t.acknowledgedPartitions + t.pendingPartitions.length !== t.expectedPartitions;
                  const pct = t.expectedPartitions > 0
                    ? Math.round((t.acknowledgedPartitions / t.expectedPartitions) * 100)
                    : 0;
                  const done = !inconsistent && t.status === DistributionStatus.COMPLETED && t.acknowledgedPartitions === t.expectedPartitions && t.expectedPartitions > 0;

                  return (
                    <div
                      key={t.clusterId}
                      className="distribution-target"
                    >
                      <div className="distribution-target-header">
                        <div className="distribution-target-labels">
                          <Button type="link" className="resource-cluster-link p-0"
                            onClick={() => navigate(`/clusters/${encodeURIComponent(t.clusterId)}`)}><SubscriptionText value={t.clusterId} /></Button>
                          <Tag className="resource-pill">{t.clusterId === detail.sourceClusterId ? '源集群' : 'Mirror 目标'}</Tag>
                          <StatusTag status={t.status} />
                          {t.mirrorKey && <Button type="link" size="small"
                            onClick={() => navigate(`/subscriptions/mirrors/${t.mirrorKey}`)}>查看 Mirror</Button>}
                        </div>
                        <span className="text-sm font-semibold whitespace-nowrap">
                          {t.acknowledgedPartitions} / {t.expectedPartitions}
                        </span>
                      </div>

                      {t.expectedPartitions > 0 ? (
                        <Progress
                          percent={Math.min(100, pct)}
                          size="small"
                          status={done ? 'success' : 'active'}
                          trailColor="hsl(var(--border))"
                          strokeColor={done ? 'hsl(var(--success))' : 'hsl(var(--primary))'}
                        />
                      ) : (
                        <div className="text-xs text-[hsl(var(--muted-foreground))]">无期望分区</div>
                      )}
                      {inconsistent && <Alert type="warning" message="计数不一致，请核对原始状态" />}
                      <div className="mt-2 flex flex-wrap items-center gap-1">
                        <span className="text-xs text-[hsl(var(--muted-foreground))]">已确认分区：</span>
                        {t.confirmedPartitionIds.length ? t.confirmedPartitionIds.map(id =>
                          <Tag className="resource-pill" key={id}>{id}</Tag>) : <span className="text-xs">暂无</span>}
                      </div>

                      {t.pendingPartitions.length > 0 && (
                        <div className="mt-2 flex flex-wrap items-center gap-2">
                          <span className="text-xs text-[hsl(var(--muted-foreground))]">
                            待确认 {t.pendingPartitions.length} 个分区：
                          </span>
                          <Tag className="resource-pill"><CopyableId id={t.pendingPartitions.join(', ')} maxLength={30} /></Tag>
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : (
              <Empty description="无目标集群" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            )}
          </DetailSection>
          <DetailSection title="任务订阅原文" description="任务订阅创建参数"
            extra={subscriptionContent && <Button size="small" onClick={async () => {
              try { await copyText(subscriptionContent!); message.success('已复制任务订阅原文'); }
              catch { message.error('复制失败，请手动选择原文'); }
            }}>复制原文</Button>}>
            {subscriptionContent ? <pre className="rounded-lg p-4 text-xs bg-[hsl(var(--muted))]"
              style={{overflow: 'auto', maxHeight: 480, whiteSpace: 'pre'}}>{subscriptionContent}</pre>
              : <Empty description="无任务订阅原文" image={Empty.PRESENTED_IMAGE_SIMPLE} />}
          </DetailSection>
        </div>
      ) : (
        !state.error && <Empty description="未找到任务订阅" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      )}
      <ConfirmModal open={!!confirmation} title="重新下发任务订阅" confirmLabel="确认下发"
        disabled={!canRedistribute || detail?.generation !== confirmation?.generation}
        content={<div className="space-y-3"><p>将使用原任务订阅内容和原分发 Key，下发至以下目标集群，并重新确认全部分区。</p>
          <Descriptions column={1} size="small">
            <Descriptions.Item label="任务订阅">{confirmation?.name || '—'}</Descriptions.Item>
            <Descriptions.Item label="分发 Key"><CopyableId id={confirmation?.distributionKey || ''} /></Descriptions.Item>
            <Descriptions.Item label="目标集群">{confirmation?.targets.map(t => t.clusterId).join('、')}</Descriptions.Item>
          </Descriptions>
          {confirmation && detail?.generation !== confirmation.generation && <Alert type="warning" message="任务订阅轮次已变化，请关闭后重新确认。" />}
        </div>}
        onCancel={() => setConfirmation(null)} onConfirm={redistribute} />
    </div>
  );
};
