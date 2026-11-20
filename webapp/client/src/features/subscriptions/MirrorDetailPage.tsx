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
 * Mirror 详情页面
 * 按 mirrorKey 查询完整信息
 */

import {JobSubscriptionCreationType} from '@/shared/api/enums';
import React, { useCallback } from 'react';
import { Descriptions, Tag, Button, Empty } from 'antd';
import { useParams, useNavigate } from 'react-router-dom';
import { HardDrive, ArrowRight, Server } from 'lucide-react';
import { CopyableId } from '@/shared/ui/CopyableId';
import { StatusTag } from '@/shared/ui/StatusTag';
import { PageHeader, DetailSection } from '@/shared/ui/DetailPrimitives';
import { usePolling } from '@/shared/hooks/usePolling';
import { apiClient } from '@/shared/api/client';
import { RequestState } from '@/shared/ui/RequestState';
import type { MirrorItem } from '@/shared/api/types';

export const MirrorDetailPage: React.FC = () => {
  const { mirrorKey } = useParams<{ mirrorKey: string }>();
  const navigate = useNavigate();

  const fetchMirror = useCallback(async (signal: AbortSignal) => {
    return apiClient.get<MirrorItem>(`/broker/job-subscription-mirrors/${encodeURIComponent(mirrorKey || '')}`, signal);
  }, [mirrorKey]);

  const mirror = usePolling(fetchMirror, { interval: 5000, enabled: !!mirrorKey, cacheKey: JSON.stringify(['mirror', mirrorKey]) });

  return (
    <div className="space-y-6">
      <PageHeader title="Mirror 详情" icon={<HardDrive size={22} />}
        subtitle={<>Mirror Key · <CopyableId id={mirrorKey || ''} maxLength={36} /></>}
        onBack={() => navigate('/subscriptions/mirrors')}
        badges={mirror.data && <StatusTag status={mirror.data.status} />}
        actions={mirror.data && <Button type="primary" onClick={() => navigate(`/subscriptions/distributions?${new URLSearchParams({
          clusterId: mirror.data!.targetClusterId, sourceClusterId: mirror.data!.sourceClusterId, creationType: JobSubscriptionCreationType.MIRROR,
        })}`)}>查看任务订阅分发</Button>} />

      <RequestState state={mirror} />
      {mirror.data ? (
        <>
          <div className="mirror-route">
            <div className="mirror-endpoint">
              <span className="object-icon"><Server size={22} /></span>
              <div className="object-eyebrow">SOURCE / 源集群</div>
              <div className="mirror-endpoint-name">{mirror.data.sourceClusterId}</div>
              <Button type="link" size="small" className="p-0 mt-2" onClick={() => navigate(`/clusters/${encodeURIComponent(mirror.data!.sourceClusterId)}`)}>查看集群 <ArrowRight size={12} /></Button>
            </div>
            <div className="mirror-route-connector" aria-label="源集群单向同步至目标集群">
              <div className="mirror-route-arrow"><ArrowRight size={24} /></div>
              <span className="mirror-route-caption">单向同步</span>
            </div>
            <div className="mirror-endpoint">
              <span className="object-icon"><Server size={22} /></span>
              <div className="object-eyebrow">TARGET / 目标集群</div>
              <div className="mirror-endpoint-name">{mirror.data.targetClusterId}</div>
              <Button type="link" size="small" className="p-0 mt-2" onClick={() => navigate(`/clusters/${encodeURIComponent(mirror.data!.targetClusterId)}`)}>查看集群 <ArrowRight size={12} /></Button>
            </div>
          </div>
        <DetailSection title="关系信息" description="任务订阅由源集群单向同步至目标集群">
          <Descriptions size="small" column={{ xs: 1, sm: 2, lg: 3 }} layout="vertical">
            <Descriptions.Item label="Mirror Key">
              <CopyableId id={mirror.data.mirrorKey} maxLength={30} />
            </Descriptions.Item>
            <Descriptions.Item label="状态">
              <StatusTag status={mirror.data.status} />
            </Descriptions.Item>
            <Descriptions.Item label="创建结果">
              {mirror.data.created === true ? (
                <Tag color="success" className="rounded-full">新建</Tag>
              ) : mirror.data.created === false ? (
                <Tag className="rounded-full">已存在</Tag>
              ) : '-'}
            </Descriptions.Item>
            <Descriptions.Item label="源集群">
              <Tag className="rounded-full" color="blue">{mirror.data.sourceClusterId}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="目标集群">
              <Tag className="rounded-full" color="green">{mirror.data.targetClusterId}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="同步方向">单向同步</Descriptions.Item>
          </Descriptions>
        </DetailSection>
        </>
      ) : !mirror.loading && !mirror.error ? (
        <Empty description="未找到 Mirror" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ) : null}
    </div>
  );
};
