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
 * 路由配置模块
 * 使用 HashRouter 配置所有页面路由
 */

import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { DashboardPage } from '@/features/dashboard/DashboardPage';
import { PDPage } from '@/features/pd/PDPage';
import { ClusterListPage } from '@/features/clusters/ClusterListPage';
import { ClusterDetailPage } from '@/features/clusters/ClusterDetailPage';
import { WorkerListPage } from '@/features/workers/WorkerListPage';
import {JobYieldListPage, JobYieldDetailPage} from '@/features/workers/JobYieldPage';
import { MirrorListPage } from '@/features/subscriptions/MirrorListPage';
import { MirrorDetailPage } from '@/features/subscriptions/MirrorDetailPage';
import { DistributionPage } from '@/features/subscriptions/DistributionPage';
import { JobSubscriptionListPage } from '@/features/subscriptions/JobSubscriptionListPage';
import { ChangeListPage } from '@/features/changes/ChangeListPage';
import { ChangeDetailPage } from '@/features/changes/ChangeDetailPage';

export const AppRouter: React.FC = () => {
  return (
    <Routes>
      <Route path="/" element={<DashboardPage />} />
      <Route path="/pd" element={<PDPage />} />
      <Route path="/clusters" element={<ClusterListPage />} />
      <Route path="/clusters/:clusterId" element={<ClusterDetailPage />} />
      <Route path="/workers" element={<WorkerListPage />} />
      <Route path="/workers/yields" element={<JobYieldListPage />} />
      <Route path="/workers/yields/:yieldId" element={<JobYieldDetailPage />} />
      <Route path="/subscriptions/mirrors" element={<MirrorListPage />} />
      <Route path="/subscriptions/mirrors/:mirrorKey" element={<MirrorDetailPage />} />
      <Route path="/subscriptions/distributions" element={<JobSubscriptionListPage />} />
      <Route path="/subscriptions/distributions/:distributionKey" element={<DistributionPage />} />
      <Route path="/changes" element={<ChangeListPage />} />
      <Route path="/changes/subscriptions/:distributionKey" element={<DistributionPage change />} />
      <Route path="/changes/pd/:changeId" element={<ChangeDetailPage key="pd" type="pd" />} />
      <Route path="/changes/clusters/:clusterId/:changeId" element={<ChangeDetailPage key="cluster" type="cluster" />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
};
