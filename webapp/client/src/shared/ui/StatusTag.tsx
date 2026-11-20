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
 * 状态标签组件
 * 根据状态值渲染带颜色和图标的标签，支持 PD/Broker 变更状态、Worker 状态等
 */

import React from 'react';
import { Tag } from 'antd';
import {
  CheckCircle2,
  AlertCircle,
  XCircle,
  PauseCircle,
  Loader2,
  Zap,
  HelpCircle,
  Clock,
  Ban,
  ArrowRightCircle,
  Timer,
  ArrowRightLeft,
} from 'lucide-react';
import { ChangeStatus, WorkerStatus, GeneralStatus, PartitionStatus } from '@/shared/api/enums';

export type StatusCategory = 'change' | 'worker' | 'partition' | 'general';

const changeStatusMap: Record<ChangeStatus, { color: string; icon: React.ReactNode; label: string }> = {
  [ChangeStatus.QUEUED]: { color: 'default', icon: <Clock size={12} />, label: '排队中' },
  [ChangeStatus.PENDING]: { color: 'default', icon: <Clock size={12} />, label: '待执行' },
  [ChangeStatus.WAITING_CATCH_UP]: { color: 'processing', icon: <Timer size={12} />, label: '等待追平' },
  [ChangeStatus.IN_PROGRESS]: { color: 'processing', icon: <Loader2 size={12} className="animate-spin" />, label: '执行中' },
  [ChangeStatus.WAITING_READY]: { color: 'warning', icon: <Clock size={12} />, label: '等待就绪' },
  [ChangeStatus.WAITING_LEADER_CONVERGE]: { color: 'warning', icon: <Clock size={12} />, label: '等待 Leader 收敛' },
  [ChangeStatus.SUPERSEDING]: { color: 'warning', icon: <Timer size={12} />, label: '正在替代' },
  [ChangeStatus.SUPERSEDED]: { color: 'default', icon: <ArrowRightCircle size={12} />, label: '已被替代' },
  [ChangeStatus.CANCELLING]: { color: 'warning', icon: <Ban size={12} />, label: '取消中' },
  [ChangeStatus.COMPLETED]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '已完成' },
  [ChangeStatus.FAILED]: { color: 'error', icon: <XCircle size={12} />, label: '失败' },
  [ChangeStatus.CANCELLED]: { color: 'default', icon: <Ban size={12} />, label: '已取消' },
};

const workerStatusMap: Record<WorkerStatus, { color: string; icon: React.ReactNode; label: string }> = {
  [WorkerStatus.ONLINE]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '在线' },
  [WorkerStatus.OFFLINE]: { color: 'error', icon: <XCircle size={12} />, label: '离线' },
};

const partitionStatusMap: Record<PartitionStatus, { color: string; icon: React.ReactNode; label: string }> = {
  [PartitionStatus.CREATING]: { color: 'processing', icon: <Loader2 size={12} className="animate-spin" />, label: '创建中' },
  [PartitionStatus.ACTIVE]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '活跃' },
  [PartitionStatus.REBALANCING]: { color: 'processing', icon: <ArrowRightLeft size={12} />, label: '重平衡中' },
  [PartitionStatus.DELETING]: { color: 'warning', icon: <Loader2 size={12} className="animate-spin" />, label: '删除中' },
};

const generalStatusMap: Record<GeneralStatus, { color: string; icon: React.ReactNode; label: string }> = {
  [GeneralStatus.HEALTHY]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '健康' },
  [GeneralStatus.ONLINE]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '在线' },
  [GeneralStatus.IDLE]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '空闲' },
  [GeneralStatus.BUSY]: { color: 'warning', icon: <Zap size={12} />, label: '忙碌' },
  [GeneralStatus.WARNING]: { color: 'warning', icon: <AlertCircle size={12} />, label: '警告' },
  [GeneralStatus.ERROR]: { color: 'error', icon: <XCircle size={12} />, label: '故障' },
  [GeneralStatus.OFFLINE]: { color: 'error', icon: <XCircle size={12} />, label: '离线' },
  [GeneralStatus.DRAINING]: { color: 'processing', icon: <ArrowRightCircle size={12} />, label: '退出中' },
  [GeneralStatus.FAULT]: { color: 'error', icon: <XCircle size={12} />, label: '故障' },
  [GeneralStatus.PAUSED]: { color: 'processing', icon: <PauseCircle size={12} />, label: '暂停' },
  [GeneralStatus.MAINTENANCE]: { color: 'processing', icon: <PauseCircle size={12} />, label: '维护中' },
  [GeneralStatus.INITIALIZING]: { color: 'purple', icon: <Loader2 size={12} className="animate-spin" />, label: '初始化中' },
  [GeneralStatus.RESTARTING]: { color: 'purple', icon: <Loader2 size={12} className="animate-spin" />, label: '重启中' },
  [GeneralStatus.ACTIVE]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '活跃' },
  [GeneralStatus.SYNCING]: { color: 'processing', icon: <Loader2 size={12} className="animate-spin" />, label: '同步中' },
  [GeneralStatus.DISTRIBUTING]: { color: 'processing', icon: <Loader2 size={12} className="animate-spin" />, label: '分发中' },
  [GeneralStatus.DELETING]: { color: 'warning', icon: <Loader2 size={12} className="animate-spin" />, label: '删除中' },
  [GeneralStatus.DELETED]: { color: 'default', icon: <Ban size={12} />, label: '已删除' },
  [GeneralStatus.COMPLETED]: { color: 'success', icon: <CheckCircle2 size={12} />, label: '已完成' },
  [GeneralStatus.IN_PROGRESS]: { color: 'processing', icon: <Loader2 size={12} className="animate-spin" />, label: '执行中' },
  [GeneralStatus.PENDING]: { color: 'default', icon: <Clock size={12} />, label: '待执行' },
  [GeneralStatus.FAILED]: { color: 'error', icon: <XCircle size={12} />, label: '失败' },
  [GeneralStatus.CANCELLED]: { color: 'default', icon: <Ban size={12} />, label: '已取消' },
  [GeneralStatus.UNKNOWN]: { color: 'default', icon: <HelpCircle size={12} />, label: '未知' },
};

interface StatusTagProps {
  status: string;
  category?: StatusCategory;
  className?: string;
}

export const StatusTag: React.FC<StatusTagProps> = ({ status, category = 'general', className }) => {
  const map: Partial<Record<string, {color: string; icon: React.ReactNode; label: string}>> = category === 'change' ? changeStatusMap : category === 'worker' ? workerStatusMap : category === 'partition' ? partitionStatusMap : generalStatusMap;
  const normalized = status.trim();
  const config = map[category === 'general' ? normalized.toLowerCase() : normalized.toUpperCase()]
    || generalStatusMap[normalized.toLowerCase() as GeneralStatus] || changeStatusMap[normalized.toUpperCase() as ChangeStatus]
    || workerStatusMap[normalized.toUpperCase() as WorkerStatus] || { color: 'default', icon: <HelpCircle size={12} />, label: status };
  return (
    <Tag
      color={config.color}
      icon={config.icon}
      className={`inline-flex items-center gap-1 ${normalized.toLowerCase() === GeneralStatus.DRAINING ? 'status-tag-draining' : ''} ${className || ''}`}
    >
      {config.label}
    </Tag>
  );
};

/** 判断变更是否已达终态 */
export function isChangeTerminal(status: ChangeStatus): boolean {
  return status === ChangeStatus.COMPLETED || status === ChangeStatus.FAILED || status === ChangeStatus.CANCELLED || status === ChangeStatus.SUPERSEDED;
}
