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
 * 格式化工具模块
 * 日期、时间、JSON、字节、Int64 等格式化
 */

import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import type { Int64String } from '../api/types';
dayjs.extend(utc);

export function formatDateTime(value: string | number | Date): string {
  if (!value) return '-';
  return dayjs(value).format('YYYY-MM-DD HH:mm:ss');
}

export function formatDateTimeUTC(value: string | number | Date): string {
  if (!value) return '-';
  return dayjs(value).utc().format('YYYY-MM-DD HH:mm:ss') + ' UTC';
}

export function formatDateTimeRelative(value: string | number | Date): string {
  if (!value) return '-';
  const d = dayjs(value);
  const now = dayjs();
  const diff = now.diff(d, 'second');
  if (diff < 0) return d.format('YYYY-MM-DD HH:mm');
  if (diff < 60) return `${diff}秒前`;
  if (diff < 3600) return `${Math.floor(diff / 60)}分钟前`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}小时前`;
  return d.format('YYYY-MM-DD HH:mm');
}

export function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}

export function toDecimalString(value: unknown): string {
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'number') return String(value);
  if (typeof value === 'string') return value;
  return String(value);
}

export function formatBytes(bytes: number): string {
  if (bytes === 0 || bytes == null) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

export function formatDuration(ms: number): string {
  if (ms == null) return '-';
  if (ms < 1000) return `${ms}ms`;
  const seconds = Math.floor(ms / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ${seconds % 60}s`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ${minutes % 60}m`;
  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}

export function formatNumber(n: number): string {
  if (n == null) return '-';
  return n.toLocaleString('zh-CN');
}

export function formatInt64(value: Int64String | undefined): string {
  if (value == null) return '-';
  return value;
}

export function formatPercent(numerator: number, denominator: number): string {
  if (!denominator) return '0%';
  return `${Math.floor((numerator / denominator) * 100)}%`;
}

/** IPv6 在端点文本中使用方括号；已带括号的 host 不重复包裹。 */
export function formatEndpoint(host: string, port: number): string {
  const trimmed = host.trim();
  const bareHost = trimmed.startsWith('[') && trimmed.endsWith(']')
    ? trimmed.slice(1, -1) : trimmed;
  return `${bareHost.includes(':') ? `[${bareHost}]` : bareHost}:${port}`;
}

/** 生成 Worker 复合行 key */
export function encodeWorkerKey(clusterId: string, tenantId: string, name: string, workerId: string): string {
  return JSON.stringify([clusterId, tenantId, name, workerId]);
}
