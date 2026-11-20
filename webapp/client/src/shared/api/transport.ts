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
import {isLosslessNumber, parse} from 'lossless-json';
import {validateResponse} from './schema';
import {ChangeAcceptanceOutcome} from './enums';

// HTTP Java long fields remain exact strings even when their current value is small.
const longFields = new Set([
  'timerSubscriptionKey', 'dueDate', 'occurrence',
  'mirrorKey', 'distributionKey', 'subscriptionKey', 'deletionDistributionKey', 'version', 'term', 'epoch', 'sourceEpoch', 'targetEpoch',
  'endpointEpoch', 'recoveryEpoch', 'commitIndex', 'lastApplied', 'lastLogIndex', 'matchIndex',
  'lag', 'barrierIndex', 'configurationIndex', 'observedAtEpochMillis', 'renewalTimeoutMillis',
  'timeoutMillis', 'requestTimeoutMillis', 'pollIntervalMillis',
  'executionGeneration', 'authorityIndex',
  'generation', 'expectedGeneration', 'lastRedistributedAt',
  'workerLeaseToken', 'leaseDeadlineEpochMillis', 'yieldId', 'releasedJobCount',
  'createdAtEpochMillis', 'completedAtEpochMillis', 'lastAttemptAtEpochMillis',
  'nextAttemptAtEpochMillis', 'acknowledgedAtEpochMillis',
]);

export function parseResponse(text: string): unknown {
  function convert(value: unknown, key = '', parent = ''): unknown {
    if (Array.isArray(value)) return value.map(item => convert(item, '', key));
    if (!isLosslessNumber(value)) {
      if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([child, item]) => [child, convert(item, child, key)]));
      return value;
    }
    const token = value.value;
    if (longFields.has(key) && parent !== 'priorities') {
      if (!/^-?\d+$/.test(token)) throw new Error(`字段 ${key} 必须是整数`);
      const exact = BigInt(token);
      if (exact < -(1n << 63n) || exact > (1n << 63n) - 1n) {
        throw new Error(`字段 ${key} 超出 int64 范围`);
      }
      return token;
    }
    const numeric = Number(token);
    if (!Number.isFinite(numeric)) throw new Error(`字段 ${key} 数值无效`);
    // Unknown future integer fields also stay exact rather than silently rounding.
    return /^-?\d+$/.test(token) && !Number.isSafeInteger(numeric) ? token : numeric;
  }
  return convert(parse(text));
}

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status = 0,
    readonly code = 'NETWORK_ERROR',
    readonly pendingChangeIds: string[] = [],
    readonly leaderEndpoint?: string,
    readonly outcomeUnknown = false,
  ) { super(message); this.name = 'ApiError'; }
}

export function isEmptyActive(error: unknown): boolean {
  return error instanceof ApiError && error.status === 404 &&
    ['ACTIVE_CHANGE_NOT_FOUND', 'ACTIVE_PD_MEMBER_CHANGE_NOT_FOUND'].includes(error.code);
}
export function isNoBrokerRegistration(error: unknown): boolean {
  return error instanceof ApiError && error.code === 'NO_BROKER_REGISTERED';
}

export async function request<T>(
  path: string,
  options: {method?: 'GET' | 'POST' | 'PATCH' | 'DELETE'; body?: unknown; signal?: AbortSignal} = {},
): Promise<T> {
  const method = options.method ?? 'GET';
  const isWrite = method !== 'GET';
  const timeout = AbortSignal.timeout(30_000);
  const signal = options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
  try {
    const response = await fetch(`/api/v1${path}`, {
      method,
      credentials: 'same-origin',
      signal,
      headers: {Accept: 'application/json', ...(options.body === undefined ? {} : {'Content-Type': 'application/json'})},
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    });
    if (response.status === 204) {
      const route = path.split('?')[0];
      if (method === 'DELETE' && /^\/broker\/(?:clusters|job-subscription-mirrors)\/[^/]+$/.test(route)) return undefined as T;
      throw new ApiError('本端点的响应正文缺失', 204, 'PROTOCOL_ERROR', [], undefined, isWrite);
    }
    const text = await response.text();
    let body: unknown;
    try { body = text ? parseResponse(text) : undefined; }
    catch {
      if (response.ok) throw new ApiError('响应解析失败，请查询实际状态', response.status, 'PROTOCOL_ERROR', [], undefined, isWrite);
    }
    if (!response.ok) {
      const error = body && typeof body === 'object' ? body as Record<string, unknown> : {};
      const code = typeof error.error === 'string' ? error.error : `HTTP_${response.status}`;
      const detail = typeof error.message === 'string' ? error.message : text.slice(0, 600) || response.statusText;
      throw new ApiError(detail || code, response.status, code,
        Array.isArray(error.pendingChangeIds) ? error.pendingChangeIds.filter((id): id is string => typeof id === 'string') : [],
        typeof error.leaderEndpoint === 'string' ? error.leaderEndpoint : undefined,
        isWrite && response.status >= 500 && typeof error.error !== 'string');
    }
    if (body === undefined) throw new ApiError('响应正文缺失', response.status, 'PROTOCOL_ERROR', [], undefined, isWrite);
    try { validateResponse(path, method, body); }
    catch (error) {
      const candidate = body && typeof body === 'object' && 'changeId' in body ? body.changeId : undefined;
      const knownIds = typeof candidate === 'string' && candidate !== ChangeAcceptanceOutcome.NO_CHANGES ? [candidate] : [];
      throw new ApiError(error instanceof Error ? error.message : '响应契约无效', response.status, 'PROTOCOL_ERROR', knownIds, undefined, isWrite);
    }
    return body as T;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    if (options.signal?.aborted) throw error;
    throw new ApiError(
      isWrite ? '请求中断，结果未知。请查询当前变更与配置后再决定是否重试。' : '管理入口暂不可达，请重试。',
      0, timeout.aborted ? 'REQUEST_TIMEOUT' : 'NETWORK_ERROR', [], undefined, isWrite,
    );
  }
}

export const segment = (value: string | number) => encodeURIComponent(String(value));
export const clusterPath = (clusterId: string) => `/broker/clusters/${segment(clusterId)}`;
export function queryString(values: Record<string, string | undefined>): string {
  const params = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => { if (value?.trim()) params.set(key, value.trim()); });
  return params.size ? `?${params}` : '';
}
