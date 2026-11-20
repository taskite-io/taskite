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
import type {TimerSubscriptionType} from '@/shared/api/types';

function positiveInterval(value: string): boolean {
  if (!/^P(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$/.test(value)
    || value.endsWith('T')) return false;
  if (!(value.match(/\d+(?:\.\d+)?/g) ?? []).some(part => Number(part) > 0)) return false;
  const fraction = value.match(/\.(\d+)S$/)?.[1];
  return !fraction || !/[1-9]/.test(fraction.slice(3));
}

/** Check schedule shape here; PD performs authoritative parsing. */
export function timerExpressionError(type: TimerSubscriptionType, expression?: string): string | null {
  const value = expression?.trim() ?? '';
  if (!value) return '请输入定时表达式';
  if (type === 'DURATION') return positiveInterval(value) ? null
    : 'Duration 只接受正的 ISO 8601 时长，例如 PT30S；循环表达式请使用 Cycle';
  if (type === 'CYCLE') {
    const parts = value.split('/');
    const match = parts[0].match(/^R(\d*)$/);
    const validCount = !!match && (!match[1] || (BigInt(match[1]) > 0n && BigInt(match[1]) <= 2147483647n));
    const validStart = parts.length !== 3 ||
      (/^\d{4}-\d\d-\d\dT.+(?:Z|[+-]\d\d:\d\d)$/.test(parts[1]) && Number.isFinite(Date.parse(parts[1])));
    return (parts.length === 2 || parts.length === 3) && validCount && validStart && positiveInterval(parts.at(-1)!)
      ? null : 'Cycle 请使用 R[n]/[开始时间/]时长，例如 R/PT30S 或 R5/PT1M';
  }
  return value.split(/\s+/).length === 6 && value.split(/\s+/).every(field => /^[\dA-Za-z*?,/#LW-]+$/.test(field))
    ? null : 'Cron 需要六个字段：秒、分、时、日、月、星期，例如 0 */5 * * * *';
}
