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
import {ChangeAcceptanceOutcome} from './enums';
export interface KnownChangeRecord {
  scope: 'pd' | 'cluster' | 'subscription'; clusterId?: string; changeId: string; firstSeenAt: string; lastSeenAt: string;
}
const key = (deployment: string) => `taskite-console:${deployment}:known-changes`;
export function readKnownChanges(deployment: string): KnownChangeRecord[] {
  try {
    const data: unknown = JSON.parse(localStorage.getItem(key(deployment)) || '[]');
    return Array.isArray(data) ? data.filter(r => r && ['pd', 'cluster', 'subscription'].includes(r.scope) && typeof r.changeId === 'string' && typeof r.lastSeenAt === 'string' && (r.scope === 'pd' || typeof r.clusterId === 'string')).slice(0, 50) : [];
  } catch {return [];}
}
export function clearHistory(deployment: string) {try {localStorage.removeItem(key(deployment));} catch {}}
export function rememberChange(deployment: string, record: Pick<KnownChangeRecord, 'scope' | 'clusterId' | 'changeId'>) {
  if (record.changeId === ChangeAcceptanceOutcome.NO_CHANGES) return;
  const records = readKnownChanges(deployment);
  const previous = records.find(r => r.scope === record.scope && r.clusterId === record.clusterId && r.changeId === record.changeId);
  const now = new Date().toISOString();
  const updated = [{...record, firstSeenAt: previous?.firstSeenAt ?? now, lastSeenAt: now},
    ...records.filter(r => r !== previous)].slice(0, 50);
  try {localStorage.setItem(key(deployment), JSON.stringify(updated));} catch {}
}
