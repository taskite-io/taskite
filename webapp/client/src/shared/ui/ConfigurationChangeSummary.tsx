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
import {ArrowRight, Minus, Plus} from 'lucide-react';

export type ConfigurationChange =
  | {kind: 'add' | 'remove'; label: string; members: string[]}
  | {kind: 'update'; label: string; before: number; after: number; danger?: boolean};

export const ConfigurationChangeSummary: React.FC<{changes: ConfigurationChange[]}> = ({changes}) => (
  <div className="configuration-change-summary">
    <p className="configuration-change-intro">请核对以下变更，确认后将提交执行。</p>
    <ul className="configuration-change-list" aria-label="配置变更摘要">
      {changes.map(change => (
        <li key={`${change.kind}-${change.label}`} aria-label={`${change.kind === 'add' ? '添加' : change.kind === 'remove' ? '移除' : '调整'}${change.label}`}
          className={`configuration-change-row configuration-change-${change.kind}${change.kind === 'update' && change.danger ? ' configuration-change-danger' : ''}`}>
          {change.kind === 'update' ? (
            <>
              <span className="configuration-change-label">{change.label}</span>
              <div className="configuration-value-change">
                <span className="configuration-value-before">{change.before}</span>
                <ArrowRight size={15} aria-label="变更为" />
                <strong className="configuration-value-after">{change.after}</strong>
              </div>
            </>
          ) : (
            <>
              <div className="configuration-member-heading">
                <span className="configuration-operation-tag">
                  {change.kind === 'add' ? <Plus size={13} aria-hidden="true" /> : <Minus size={13} aria-hidden="true" />}
                  {change.kind === 'add' ? '添加' : '移除'}
                </span>
                <span className="configuration-change-label">{change.label}</span>
                <span className="configuration-member-count">{change.members.length} 个成员</span>
              </div>
              <div className="configuration-member-ids">
                {change.members.map(id => <span className="configuration-member-id" key={id}>{id}</span>)}
              </div>
            </>
          )}
        </li>
      ))}
    </ul>
  </div>
);
