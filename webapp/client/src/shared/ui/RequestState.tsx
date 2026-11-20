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
import {Alert, Button, Skeleton} from 'antd';
import {ApiError} from '../api/client';

export function RequestState({state}: {state: {loading: boolean; error: string | null; stale: boolean; refresh: () => void}}) {
  return <>
    {state.error && <Alert className="mb-4" type="warning" showIcon message={state.error}
      description={state.stale ? '数据可能已过时，当前显示最后一次成功快照。' : undefined}
      action={<Button aria-label="刷新" size="small" onClick={state.refresh}>刷新</Button>} />}
    {state.loading && <Skeleton active paragraph={{rows: 4}} />}
  </>;
}
export function MutationError({error}: {error: unknown}) {
  if (!error) return null;
  const e = error instanceof Error ? error : new Error(String(error));
  const api = error instanceof ApiError ? error : undefined;
  const code = api?.code ?? 'ERROR';
  const message = e.message.startsWith(`${code}:`) ? e.message : `${code}: ${e.message}`;
  return <Alert className="mb-4" type="error" showIcon message={message}
    description={<>
      {api?.outcomeUnknown && <p>结果未知，请先查询当前变更和配置后再决定是否重新提交。</p>}
      {api?.leaderEndpoint && <p>Leader：{api.leaderEndpoint}</p>}
      {!!api?.pendingChangeIds.length && <p>关联变更：{api.pendingChangeIds.join(', ')}（可在变更追踪中查询）</p>}
    </>} />;
}
