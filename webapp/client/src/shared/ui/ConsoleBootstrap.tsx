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
import React, {createContext, useContext, useCallback, useMemo} from 'react';
import {apiClient} from '../api/client';
import type {ConsoleBootstrap as Bootstrap} from '../api/types';
import {QueryCacheContext, usePolling} from '../hooks/usePolling';
import {RequestState} from './RequestState';

const Context = createContext<Bootstrap | null>(null);
export const useBootstrap = () => useContext(Context)!;
export function ConsoleBootstrap({children}: React.PropsWithChildren) {
  const fetcher = useCallback((signal: AbortSignal) => apiClient.get<Bootstrap>('/console/bootstrap', signal), []);
  const state = usePolling(fetcher, {interval: 60000, enabled: true});
  const cache = useMemo(() => new Map(), [state.data?.deploymentId]);
  return <Context.Provider value={state.data}>
    <RequestState state={state} />
    {state.data && <QueryCacheContext.Provider key={state.data.deploymentId} value={cache}>{children}</QueryCacheContext.Provider>}
  </Context.Provider>;
}
