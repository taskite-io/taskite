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
import {createContext, useCallback, useContext, useEffect, useRef, useState} from 'react';
import {ApiError} from '../api/client';

interface Snapshot { data: unknown; lastUpdated: number; }
export const QueryCacheContext = createContext<Map<string, Snapshot> | null>(null);
const CACHE_TTL = 120_000;
const CACHE_LIMIT = 80;
interface Options<T> { interval: number; enabled: boolean; cacheKey?: string; isTerminal?: (data: T) => boolean; }
interface State<T> { data: T | null; loading: boolean; error: string | null; stale: boolean; lastUpdated: number; }
export function usePolling<T>(fetcher: (signal: AbortSignal) => Promise<T>, options: Options<T>) {
  const cache = useContext(QueryCacheContext);
  const key = options.cacheKey ?? '';
  const snapshot = (): State<T> => {
    const entry = options.enabled && key ? cache?.get(key) : undefined;
    return entry && Date.now() - entry.lastUpdated < CACHE_TTL
      ? {data: entry.data as T, loading: false, error: null, stale: true, lastUpdated: entry.lastUpdated}
      : {data: null, loading: options.enabled, error: null, stale: false, lastUpdated: 0};
  };
  const [state, setState] = useState<State<T> & {key: string}>(() => ({...snapshot(), key}));
  const [revision, setRevision] = useState(0);
  const terminal = useRef(options.isTerminal);
  terminal.current = options.isTerminal;
  const refresh = useCallback(() => setRevision(v => v + 1), []);
  useEffect(() => {
    let disposed = false, stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let controller: AbortController | undefined;
    setState(s => s.key === key ? s : {...snapshot(), key});
    async function run() {
      if (disposed || !options.enabled || controller || stopped) return;
      if (document.hidden) { timer = setTimeout(run, options.interval * 3); return; }
      controller = new AbortController();
      const current = controller;
      try {
        setState(s => ({...s, loading: !s.data}));
        const data = await fetcher(current.signal);
        if (disposed || current.signal.aborted) return;
        const lastUpdated = Date.now();
        if (cache && key) {
          cache.delete(key);
          cache.set(key, {data, lastUpdated});
          for (const [cachedKey, entry] of cache) {
            if (lastUpdated - entry.lastUpdated >= CACHE_TTL) cache.delete(cachedKey);
          }
          while (cache.size > CACHE_LIMIT) cache.delete(cache.keys().next().value!);
        }
        setState({key, data, loading: false, error: null, stale: false, lastUpdated});
        stopped = terminal.current?.(data) ?? false;
      } catch (error) {
        if (disposed || current.signal.aborted) return;
        const message = error instanceof ApiError ? `${error.code}: ${error.message}` : error instanceof Error ? error.message : '请求失败';
        stopped = error instanceof ApiError && [400, 403, 404].includes(error.status);
        if (stopped) {
          if (key) cache?.delete(key);
          setState({key, data: null, loading: false, error: message, stale: false, lastUpdated: 0});
        } else {
          setState(s => ({...s, loading: false, error: message, stale: s.data !== null}));
        }
      } finally {
        controller = undefined;
        if (!disposed && !stopped) timer = setTimeout(run, options.interval);
      }
    }
    const visible = () => { if (!document.hidden && !stopped) { clearTimeout(timer); void run(); } };
    if (options.enabled) void run();
    document.addEventListener('visibilitychange', visible);
    return () => { disposed = true; clearTimeout(timer); controller?.abort(); document.removeEventListener('visibilitychange', visible); };
  }, [fetcher, options.enabled, options.interval, key, cache, revision]);
  // A new query must never render the preceding query's rows, even before its effect runs.
  return {...(state.key === key ? state : snapshot()), refresh};
}
