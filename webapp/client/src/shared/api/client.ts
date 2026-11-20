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
import {request, isEmptyActive} from './transport';
export {ApiError, isEmptyActive, isNoBrokerRegistration, segment, clusterPath} from './transport';

export const apiClient = {
  get<T>(path: string, signal?: AbortSignal) { return request<T>(path, {signal}); },
  post<T>(path: string, body?: unknown, signal?: AbortSignal) { return request<T>(path, {method: 'POST', body, signal}); },
  patch<T>(path: string, body?: unknown, signal?: AbortSignal) { return request<T>(path, {method: 'PATCH', body, signal}); },
  delete<T>(path: string, signal?: AbortSignal) { return request<T>(path, {method: 'DELETE', signal}); },
};

export async function activeChange<T>(path: string, signal?: AbortSignal): Promise<T | null> {
  try { return await apiClient.get<T>(path, signal); }
  catch (error) { if (isEmptyActive(error)) return null; throw error; }
}
