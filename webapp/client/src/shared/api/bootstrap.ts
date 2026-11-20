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
 * Bootstrap 模块
 * 加载控制台初始化配置，展示产品、路径、版本、环境及 UI 只读配置
 */

import { apiClient } from './client';
import type { ConsoleBootstrap } from './types';

let bootstrapPromise: Promise<ConsoleBootstrap> | null = null;

export async function getBootstrap(): Promise<ConsoleBootstrap> {
  if (!bootstrapPromise) {
    bootstrapPromise = apiClient.get<ConsoleBootstrap>('/console/bootstrap').catch(error => {bootstrapPromise = null; throw error;});
  }
  return bootstrapPromise;
}

export function clearBootstrapCache() {
  bootstrapPromise = null;
}
