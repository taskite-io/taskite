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
import {defineConfig} from '@playwright/test';

if (!process.env.PD_CONSOLE_URL) throw new Error('请配置隔离 PD 的 PD_CONSOLE_URL');
export default defineConfig({
  testDir: './e2e', testMatch: '**/live-console.spec.ts', workers: 1, retries: 0,
  reporter: 'list', outputDir: 'target/live-test-results',
  use: {baseURL: process.env.PD_CONSOLE_URL, actionTimeout: 10000, viewport: {width: 1440, height: 1000}, trace: 'retain-on-failure'},
});
