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

export default defineConfig({
  testDir: './e2e',
  testMatch: '**/console.spec.ts',
  fullyParallel: true,
  retries: 0,
  workers: 3,
  reporter: [['list'], ['html', {outputFolder: 'target/playwright-report', open: 'never'}]],
  outputDir: 'target/test-results',
  use: {baseURL: 'http://127.0.0.1:14173', trace: 'retain-on-failure'},
  webServer: {
    command: 'npm run preview -- --port 14173 --strictPort',
    url: 'http://127.0.0.1:14173/console/',
    reuseExistingServer: !process.env.CI,
  },
});
