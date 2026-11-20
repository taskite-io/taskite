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
import {defineConfig, loadEnv} from 'vite';
import react from '@vitejs/plugin-react';
import {fileURLToPath, URL} from 'node:url';

export default defineConfig(({mode}) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    base: '/console/',
    plugins: [react()],
    resolve: {alias: {'@': fileURLToPath(new URL('./src', import.meta.url))}},
    build: {outDir: 'target/dist', emptyOutDir: true, chunkSizeWarningLimit: 1200, license: {fileName: 'licenses.txt'}},
    server: {
      proxy: {'/api/v1': {target: env.TASKITE_CONSOLE_DEV_PROXY_PD_HTTP_URL || 'http://127.0.0.1:19001', changeOrigin: true}},
    },
  };
});
