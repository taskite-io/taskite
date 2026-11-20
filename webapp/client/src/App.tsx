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
 * 应用入口文件
 * Taskite Console – 分布式集群管理控制台
 */

import React from 'react';
import { HashRouter } from 'react-router-dom';
import { AppRouter } from '@/app/router';
import { AppLayout } from '@/app/layout/AppLayout';
import { ConsoleTheme } from '@/shared/ui/ConsoleTheme';
import {ConsoleBootstrap} from '@/shared/ui/ConsoleBootstrap';
import './global.css';

export default function App() {
  return (
    <ConsoleTheme>
      <HashRouter>
        <ConsoleBootstrap>
        <AppLayout>
          <AppRouter />
        </AppLayout>
        </ConsoleBootstrap>
      </HashRouter>
    </ConsoleTheme>
  );
}
