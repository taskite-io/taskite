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
 * 应用布局组件
 * 左侧导航 + 顶部面包屑与主题切换 + 内容区
 * 品牌：Taskite Console – 分布式任务控制台
 */

import React, { useState, useEffect } from 'react';
import { Layout, Menu, Tooltip, Tag } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';
import {
  LayoutDashboard,
  Server,
  Boxes,
  Cpu,
  HardDrive,
  ListChecks,
  ChevronLeft,
  ChevronRight,
  Moon,
  Sun,
  Activity,
} from 'lucide-react';
import { cn } from '@/shared/lib/utils';
import { TaskiteIcon } from '@/shared/ui/TaskiteIcon';
import { useConsoleTheme } from '@/shared/ui/ConsoleTheme';
import {useBootstrap} from '@/shared/ui/ConsoleBootstrap';

const { Header, Sider, Content } = Layout;

interface MenuItem {
  key: string;
  icon: React.ReactNode;
  label: string;
  children?: { key: string; label: string }[];
}

const menuItems: MenuItem[] = [
  { key: '/', icon: <LayoutDashboard size={18} />, label: '总览' },
  { key: '/pd', icon: <Server size={18} />, label: 'PD 管理' },
  { key: '/clusters', icon: <Boxes size={18} />, label: 'Broker 集群' },
  { key: '/workers', icon: <Cpu size={18} />, label: 'Worker' },
  {
    key: '/subscriptions',
    icon: <HardDrive size={18} />,
    label: '资源管理',
    children: [
      { key: '/subscriptions/mirrors', label: 'Mirror 列表' },
      { key: '/subscriptions/distributions', label: '任务订阅' },
    ],
  },
  { key: '/changes', icon: <ListChecks size={18} />, label: '变更追踪' },
];

const ENV_INFO = {
  productName: 'Taskite Console',
};

export const AppLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const location = useLocation();
  const navigate = useNavigate();
  const [collapsed, setCollapsed] = useState(false);
  const [expandedKeys, setExpandedKeys] = useState<string[]>([]);
  const { dark: darkMode, toggle: toggleTheme } = useConsoleTheme();
  const bootstrap = useBootstrap();
  const grafana = import.meta.env.VITE_TASKITE_CONSOLE_GRAFANA_URL;

  const selectedKeys = [location.pathname.startsWith('/clusters') ? '/clusters'
    : location.pathname.startsWith('/changes') ? '/changes'
    : location.pathname.startsWith('/workers') ? '/workers'
    : location.pathname.startsWith('/subscriptions/mirrors') ? '/subscriptions/mirrors'
    : location.pathname.startsWith('/subscriptions/distributions') ? '/subscriptions/distributions'
    : location.pathname];
  const openKeys = menuItems
    .filter((item) => item.children?.some((c) => location.pathname.startsWith(c.key)))
    .map((item) => item.key);
  useEffect(() => {
    if (openKeys.length) setExpandedKeys(openKeys);
  }, [location.pathname]);

  const getBreadcrumbs = () => {
    const parts: string[] = [];
    for (const item of menuItems) {
      if (item.key === location.pathname || (item.key !== '/' && !item.children && location.pathname.startsWith(`${item.key}/`))) {
        parts.push(item.label);
        if (item.key !== location.pathname) parts.push('详情');
        break;
      }
      if (item.children) {
        const child = item.children.find((c) => location.pathname.startsWith(c.key));
        if (child) {
          parts.push(item.label, child.label);
          if (child.key !== location.pathname) parts.push('详情');
          break;
        }
      }
    }
    return parts;
  };

  const breadcrumbs = getBreadcrumbs();

  return (
    <Layout className="min-h-screen">
      {/* 侧边导航 */}
      <Sider
        trigger={null}
        collapsible
        collapsed={collapsed}
        width={200}
        collapsedWidth={64}
        className="console-sidebar z-50 border-r border-[hsl(var(--border))]"
        style={{ background: 'hsl(var(--card))' }}
      >
        {/* Logo 区域 */}
        <div
          className={cn(
            'h-14 flex items-center px-3 border-b border-[hsl(var(--border))]',
            collapsed && 'justify-center px-2'
          )}
        >
          <div className="flex items-center justify-center flex-shrink-0">
            <TaskiteIcon size={28} />
          </div>
          {!collapsed && (
            <div className="ml-3 overflow-hidden">
              <div className="font-semibold text-[hsl(var(--foreground))] text-sm leading-tight">
                Taskite Console
              </div>
              <div className="text-[10px] text-[hsl(var(--muted-foreground))] leading-tight truncate">
                分布式任务控制台
              </div>
            </div>
          )}
        </div>

        {/* 导航菜单 */}
        <Menu
          mode="inline"
          selectedKeys={selectedKeys}
          openKeys={expandedKeys}
          onOpenChange={setExpandedKeys}
          onClick={({ key }) => {
            navigate(String(key));
          }}
          items={menuItems.map((item) => ({
            key: item.key,
            icon: item.icon,
            label: item.label,
            children: item.children,
          }))}
          className="border-r-0 pt-1"
          style={{ background: 'transparent' }}
        />

        {/* 弹性空间：将折叠按钮推到底部 */}
        <div className="flex-1" />

        {/* 底部折叠按钮 */}
        <div className="p-3 border-t border-[hsl(var(--border))]">
          <button
            onClick={() => setCollapsed(!collapsed)}
            aria-label={collapsed ? '展开导航' : '收起导航'}
            className="w-full flex items-center justify-center p-2 rounded-lg hover:bg-[hsl(var(--muted))] transition-colors text-[hsl(var(--muted-foreground))]"
          >
            {collapsed ? <ChevronRight size={16} /> : <ChevronLeft size={16} />}
          </button>
        </div>
      </Sider>

      {/* 右侧主体 */}
      <Layout
        className="transition-all duration-300"
        style={{ minWidth: 0 }}
      >
        {/* 顶部导航栏 */}
        <Header
          className="console-topbar flex items-center justify-between px-6 h-14 sticky top-0 z-40 border-b border-[hsl(var(--border))]"
          style={{ background: 'hsl(var(--card))' }}
        >
          {/* 左侧面包屑 */}
          <div className="flex items-center gap-2 min-w-0">
            <Activity size={14} className="text-[hsl(var(--primary))] flex-shrink-0" />
            {breadcrumbs.length > 0 ? (
              <div className="flex items-center gap-1.5 text-sm min-w-0">
                <span className="text-[hsl(var(--muted-foreground))] truncate">
                  {ENV_INFO.productName}
                </span>
                {breadcrumbs.map((part, i) => (
                  <React.Fragment key={i}>
                    <span className="text-[hsl(var(--border))]">/</span>
                    <span
                      className={cn(
                        'truncate',
                        i === breadcrumbs.length - 1
                          ? 'text-[hsl(var(--foreground))] font-medium'
                          : 'text-[hsl(var(--muted-foreground))]'
                      )}
                    >
                      {part}
                    </span>
                  </React.Fragment>
                ))}
              </div>
            ) : (
              <span className="text-sm text-[hsl(var(--foreground))] font-medium">
                {ENV_INFO.productName}
              </span>
            )}
          </div>

          {/* 主题切换 */}
          <div className="flex items-center flex-shrink-0">
            <Tooltip title={`创建 ${bootstrap.deploymentId} · 节点 ${bootstrap.nodeId}`}>
              <Tag>{bootstrap.buildVersion}</Tag>
            </Tooltip>
            <a href="/console/licenses.txt" className="mr-3" target="_blank" rel="noreferrer">许可</a>
            {typeof grafana === 'string' && /^https?:\/\//.test(grafana) && <a className="mr-3" href={grafana} target="_blank" rel="noreferrer">Grafana</a>}
            <Tooltip title={darkMode ? '切换亮色主题' : '切换暗色主题'}>
              <button
                onClick={toggleTheme}
                aria-label={darkMode ? '切换亮色主题' : '切换暗色主题'}
                className="flex items-center justify-center w-8 h-8 rounded-lg hover:bg-[hsl(var(--muted))] transition-colors text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]"
              >
                {darkMode ? <Sun size={16} /> : <Moon size={16} />}
              </button>
            </Tooltip>
          </div>
        </Header>

        {/* 内容区域 */}
        <Content className="console-content bg-[hsl(var(--background))] min-h-[calc(100vh-56px)]">
          {children}
        </Content>
      </Layout>
    </Layout>
  );
};
