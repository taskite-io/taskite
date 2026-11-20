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
import React, { createContext, useContext, useEffect, useState } from 'react';
import { App as AntApp, ConfigProvider, theme } from 'antd';
import zhCN from 'antd/locale/zh_CN';

const ThemeContext = createContext({ dark: false, toggle: () => {} });
export const useConsoleTheme = () => useContext(ThemeContext);

export const ConsoleTheme: React.FC<React.PropsWithChildren> = ({ children }) => {
  const [dark, setDark] = useState(() => { try { return localStorage.getItem('taskite-console-theme') === 'dark'; } catch { return false; } });
  useEffect(() => {
    document.documentElement.dataset.theme = dark ? 'dark' : 'light';
    try { localStorage.setItem('taskite-console-theme', dark ? 'dark' : 'light'); } catch { /* Theme still works without storage. */ }
  }, [dark]);

  return (
    <ThemeContext.Provider value={{ dark, toggle: () => setDark((value) => !value) }}>
      <ConfigProvider locale={zhCN} theme={{
        algorithm: dark ? theme.darkAlgorithm : theme.defaultAlgorithm,
        token: {
          colorPrimary: '#2563EB',
          colorInfo: '#0EA5E9',
          colorLink: dark ? '#60A5FA' : '#2563EB',
          colorSuccess: '#059669',
          colorWarning: '#F59E0B',
          colorError: '#DC2626',
          colorBgLayout: dark ? '#080F1C' : '#F4F7FB',
          colorBgContainer: dark ? '#111C2D' : '#FFFFFF',
          colorBgElevated: dark ? '#111C2D' : '#FFFFFF',
          colorBorder: dark ? '#2A3A50' : '#DEE6EF',
          colorBorderSecondary: dark ? '#223147' : '#E9EEF5',
          colorText: dark ? '#E8EFF8' : '#0B1220',
          colorTextSecondary: dark ? '#9EACC1' : '#64748B',
          colorTextTertiary: dark ? '#9EACC1' : '#64748B',
          borderRadius: 8,
          fontSize: 13,
          controlHeight: 34,
          controlHeightSM: 30,
          fontFamily: '"Avenir Next", "PingFang SC", "Microsoft YaHei", sans-serif',
        },
        components: {
          Layout: { headerHeight: 56, headerPadding: '0 24px', headerBg: dark ? '#111C2D' : '#FFFFFF', siderBg: dark ? '#111C2D' : '#FFFFFF' },
          Card: { paddingLG: 20, headerHeight: 50, headerFontSize: 14 },
          Table: { cellPaddingBlockSM: 12, cellPaddingInlineSM: 14, headerBg: dark ? '#172438' : '#F8FAFD', headerColor: dark ? '#9EACC1' : '#64748B', headerSplitColor: 'transparent' },
          Tabs: { itemSelectedColor: dark ? '#60A5FA' : '#2563EB', horizontalItemGutter: 28, titleFontSizeLG: 13 },
          Menu: { itemHeight: 40, itemMarginInline: 10, itemSelectedBg: dark ? '#172F50' : '#EBF3FF', itemSelectedColor: dark ? '#60A5FA' : '#2563EB' },
          Descriptions: { itemPaddingBottom: 16 },
          Button: { primaryShadow: 'none', defaultShadow: 'none', dangerShadow: 'none' },
          Modal: { titleFontSize: 16 },
        },
      }}>
        <AntApp>{children}</AntApp>
      </ConfigProvider>
    </ThemeContext.Provider>
  );
};
