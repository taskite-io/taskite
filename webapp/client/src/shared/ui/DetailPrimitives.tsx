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
import React from 'react';
import { Card } from 'antd';
import { ArrowLeft } from 'lucide-react';

export const PageHeader: React.FC<{
  title: React.ReactNode;
  subtitle?: React.ReactNode;
  icon?: React.ReactNode;
  badges?: React.ReactNode;
  actions?: React.ReactNode;
  onBack?: () => void;
}> = ({ title, subtitle, icon, badges, actions, onBack }) => (
  <div className="page-header">
    <div className="page-header-main">
      {onBack && <button className="back-button" aria-label="返回列表" onClick={onBack}><ArrowLeft size={17} /></button>}
      {icon && <span className="object-icon">{icon}</span>}
      <div className="page-header-copy">
        <div className="page-title-row"><h1>{title}</h1>{badges && <div className="object-badges">{badges}</div>}</div>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
    </div>
    {actions && <div className="page-header-actions">{actions}</div>}
  </div>
);

export const DrawerTitle: React.FC<{ title?: string; eyebrow: string; icon: React.ReactNode }> = ({ title, eyebrow, icon }) => (
  <div className="drawer-title">
    <span className="object-icon">{icon}</span>
    <div><div className="object-eyebrow">{eyebrow}</div><div className="drawer-object-name">{title}</div></div>
  </div>
);

export const DetailSection: React.FC<React.PropsWithChildren<{
  title: string;
  description?: string;
  extra?: React.ReactNode;
  className?: string;
}>> = ({ title, description, extra, children, className = '' }) => (
  <Card className={`detail-section ${className}`} title={
    <div className="section-heading"><span>{title}</span>{description && <p>{description}</p>}</div>
  } extra={extra}>{children}</Card>
);

export interface MetricItem {
  label: string;
  value: React.ReactNode;
  hint?: React.ReactNode;
  tone?: 'brand' | 'info' | 'success' | 'warning';
}

export const MetricStrip: React.FC<{ items: MetricItem[] }> = ({ items }) => (
  <div className="metric-strip" style={{ '--metric-count': items.length } as React.CSSProperties}>
    {items.map((item) => <div className={`metric-item ${item.tone ? `metric-${item.tone}` : ''}`} key={item.label}>
      <div className="metric-label">{item.label}</div>
      <div className="metric-value">{item.value}</div>
      {item.hint && <div className="metric-hint">{item.hint}</div>}
    </div>)}
  </div>
);
