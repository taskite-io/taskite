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
import {Tooltip} from 'antd';

export const ClusterCountBreakdown: React.FC<{
  items: {clusterId: string; value: number}[];
  tone?: 'brand' | 'info' | 'success';
}> = ({items, tone = 'brand'}) => {
  if (!items.length) return null;
  const density = items.length > 12 ? 'dense' : items.length > 4 ? 'compact' : 'normal';
  return (
    <div className={`cluster-count-breakdown cluster-count-${density} cluster-count-${tone}`}>
      {items.map(({clusterId, value}) => (
        <span key={clusterId} className="cluster-count-entry" data-cluster-id={clusterId}
          aria-label={`${clusterId}：${value}`}>
          <Tooltip title={clusterId} trigger={['hover', 'focus']} overlayClassName="cluster-count-tooltip">
            <span className="cluster-count-name" tabIndex={0}>{clusterId}</span>
          </Tooltip>
          <span className="cluster-count-value">{value}</span>
        </span>
      ))}
    </div>
  );
};
