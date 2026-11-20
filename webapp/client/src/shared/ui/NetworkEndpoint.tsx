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
import React, {useState} from 'react';
import {message, Tooltip} from 'antd';
import {Check, Copy} from 'lucide-react';
import {copyText} from '@/shared/lib/clipboard';

/** 端点单行省略，悬停或聚焦展示完整值，复制完整地址。 */
export function NetworkEndpoint({value}: {value: string}) {
  const [copied, setCopied] = useState(false);

  const copy = async (event: React.MouseEvent) => {
    event.stopPropagation();
    try {
      await copyText(value);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      message.error('复制失败，请手动选择地址复制');
    }
  };

  return (
    <span className="network-endpoint font-mono-id">
      <Tooltip title={value} trigger={['hover', 'focus']} overlayClassName="network-endpoint-tooltip">
        <span className="network-endpoint-value" tabIndex={0}>{value}</span>
      </Tooltip>
      <button type="button" onClick={copy} aria-label={copied ? '已复制' : `复制 ${value}`}
        className="network-endpoint-copy p-0.5 rounded hover:bg-[hsl(var(--muted))] transition-colors opacity-60 hover:opacity-100">
        {copied ? <Check size={12} className="text-[hsl(var(--success))]" />
          : <Copy size={12} className="text-[hsl(var(--muted-foreground))]" />}
      </button>
    </span>
  );
}
