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
 * 可复制 ID 组件
 * 使用等宽字体展示长标识符，支持一键复制和 tooltip 查看完整值
 */

import React, { useState } from 'react';
import { message, Tooltip } from 'antd';
import { Copy, Check } from 'lucide-react';
import { Link } from 'react-router-dom';
import {copyText} from '@/shared/lib/clipboard';

interface CopyableIdProps {
  id?: string;
  maxLength?: number;
  className?: string;
  showTooltip?: boolean;
  to?: string;
}

export const CopyableId: React.FC<CopyableIdProps> = ({
  id = '',
  maxLength = 24,
  className,
  showTooltip = true,
  to,
}) => {
  const [copied, setCopied] = useState(false);

  const display = id.length > maxLength ? `${id.slice(0, maxLength)}...` : id;

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation();
    try {
      await copyText(id);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      message.error('复制失败，请手动选择内容复制');
    }
  };

  const content = (
    <span className={`copyable-id inline-flex items-center gap-1 font-mono-id ${className || ''}`}>
      {to ? <Link to={to} className="copyable-id-value copyable-id-link">{display}</Link>
        : <span className="copyable-id-value cursor-default">{display}</span>}
      <button
        onClick={handleCopy}
        aria-label={copied ? '已复制' : `复制 ${id}`}
        className="p-0.5 rounded hover:bg-[hsl(var(--muted))] transition-colors opacity-60 hover:opacity-100"
      >
        {copied ? (
          <Check size={12} className="text-[hsl(var(--success))]" />
        ) : (
          <Copy size={12} className="text-[hsl(var(--muted-foreground))]" />
        )}
      </button>
    </span>
  );

  if (showTooltip && id.length > maxLength) {
    return <Tooltip title={id}>{content}</Tooltip>;
  }
  return content;
};
