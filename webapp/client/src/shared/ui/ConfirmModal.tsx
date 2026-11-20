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
 * 确认弹窗组件
 * 危险操作二次确认，支持输入确认文本
 */

import React, { useEffect, useState } from 'react';
import { Modal, Input } from 'antd';
import { AlertTriangle, CheckCircle2 } from 'lucide-react';
import {MutationError} from './RequestState';
import {ApiError} from '../api/client';

interface ConfirmModalProps {
  open: boolean;
  title: string;
  content: React.ReactNode;
  confirmText?: string;
  confirmLabel?: string;
  danger?: boolean;
  warning?: React.ReactNode;
  disabled?: boolean;
  onConfirm: () => void | Promise<void>;
  onCancel: () => void;
}

export const ConfirmModal: React.FC<ConfirmModalProps> = ({
  open,
  title,
  content,
  confirmText,
  confirmLabel = '确认',
  danger = false,
  warning,
  disabled = false,
  onConfirm,
  onCancel,
}) => {
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<unknown>(null);
  useEffect(() => {
    if (open) { setInput(''); setError(null); }
  }, [open]);

  const canConfirm = !disabled && (!confirmText || input === confirmText);

  const handleConfirm = async () => {
    if (!canConfirm || loading || (error instanceof ApiError && error.outcomeUnknown)) return;
    setLoading(true);
    setError(null);
    try {
      await onConfirm();
    } catch (error) {
      setError(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      className={`console-modal${danger ? ' console-modal-danger' : ''}`}
      width={520}
      open={open}
      title={
        <div className="flex items-center gap-2">
          <span className={`object-icon ${danger ? 'object-icon-danger' : ''}`}>
            {danger ? <AlertTriangle size={20} /> : <CheckCircle2 size={20} />}
          </span>
          <div className="confirmation-title-copy">
            <span>{title}</span>
            {danger && <span className="confirmation-danger-tag">高危操作</span>}
          </div>
        </div>
      }
      onCancel={() => { if (!loading) onCancel(); }}
      onOk={handleConfirm}
      okText={confirmLabel}
      okButtonProps={{ 'aria-label': confirmLabel, danger, disabled: !canConfirm || (error instanceof ApiError && error.outcomeUnknown), loading }}
      cancelText="取消"
      destroyOnClose
    >
      <div className="py-2 space-y-4">
        <MutationError error={error} />
        {warning && <div className="configuration-risk-notice" role="alert">
          <AlertTriangle size={18} aria-hidden="true" />
          <div><strong>操作风险</strong><p>{warning}</p></div>
        </div>}
        <div className="confirmation-summary">{content}</div>
        {confirmText && (
          <div>
            <p className="text-sm text-[hsl(var(--muted-foreground))] mb-2">
              请输入 <strong>{confirmText}</strong> 以确认操作
            </p>
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={`输入 ${confirmText}`}
            />
          </div>
        )}
      </div>
    </Modal>
  );
};
