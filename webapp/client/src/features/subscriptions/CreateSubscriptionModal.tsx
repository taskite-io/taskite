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
import React, {useCallback, useRef, useState} from 'react';
import {Alert, Form, Input, InputNumber, Modal, Select} from 'antd';
import {Braces, Clock, FileCode2, Layers3} from 'lucide-react';
import {apiClient, ApiError} from '@/shared/api/client';
import {DistributionStatus} from '@/shared/api/enums';
import type {
  ClusterListResponse, CreateJobSubscriptionRequest, CreateJobSubscriptionResponse,
  CreateTimerSubscriptionRequest, CreateTimerSubscriptionResponse, JobSubscriptionListResponse,
  TimerSubscriptionType,
} from '@/shared/api/types';
import {usePolling} from '@/shared/hooks/usePolling';
import {MutationError, RequestState} from '@/shared/ui/RequestState';
import {JsonEditor} from '@/shared/ui/JsonEditor';
import {timerExpressionError} from './timerValidation';

type Kind = 'job' | 'timer';
type FormValues = CreateJobSubscriptionRequest & CreateTimerSubscriptionRequest;
const DEFAULT_TENANT = '<default>';
const schedules: Record<TimerSubscriptionType, {label: string; example: string; hint: string}> = {
  DURATION: {label: 'Duration · 单次', example: 'PT30S', hint: 'ISO 8601 时长，例如 PT30S（30 秒后执行一次）。'},
  CYCLE: {label: 'Cycle · 循环', example: 'R/PT30S', hint: 'ISO 8601 循环，例如 R/PT30S（每 30 秒）或 R5/PT1M（每分钟，共 5 次）。'},
  CRON: {label: 'Cron · 定时', example: '0 */5 * * * *', hint: '六字段 Cron：秒、分、时、日、月、星期，例如每 5 分钟执行。'},
};
const variablesRule = {
  validator(_: unknown, value?: string) {
    try {
      const parsed: unknown = JSON.parse(value?.trim() || '{}');
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error();
      return Promise.resolve();
    } catch { return Promise.reject(new Error('变量必须是有效的 JSON 对象')); }
  },
};

export function CreateSubscriptionModal({kind, initialClusterId, initialTenantId, onClose}: {
  kind: Kind; initialClusterId?: string; initialTenantId?: string; onClose: () => void;
}) {
  const timer = kind === 'timer';
  const [form] = Form.useForm<FormValues>();
  const [submitting, setSubmitting] = useState(false);
  const inFlight = useRef(false);
  const [error, setError] = useState<unknown>(null);
  const clusterState = usePolling(useCallback((signal: AbortSignal) =>
    apiClient.get<ClusterListResponse>('/broker/clusters', signal), []),
    {interval: 5000, enabled: true, cacheKey: 'clusters'});
  const subscriptionState = usePolling(useCallback((signal: AbortSignal) =>
    apiClient.get<JobSubscriptionListResponse>('/broker/job-subscriptions', signal), []),
    {interval: 5000, enabled: timer, cacheKey: 'job-subscriptions'});
  const clusterId = Form.useWatch('clusterId', form);
  const tenantId = Form.useWatch('tenantId', form)?.trim() || DEFAULT_TENANT;
  const type = Form.useWatch('type', form) || 'DURATION';
  const clusters = clusterState.data?.clusters ?? [];
  const clusterUnavailable = !clusterState.data || clusterState.stale || !!clusterState.error;
  const subscriptionsUnavailable = timer && (!subscriptionState.data || subscriptionState.stale || !!subscriptionState.error);
  const jobTypes = [...new Set((subscriptionState.data?.subscriptions ?? [])
    .filter(item => item.clusterId === clusterId && item.tenantId === tenantId && item.status === DistributionStatus.COMPLETED)
    .map(item => item.jobType))].sort();
  const unknownOutcome = error instanceof ApiError && error.outcomeUnknown;
  const unavailable = clusterUnavailable || subscriptionsUnavailable;
  const validCluster = clusters.some(item => item.clusterId === clusterId);

  const create = async (values: FormValues) => {
    if (inFlight.current || unknownOutcome || unavailable || !validCluster) return;
    if (timer && values.jobTypes.some(value => !jobTypes.includes(value))) {
      form.setFields([{name: 'jobTypes', errors: ['任务订阅状态已变化，请重新选择已完成分发的任务类型']}]);
      return;
    }
    inFlight.current = true;
    setSubmitting(true);
    setError(null);
    const scope = {clusterId: values.clusterId, tenantId: values.tenantId?.trim() || DEFAULT_TENANT,
      name: values.name?.trim() || '', variables: values.variables?.trim() || '{}'};
    try {
      if (timer) {
        await apiClient.post<CreateTimerSubscriptionResponse>('/broker/timer-subscriptions', {
          ...scope, type: values.type, expression: values.expression.trim(), jobTypes: values.jobTypes,
        } satisfies CreateTimerSubscriptionRequest);
      } else {
        await apiClient.post<CreateJobSubscriptionResponse>('/broker/job-subscriptions', {
          ...scope, jobType: values.jobType.trim(), retries: values.retries, priority: values.priority,
        } satisfies CreateJobSubscriptionRequest);
      }
      onClose();
    } catch (failure) {
      setError(failure instanceof ApiError && failure.status >= 500
        ? new ApiError(failure.message, failure.status, failure.code, failure.pendingChangeIds, failure.leaderEndpoint, true)
        : failure);
    } finally { inFlight.current = false; setSubmitting(false); }
  };

  return <Modal className="console-modal subscription-create-modal" width={980} centered open
    title={<div className="subscription-create-title"><span className="object-icon">
      {timer ? <Clock size={22} /> : <FileCode2 size={22} />}
    </span><div><span>创建{timer ? '定时' : '任务'}订阅</span>
      <p>{timer ? '设置触发计划，每次触发为所选任务类型创建 Job。'
        : '设置任务模板，创建后可查看各分区的分发进度。'}</p>
    </div></div>}
    onCancel={() => {if (!inFlight.current) onClose();}}
    onOk={() => form.submit()} confirmLoading={submitting}
    okText="创建" cancelText="取消"
    okButtonProps={{'aria-label': '创建', disabled: unavailable || !validCluster || unknownOutcome || (timer && !jobTypes.length)}}>
      <MutationError error={error} />
      <RequestState state={clusterState} />
      {timer && <RequestState state={subscriptionState} />}
      {!clusterUnavailable && !clusters.length && <Alert className="mb-4" type="info" showIcon message="请先创建 Broker 集群" />}
      <Form className="subscription-create-form" form={form} layout="vertical" onFinish={create} disabled={submitting || unknownOutcome}
        initialValues={{clusterId: initialClusterId, tenantId: initialTenantId || DEFAULT_TENANT,
          variables: '{}', retries: 3, priority: 50, type: 'DURATION', jobTypes: []}}>
        <section className="subscription-create-basics" aria-labelledby="subscription-create-basics-title">
          <h3 className="subscription-create-section-title" id="subscription-create-basics-title">
            <Layers3 size={15} aria-hidden="true" />基础信息
          </h3>
          <div className="subscription-create-basic-fields">
            <Form.Item name="clusterId" label="所属集群" rules={[{required: true, message: '请选择所属集群'}]}>
              <Select aria-label="所属集群" showSearch optionFilterProp="label" placeholder="选择集群"
                disabled={submitting || unknownOutcome || clusterUnavailable}
                onChange={() => form.setFieldValue('jobTypes', [])}
                options={clusters.map(item => ({value: item.clusterId, label: item.clusterId}))} />
            </Form.Item>
            <Form.Item name="tenantId" label="所属租户" extra="留空使用默认租户 <default>">
              <Input placeholder={DEFAULT_TENANT} onChange={() => form.setFieldValue('jobTypes', [])} />
            </Form.Item>
            <Form.Item name="name" label="订阅名称" rules={timer ? [{required: true, whitespace: true, message: '请输入定时订阅名称'}] : []}>
              <Input placeholder={timer ? '如：每分钟结算' : '可选的显示名称'} />
            </Form.Item>
          </div>
        </section>
        <div className="subscription-create-columns">
          <section className="subscription-create-settings" aria-labelledby="subscription-create-settings-title">
            <h3 className="subscription-create-section-title" id="subscription-create-settings-title">
              {timer ? <Clock size={15} aria-hidden="true" /> : <FileCode2 size={15} aria-hidden="true" />}
              {timer ? '定时计划' : '任务配置'}
            </h3>
            {timer ? <>
              <Form.Item name="jobTypes" label="任务类型" dependencies={['clusterId', 'tenantId']}
                rules={[{required: true, type: 'array', min: 1, message: '请选择至少一个任务类型'},
                  {validator: (_, values: string[] = []) => values.every(value => jobTypes.includes(value))
                    ? Promise.resolve() : Promise.reject(new Error('请选择当前集群、租户下已完成分发的任务类型'))}]}
                extra={clusterId && !subscriptionsUnavailable && !jobTypes.length
                  ? '该集群和租户暂无已完成分发的任务订阅，请先创建任务订阅并等待分发完成。'
                  : '仅显示当前集群和租户下已完成分发的任务类型。'}>
                <Select aria-label="任务类型" mode="multiple" maxTagCount="responsive" showSearch optionFilterProp="label"
                  placeholder="选择一个或多个任务类型" disabled={submitting || unknownOutcome || subscriptionsUnavailable || !validCluster}
                  options={jobTypes.map(value => ({value, label: value}))} />
              </Form.Item>
              <div className="subscription-create-schedule">
                <Form.Item name="type" label="定时类型" rules={[{required: true}]}>
                  <Select aria-label="定时类型" options={Object.entries(schedules).map(([value, item]) => ({value, label: item.label}))} />
                </Form.Item>
                <Form.Item name="expression" label="定时表达式" dependencies={['type']} validateTrigger={['onChange', 'onBlur']}
                  rules={[({getFieldValue}) => ({validator: (_, value?: string) => {
                    const error = timerExpressionError(getFieldValue('type'), value);
                    return error ? Promise.reject(new Error(error)) : Promise.resolve();
                  }})]}>
                  <Input className="subscription-create-expression" placeholder={schedules[type].example}
                    aria-describedby="subscription-create-schedule-hint" />
                </Form.Item>
              </div>
              <p className="subscription-create-hint" id="subscription-create-schedule-hint">
                {schedules[type].hint}
              </p>
            </> : <>
              <Form.Item name="jobType" label="任务类型" rules={[{required: true, whitespace: true, message: '请输入任务类型'}]}>
                <Input placeholder="如：payment" />
              </Form.Item>
              <div className="subscription-create-execution">
                <Form.Item name="retries" label="初始重试次数" rules={[{required: true, type: 'integer', min: 1, max: 2147483647, message: '请输入正整数'}]}>
                  <InputNumber min={1} max={2147483647} precision={0} className="w-full" />
                </Form.Item>
                <Form.Item name="priority" label="初始优先级" rules={[{required: true, type: 'integer', min: 1, max: 100, message: '请输入 1–100 的整数'}]}>
                  <InputNumber min={1} max={100} precision={0} className="w-full" />
                </Form.Item>
              </div>
            </>}
          </section>
          <Form.Item className="subscription-create-variables" name="variables"
            label={<span className="subscription-create-section-title"><Braces size={15} aria-hidden="true" />
              {timer ? '任务变量（JSON）' : '模板变量（JSON）'}</span>} rules={[variablesRule]}
            extra={timer ? '复制到每次创建的 Job。留空使用 {}。' : '与创建 Job 时的变量合并，Job 请求中的同名字段优先。留空使用 {}。'}>
            <JsonEditor label={timer ? '任务变量（JSON）' : '模板变量（JSON）'} disabled={submitting || unknownOutcome} />
          </Form.Item>
        </div>
      </Form>
  </Modal>;
}
