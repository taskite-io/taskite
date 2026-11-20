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
import {test, expect, type Page} from '@playwright/test';

test.describe('Console subscription creation', () => {
  test('Literal timer expressions must match the selected type and revalidate on type changes', async ({page}) => {
    await readySubscriptions(page);
    const writes: unknown[] = [];
    await page.route('**/api/v1/broker/timer-subscriptions', route => {
      if (route.request().method() !== 'POST') return route.fallback();
      writes.push(route.request().postDataJSON());
      return route.fulfill({status: 201, json: {timerSubscriptionKey: '9007199254741099', dueDate: 1793400000000}});
    });
    const dialog = await open(page, 'timer', '?clusterId=demo&tenantId=a');
    await dialog.getByLabel('订阅名称', {exact: true}).fill('settle');
    await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
    await page.locator('.ant-select-item-option').filter({hasText: /^charge$/}).click();
    await dialog.getByLabel('定时表达式', {exact: true}).fill('R/PT30S');
    await expect(dialog.getByText('Duration 只接受正的 ISO 8601 时长，例如 PT30S；循环表达式请使用 Cycle')).toBeVisible();
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    expect(writes).toEqual([]);
    await dialog.getByRole('combobox', {name: '定时类型', exact: true}).press('ArrowDown');
    await page.locator('.ant-select-dropdown:visible .ant-select-item-option').filter({hasText: 'Cycle'}).click();
    await expect(dialog.getByText('Duration 只接受正的 ISO 8601 时长，例如 PT30S；循环表达式请使用 Cycle')).toHaveCount(0);
    await dialog.getByLabel('定时表达式', {exact: true}).fill('PT30S');
    await expect(dialog.getByText('Cycle 请使用 R[n]/[开始时间/]时长，例如 R/PT30S 或 R5/PT1M')).toBeVisible();
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    expect(writes).toEqual([]);
    await dialog.getByRole('combobox', {name: '定时类型', exact: true}).press('ArrowDown');
    await page.locator('.ant-select-dropdown:visible .ant-select-item-option').filter({hasText: 'Cron'}).click();
    await expect(dialog.getByText('Cron 需要六个字段：秒、分、时、日、月、星期，例如 0 */5 * * * *')).toBeVisible();
    await dialog.getByLabel('定时表达式', {exact: true}).fill('0 */5 * * * *');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog).not.toBeVisible();
    expect(writes).toHaveLength(1);
  });

  test('Ordinary deletion confirms scope and shows asynchronous progress, disabling mirrored deletion', async ({page}) => {
    let status = 'COMPLETED';
    const deletionKey = '9007199254741053';
    let deletionCompleted = false;
    const writes: string[] = [];
    const reads: string[] = [];
    page.on('request', request => {
      if (request.method() === 'GET' && request.url().includes('/api/v1/')) reads.push(request.url());
    });
    await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
      total: 2, subscriptions: resourceAssets.map(item => ({...item, status,
        deletionDistributionKey: status === 'COMPLETED' ? undefined : deletionKey})),
    }}));
    await page.route(`**/api/v1/broker/job-subscriptions/${deletionKey}`, route => route.fulfill({json: {
      ...resource, distributionKey: deletionKey, subscriptionKey: resource.distributionKey,
      valueType: 'JOB_SUBSCRIPTION_DELETION', status: deletionCompleted ? 'COMPLETED' : 'DISTRIBUTING',
      targets: resource.targets.map(t => ({...t,
        acknowledgedPartitions: deletionCompleted ? t.expectedPartitions : 0,
        confirmedPartitionIds: deletionCompleted ? Array.from({length: t.expectedPartitions}, (_, i) => i + 1) : [],
        pendingPartitions: deletionCompleted ? [] : Array.from({length: t.expectedPartitions}, (_, i) => i + 1),
        status: deletionCompleted ? 'COMPLETED' : 'DISTRIBUTING'})),
    }}));
    await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}`, route => route.fulfill({json: {
      ...resource, status, deletionDistributionKey: deletionKey,
    }}));
    await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}?*`, route => {
      if (route.request().method() !== 'DELETE') return route.fallback();
      writes.push(route.request().url());
      status = 'DELETING';
      return route.fulfill({status: 202, body: '{"distributionKey":9007199254741053}', contentType: 'application/json'});
    });
    await go(page, '/subscriptions/distributions');
    const rows = page.locator('.ant-table-tbody .ant-table-row');
    await expect(rows.nth(1).getByRole('button', {name: '删除', exact: true})).toBeDisabled();
    await rows.first().getByRole('button', {name: '删除', exact: true}).click();
    const dialog = page.getByRole('dialog');
    await expect(dialog).toContainText('Job、Incident 和变量');
    expect(writes).toEqual([]);
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).not.toBeVisible();
    await expect(page).toHaveURL(new RegExp(`/changes/subscriptions/${deletionKey}$`));
    await expect(page.getByRole('heading', {name: '删除任务订阅', exact: true})).toBeVisible();
    await expect(page.getByText('删除确认状态 · 2', {exact: true})).toBeVisible();
    await expect(page.locator('.distribution-target').first()).toContainText('0 / 2');
    await expect(page.locator('.distribution-target').last()).toContainText('0 / 1');
    await expect(page.getByRole('button', {name: '重新下发', exact: true})).toHaveCount(0);
    await expect(page.getByText('charge', {exact: true})).toBeVisible();
    expect(new URL(writes[0]).searchParams.get('clusterId')).toBe('demo');
    expect(new URL(writes[0]).searchParams.get('tenantId')).toBe('a');
    expect(writes[0]).toContain(resource.distributionKey);
    await page.reload();
    await expect(page.getByText('删除确认状态 · 2', {exact: true})).toBeVisible();
    await page.getByRole('link', {name: resource.distributionKey, exact: true}).click();
    await expect(page.getByRole('heading', {name: '任务订阅详情', exact: true})).toBeVisible();
    await expect(page.locator('.distribution-target').first()).toContainText('1 / 2');
    await page.getByRole('button', {name: '查看删除进度', exact: true}).click();
    await expect(page.locator('.distribution-target').first()).toContainText('0 / 2');

    await page.getByRole('button', {name: '返回列表', exact: true}).click();
    await expect(page).toHaveURL(/\/changes\?tab=subscription$/);
    const current = page.locator('.ant-card').filter({has: page.getByText('当前变更', {exact: true})});
    await expect(current.locator('.ant-table-row')).toHaveCount(1);
    await expect(current).toContainText('order');
    await expect(page.getByText('访问记录 (1)', {exact: true})).toBeVisible();
    await current.getByRole('button', {name: '查看详情', exact: true}).click();
    deletionCompleted = true;
    status = 'DELETED';
    await page.getByRole('button', {name: '刷新', exact: true}).click();
    await expect(page.getByText('删除分发已完成，源集群及 Mirror 目标分区均已确认。', {exact: true})).toBeVisible();
    await expect(page.locator('.distribution-target').first()).toContainText('2 / 2');
    await page.getByRole('button', {name: '返回列表', exact: true}).click();
    await expect(current).toContainText('暂无当前变更');
    await page.reload();
    await expect(page.getByText('访问记录 (1)', {exact: true})).toBeVisible();
    await page.locator('.ant-card').filter({has: page.getByText('访问记录 (1)', {exact: true})})
      .locator('.ant-table-row').click();
    await expect(page).toHaveURL(new RegExp(`/changes/subscriptions/${deletionKey}$`));
    await go(page, '/changes?tab=subscription');
    await page.getByLabel('按分发 Key筛选', {exact: true}).click();
    await page.getByRole('textbox', {name: '按分发 Key筛选', exact: true}).fill(deletionKey);
    await page.locator('.worker-column-filter').getByRole('button', {name: '筛选', exact: true}).click();
    const result = page.locator('.ant-card').filter({has: page.getByText('查询结果', {exact: true})});
    await expect(result).toContainText(deletionKey);
    await result.getByRole('button', {name: '查看详情', exact: true}).click();
    await expect(page).toHaveURL(new RegExp(`/changes/subscriptions/${deletionKey}$`));
    expect(reads.some(url => url.includes('/broker/clusters/subscription/'))).toBe(false);
    await go(page, '/subscriptions/distributions');
    await expect(rows).toHaveCount(0);
  });

  test('Ordinary deletion rejection and unknown outcomes never create change history', async ({page}) => {
    let writes = 0;
    await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}?*`, route => {
      writes++;
      return route.fulfill(writes === 1
        ? {status: 409, json: {error: 'INVALID_STATE', message: 'Subscription is already deleting'}}
        : {status: 503, json: {error: 'TIMEOUT', message: 'Deletion outcome is unknown'}});
    });
    await go(page, '/subscriptions/distributions');
    await page.getByRole('button', {name: '删除', exact: true}).first().click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).toContainText('Subscription is already deleting');
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).toContainText('删除结果尚未确认');
    await expect(dialog.getByRole('button', {name: '确认删除', exact: true})).toBeDisabled();
    expect(await page.evaluate(() => localStorage.getItem('taskite-console:isolated:known-changes'))).toBeNull();
    await expect(page).toHaveURL(/\/subscriptions\/distributions$/);
    expect(writes).toBe(2);
  });

  test('Timer deletion cancels and refreshes the list without a success dialog', async ({page}) => {
    let deleted = false;
    const writes: string[] = [];
    await page.route('**/api/v1/broker/timer-subscriptions', route => route.fulfill({json: {
      total: deleted ? 0 : 1, subscriptions: deleted ? [] : [timerSubscription],
    }}));
    await page.route(`**/api/v1/broker/timer-subscriptions/${timerSubscription.timerSubscriptionKey}?*`, route => {
      writes.push(route.request().url());
      deleted = true;
      return route.fulfill({json: {timerSubscriptionKey: timerSubscription.timerSubscriptionKey}});
    });
    await go(page, '/subscriptions/distributions?tab=timer');
    await page.getByRole('button', {name: '删除', exact: true}).click();
    const dialog = page.getByRole('dialog');
    await expect(dialog).toContainText('已创建或已发出的 Job 不会撤回');
    await dialog.getByRole('button', {name: /^取\s*消$/}).click();
    expect(writes).toEqual([]);
    await page.getByRole('button', {name: '删除', exact: true}).click();
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).not.toBeVisible();
    await expect(page.locator('.ant-card-head-title')).toContainText('定时任务 · 0');
    expect(writes).toHaveLength(1);
    expect(new URL(writes[0]).searchParams.get('clusterId')).toBe('demo');
    expect(new URL(writes[0]).searchParams.get('tenantId')).toBe('a');
    await expect(page).toHaveURL(/\/subscriptions\/distributions\?tab=timer$/);
    expect(await page.evaluate(() => localStorage.getItem('taskite-console:isolated:known-changes'))).toBeNull();
  });

  test('Timer deletion keeps server rejection visible and blocks resubmission of an unknown result', async ({page}) => {
    let writes = 0;
    await page.route(`**/api/v1/broker/timer-subscriptions/${timerSubscription.timerSubscriptionKey}?*`, route => {
      writes++;
      return route.fulfill(writes === 1
        ? {status: 404, json: {error: 'NOT_FOUND', message: 'Timer no longer exists in this tenant'}}
        : {status: 503, json: {error: 'TIMEOUT', message: 'Admin request timed out before completion'}});
    });
    await go(page, '/subscriptions/distributions?tab=timer');
    await page.getByRole('button', {name: '删除', exact: true}).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).toContainText('Timer no longer exists in this tenant');
    await dialog.getByRole('button', {name: '确认删除', exact: true}).click();
    await expect(dialog).toContainText('删除结果尚未确认');
    await expect(dialog.getByRole('button', {name: '确认删除', exact: true})).toBeDisabled();
    expect(writes).toBe(2);
  });

  test('Tabs display separate lists and only the matching create action, including after reload', async ({page}) => {
    await go(page, '/subscriptions/distributions');
    await expect(page.getByRole('button', {name: '创建普通任务', exact: true})).toBeVisible();
    await expect(page.getByRole('button', {name: '创建定时任务', exact: true})).toHaveCount(0);
    await expect(page.locator('.ant-table-thead')).toContainText('分发 Key');
    await page.getByRole('tab', {name: '定时任务', exact: true}).click();
    await expect(page.getByRole('button', {name: '创建定时任务', exact: true})).toBeVisible();
    await expect(page.getByRole('button', {name: '创建普通任务', exact: true})).toHaveCount(0);
    await expect(page.locator('.ant-card-head-title')).toContainText('定时任务 · 1');
    await expect(page.locator('.ant-table-thead')).toContainText('定时订阅 Key');
    await expect(page.locator('.ant-table-thead')).not.toContainText('分发 Key');
    await expect(page.locator('.ant-table-tbody')).toContainText('9007199254741099');
    await page.reload();
    await expect(page.getByRole('tab', {name: '定时任务', exact: true})).toHaveAttribute('aria-selected', 'true');
    await expect(page.getByRole('button', {name: '创建定时任务', exact: true})).toBeVisible();
    await page.getByRole('button', {name: 'settle', exact: true}).click();
    const drawer = page.locator('.ant-drawer-body');
    await expect(drawer).toContainText('Asia/Shanghai');
    await expect(drawer).toContainText('无限循环');
    await page.getByRole('button', {name: 'Close', exact: true}).click();
    await page.getByRole('textbox', {name: '按定时任务名称筛选', exact: true}).fill('missing');
    await expect(page.locator('.ant-card-head-title')).toContainText('定时任务 · 0');
    await page.getByRole('tab', {name: '普通任务', exact: true}).click();
    await expect(page.getByRole('button', {name: '创建普通任务', exact: true})).toBeVisible();
    await expect(page.locator('.ant-table-thead')).toContainText('分发 Key');
  });

  for (const theme of ['light', 'dark']) {
    test(`Subscription tabs and creation action fit desktop and mobile (${theme})`, async ({page}) => {
      await go(page, '/subscriptions/distributions');
      if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题', exact: true}).click();
      for (const tab of ['普通任务', '定时任务']) {
        await page.getByRole('tab', {name: tab, exact: true}).click();
        for (const width of [1440, 390]) {
          await page.setViewportSize({width, height: 1000});
          const action = page.getByRole('button', {name: tab === '普通任务' ? '创建普通任务' : '创建定时任务', exact: true});
          await expect(action).toBeVisible();
          const bounds = await action.boundingBox();
          expect(bounds!.x).toBeGreaterThanOrEqual(0);
          expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(width);
          const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
          expect(overflow).toBe(false);
          await page.screenshot({path: `/tmp/taskite-subscription-${tab === '普通任务' ? 'job' : 'timer'}-${theme}-${width}.png`, animations: 'disabled'});
        }
      }
    });
  }

  async function open(page: Page, kind: 'job' | 'timer', query = '') {
    await go(page, `/subscriptions/distributions${query}`);
    if (kind === 'timer') await page.getByRole('tab', {name: '定时任务', exact: true}).click();
    await page.getByRole('button', {name: kind === 'job' ? '创建普通任务' : '创建定时任务', exact: true}).click();
    const dialog = page.getByRole('dialog');
    await expect(dialog.getByRole('button', {name: '创建', exact: true})).toBeEnabled();
    return dialog;
  }

  async function readySubscriptions(page: Page) {
    await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
      total: 4, subscriptions: [
        {...resource, status: 'COMPLETED'},
        {...resource, distributionKey: '2', jobType: 'invoice', status: 'COMPLETED'},
        {...resource, distributionKey: '3', jobType: 'pending', status: 'DISTRIBUTING'},
        {...resource, distributionKey: '4', jobType: 'other-tenant', tenantId: 'b', status: 'COMPLETED'},
      ],
    }}));
  }

  test('Job creation validates JSON and submits a custom template with exact response key', async ({page}) => {
    const bodies: unknown[] = [];
    await page.route('**/api/v1/broker/job-subscriptions', route => {
      if (route.request().method() === 'POST') {
        bodies.push(route.request().postDataJSON());
        return route.fulfill({status: 202, body: '{"distributionKey":9007199254741052}', contentType: 'application/json'});
      }
      return route.fallback();
    });
    const dialog = await open(page, 'job', '?clusterId=demo&tenantId=a');
    await dialog.getByLabel('订阅名称', {exact: true}).fill('billing');
    await dialog.getByLabel('任务类型', {exact: true}).fill('charge');
    await dialog.getByLabel('初始重试次数', {exact: true}).fill('7');
    await dialog.getByLabel('初始优先级', {exact: true}).fill('90');
    await dialog.getByRole('textbox', {name: '模板变量（JSON）', exact: true}).fill('[]');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog.getByText('变量必须是有效的 JSON 对象')).toBeVisible();
    expect(bodies).toEqual([]);
    await dialog.getByRole('textbox', {name: '模板变量（JSON）', exact: true}).fill('{"amount":42,"orderId":9007199254741099}');
    await expect(dialog.locator('.cm-gutters')).toBeVisible();
    await dialog.getByRole('button', {name: '格式化', exact: true}).click();
    await expect(dialog.locator('.cm-line')).toHaveCount(4);
    await expect(dialog.locator('.cm-content')).toContainText('9007199254741099');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog).not.toBeVisible();
    await expect(page).toHaveURL(/distributions\?clusterId=demo&tenantId=a$/);
    expect(bodies).toEqual([{clusterId: 'demo', tenantId: 'a', name: 'billing', jobType: 'charge',
      variables: '{\n  "amount": 42,\n  "orderId": 9007199254741099\n}', retries: 7, priority: 90}]);
  });

  test('Job creation uses the selected cluster and default tenant and template values', async ({page}) => {
    let body: unknown;
    await page.route('**/api/v1/broker/job-subscriptions', route => {
      if (route.request().method() === 'POST') {
        body = route.request().postDataJSON();
        return route.fulfill({status: 202, json: {distributionKey: '9007199254741052'}});
      }
      return route.fallback();
    });
    await go(page, '/subscriptions/distributions');
    await page.getByRole('button', {name: '创建普通任务', exact: true}).click();
    const dialog = page.getByRole('dialog');
    await dialog.getByRole('combobox', {name: '所属集群', exact: true}).click();
    await page.locator('.ant-select-item-option').filter({hasText: /^demo$/}).click();
    await dialog.getByLabel('所属租户', {exact: true}).fill(' ');
    await dialog.getByLabel('任务类型', {exact: true}).fill('charge');
    await dialog.getByRole('textbox', {name: '模板变量（JSON）', exact: true}).fill('');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog).not.toBeVisible();
    await expect(page).toHaveURL(/distributions$/);
    expect(body).toEqual({clusterId: 'demo', tenantId: '<default>', name: '', jobType: 'charge',
      variables: '{}', retries: 3, priority: 50});
  });

  for (const [type, expression] of [
    ['DURATION', 'PT30S'],
    ['CYCLE', 'R5/PT1M'],
    ['CRON', '0 */5 * * * *'],
  ]) {
    test(`Timer creation submits ${type} and returns directly to the refreshed list`, async ({page}) => {
      await readySubscriptions(page);
      const bodies: unknown[] = [];
      let saved: typeof timerSubscription | null = null;
      await page.route('**/api/v1/broker/timer-subscriptions', route => {
        if (route.request().method() !== 'POST') return route.fulfill({json: {
          total: saved ? 1 : 0, subscriptions: saved ? [saved] : [],
        }});
        bodies.push(route.request().postDataJSON());
        saved = {...timerSubscription, type, expression};
        return route.fulfill({status: 201, body: '{"timerSubscriptionKey":9007199254741099,"dueDate":1793400000000}',
          contentType: 'application/json'});
      });
      const dialog = await open(page, 'timer', '?clusterId=demo&tenantId=a');
      await dialog.getByLabel('订阅名称', {exact: true}).fill('settle');
      await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
      const options = page.locator('.ant-select-dropdown:visible .ant-select-item-option');
      await expect(options).toHaveCount(2);
      await options.filter({hasText: /^charge$/}).click();
      await options.filter({hasText: /^invoice$/}).click();
      await dialog.getByLabel('订阅名称', {exact: true}).click();
      await dialog.getByRole('combobox', {name: '定时类型', exact: true}).press('ArrowDown');
      await page.locator('.ant-select-dropdown:visible .ant-select-item-option').filter({hasText:
        type === 'DURATION' ? 'Duration' : type === 'CYCLE' ? 'Cycle' : 'Cron'}).click();
      await dialog.getByLabel('定时表达式', {exact: true}).fill(`= "${expression}"`);
      await dialog.getByRole('button', {name: '创建', exact: true}).click();
      await expect(dialog.locator('.ant-form-item-explain-error')).toBeVisible();
      expect(bodies).toEqual([]);
      await dialog.getByLabel('定时表达式', {exact: true}).fill(expression);
      await dialog.getByRole('textbox', {name: '任务变量（JSON）', exact: true}).fill('{"delaySeconds":30}');
      await dialog.getByRole('button', {name: '创建', exact: true}).click();
      await expect(dialog).not.toBeVisible();
      await expect(page.locator('.ant-table-tbody')).toContainText('9007199254741099');
      await expect(page.locator('.ant-table-tbody')).toContainText('2026-10-30 22:40:00 UTC');
      await expect(page.locator('.ant-table-tbody')).toContainText(type);
      await expect(page.getByRole('tab', {name: '定时任务', exact: true})).toHaveAttribute('aria-selected', 'true');
      await expect(page.getByText('定时订阅已创建', {exact: true})).toHaveCount(0);
      expect(bodies).toEqual([{clusterId: 'demo', tenantId: 'a', name: 'settle', type, expression,
        jobTypes: ['charge', 'invoice'], variables: '{"delaySeconds":30}'}]);
    });
  }

  test('Timer task options reset with tenant and cluster changes', async ({page}) => {
    await readySubscriptions(page);
    await page.route('**/api/v1/broker/clusters', route => route.fulfill({json: {
      count: 2, clusters: [config, {...config, clusterId: 'target'}].map(item => ({
        ...item, registeredBrokerCount: 1, onlineBrokerCount: 1,
      })),
    }}));
    const dialog = await open(page, 'timer', '?clusterId=demo&tenantId=a');
    await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
    await page.locator('.ant-select-item-option').filter({hasText: /^charge$/}).click();
    await dialog.getByLabel('所属租户', {exact: true}).fill('b');
    await expect(dialog.locator('.ant-select-selection-item').filter({hasText: /^charge$/})).toHaveCount(0);
    await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
    await expect(page.locator('.ant-select-dropdown:visible .ant-select-item-option')).toHaveCount(1);
    await page.locator('.ant-select-item-option').filter({hasText: /^other-tenant$/}).click();
    await dialog.getByLabel('订阅名称', {exact: true}).click();
    await dialog.getByRole('combobox', {name: '所属集群', exact: true}).press('ArrowDown');
    await page.locator('.ant-select-dropdown:visible .ant-select-item-option').filter({hasText: /^target$/}).click();
    await expect(dialog.locator('.ant-select-selection-item').filter({hasText: /^other-tenant$/})).toHaveCount(0);
    await dialog.getByLabel('所属租户', {exact: true}).fill('');
    await expect(dialog.getByText('该集群和租户暂无已完成分发的任务订阅，请先创建任务订阅并等待分发完成。')).toBeVisible();
    await expect(dialog.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  });

  test('Timer rejection retains inputs and unknown outcome prevents duplicate submissions', async ({page}) => {
    await readySubscriptions(page);
    let writes = 0;
    await page.route('**/api/v1/broker/timer-subscriptions', route => {
      if (route.request().method() !== 'POST') return route.fallback();
      writes++;
      return route.fulfill(writes === 1
        ? {status: 400, json: {error: 'INVALID_ARGUMENT', message: 'Invalid timer expression'}}
        : {status: 503, json: {error: 'TIMEOUT', message: 'Admin request timed out before completion'}});
    });
    const dialog = await open(page, 'timer', '?clusterId=demo&tenantId=a');
    await dialog.getByLabel('订阅名称', {exact: true}).fill('settle');
    await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
    await page.locator('.ant-select-item-option').filter({hasText: /^charge$/}).click();
    await dialog.getByLabel('定时表达式', {exact: true}).fill('PT30S');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog.getByText('INVALID_ARGUMENT: Invalid timer expression')).toBeVisible();
    await expect(dialog.getByLabel('订阅名称', {exact: true})).toHaveValue('settle');
    await dialog.getByLabel('定时表达式', {exact: true}).fill('PT30S');
    await dialog.getByRole('button', {name: '创建', exact: true}).click();
    await expect(dialog.getByText('TIMEOUT: Admin request timed out before completion')).toBeVisible();
    await expect(dialog.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
    expect(writes).toBe(2);
  });

  test('Timer creation blocks repeated clicks while request is pending', async ({page}) => {
    await readySubscriptions(page);
    let writes = 0;
    let release: (() => void) | undefined;
    await page.route('**/api/v1/broker/timer-subscriptions', async route => {
      if (route.request().method() !== 'POST') return route.fallback();
      writes++;
      await new Promise<void>(resolve => {release = resolve;});
      return route.fulfill({status: 201, json: {timerSubscriptionKey: '9007199254741099', dueDate: '1793400000000'}});
    });
    const dialog = await open(page, 'timer', '?clusterId=demo&tenantId=a');
    await dialog.getByLabel('订阅名称', {exact: true}).fill('settle');
    await dialog.getByRole('combobox', {name: '任务类型', exact: true}).click();
    await page.locator('.ant-select-item-option').filter({hasText: /^charge$/}).click();
    await dialog.getByLabel('定时表达式', {exact: true}).fill('PT30S');
    await dialog.getByRole('button', {name: '创建', exact: true}).dblclick();
    await expect.poll(() => writes).toBe(1);
    await expect(dialog.getByLabel('订阅名称', {exact: true})).toBeDisabled();
    release!();
    await expect(dialog).not.toBeVisible();
    await expect(page.locator('.ant-table-tbody')).toContainText('9007199254741099');
    expect(writes).toBe(1);
  });
});

const config = {clusterId: 'demo', clusterKey: 1, clusterSize: 3, partitionSize: 5, replicationFactor: 3, voterRoster: ['b0', 'b1', 'b2']};
const worker = {clusterId: 'demo', tenantId: 'a', workerId: 'same-worker', name: 'payment', jobType: 'pay',
  workerLeaseToken: '9007199254741099', leaseDeadlineEpochMillis: 1793062860000,
  maxJobsActive: 32, activeJobs: 7, concurrency: 2, timeoutMillis: 30000, requestTimeoutMillis: 10000,
  pollIntervalMillis: 500, fetchVariables: ['orderId'], clientVersion: '0.1', registeredAt: '2026-10-27T01:00:00Z',
  lastRenewal: '2026-10-27T01:00:01Z', statusChangedAt: '2026-10-27T01:00:00Z', renewalTimeoutMillis: 30000, status: 'ONLINE'};
const report = {partitionId: 1, role: 'LEADER', term: 4, lifecycleState: 'ACTIVE', matchIndex: 100, lag: 2,
  readyForPromotion: false, lastOpId: '', lastOpStatus: '', priority: 100, recoveryEpoch: 0,
  election: {configurationIndex: 3, configurationDigest: 'd', desiredPriority: 100, targetPriority: 100,
    nodeState: 'LEADER', leaderId: 'b0', serviceReady: true, observedAtEpochMillis: 1793062800000}};
const topology = {...config, version: 1, updatedAt: worker.registeredAt,
  brokers: [{brokerId: 'b0', version: '0.1', host: '127.0.0.1', port: 26501, raftPort: 26502,
    registeredAt: worker.registeredAt, lastRenewal: worker.lastRenewal, renewalTimeoutMillis: 30000, status: 'ONLINE', partitions: [report]}],
  partitions: [{partitionId: 1, leaderId: 'b0', term: 4, recoveryEpoch: 0, replicas: ['b0', 'missing-voter'], learners: []}]};
const change = {changeId: 'change-1', status: 'IN_PROGRESS', changeType: 'CLUSTER', cancellable: true,
  cancellationReason: '', currentTopology: [], expectedTopology: [], plannedChanges: [], pendingChanges: []};
const pdChange = {changeId: 'pd-change', status: 'COMPLETED', sourceEpoch: 1, targetEpoch: 2, targetCount: 1,
  targetVoters: ['pd-0'], actualVoters: ['pd-0'], actualLearners: [], operations: [], plannedChanges: [], pendingChanges: []};
const resource = {distributionKey: '9007199254741052', sourceClusterId: 'demo', clusterId: 'demo', creationType: 'SUBSCRIPTION', valueType: 'JOB_SUBSCRIPTION',
  name: 'order', jobType: 'charge', tenantId: 'a',
  subscription: {clusterId: 'demo', name: 'order', jobType: 'charge', tenantId: 'a', variables: '{"amount":42}',
    retries: 3, priority: 50},
  generation: '0', lastRedistributedAt: '0', status: 'DISTRIBUTING',
  targets: [{clusterId: 'demo', expectedPartitions: 2, acknowledgedPartitions: 1,
    pendingPartitions: [2], confirmedPartitionIds: [1], status: 'DISTRIBUTING'},
  {clusterId: 'target', expectedPartitions: 1, acknowledgedPartitions: 0,
    pendingPartitions: [1], confirmedPartitionIds: [], status: 'DISTRIBUTING', mirrorKey: '42'}]};
const resourceAssets = [
  {...resource, targets: [resource.targets[0]]},
  {...resource, clusterId: 'target', creationType: 'MIRROR', targets: [resource.targets[1]]},
];
const timerSubscription = {timerSubscriptionKey: '9007199254741099', clusterId: 'demo', tenantId: 'a',
  name: 'settle', type: 'CYCLE', expression: 'R/PT30S',
  jobTypes: ['charge', 'invoice'], variables: '{"delaySeconds":30}', timeZone: 'Asia/Shanghai',
  dueDate: 1793400000000, repetitions: -1, occurrence: '0'};
async function api(page: Page) {
  await page.route('**/api/v1/**', async route => {
    const path = new URL(route.request().url()).pathname.replace('/api/v1', '');
    const responses: Record<string, unknown> = {
      '/console/bootstrap': {productName: 'Taskite Console', consoleBasePath: '/console/', apiBasePath: '/api/v1',
        buildVersion: 'test', deploymentId: 'isolated', nodeId: 'pd-0'},
      '/pd/health': {status: 'UP', nodeId: 'pd-0', role: 'LEADER', leaderId: 'pd-0', host: 'localhost',
        port: 19001, grpcPort: 26501, term: 1, quorum: 1, peerCount: 1, commitIndex: 2, lastApplied: 2, lastLogIndex: 2},
      '/pd/topology': {localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 1,
        members: [{memberId: 'pd-0', endpoint: 'localhost:26502', role: 'VOTER', self: true}]},
      '/pd/configuration': {epoch: 1, target: {count: 1, voters: ['pd-0']}, actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0'], learners: []}},
      '/pd/changes/pd-change': pdChange,
      '/broker/clusters': {count: 1, clusters: [{...config, registeredBrokerCount: 1, onlineBrokerCount: 1}]},
      '/broker/clusters/demo/configuration': config,
      '/broker/clusters/demo/topology': topology,
      '/broker/clusters/demo/changes/change-1': change,
      '/workers': {generatedAt: worker.lastRenewal, total: 2, online: 1, offline: 1,
        workers: [worker, {...worker, tenantId: 'b', status: 'OFFLINE'}]},
      '/broker/job-subscription-mirrors': {mirrors: [{mirrorKey: 42, sourceClusterId: 'demo', targetClusterId: 'target', status: 'ACTIVE'}]},
      '/broker/job-subscription-mirrors/42': {mirrorKey: 42, sourceClusterId: 'demo', targetClusterId: 'target', status: 'ACTIVE'},
      '/broker/job-subscriptions': {total: 2, subscriptions: resourceAssets},
      '/broker/timer-subscriptions': {total: 1, subscriptions: [timerSubscription]},
      '/broker/job-subscriptions/9007199254741052': resource,
    };
    if (path.endsWith('/changes/active')) return route.fulfill({status: 404, json: {error: path.startsWith('/pd') ? 'ACTIVE_PD_MEMBER_CHANGE_NOT_FOUND' : 'ACTIVE_CHANGE_NOT_FOUND'}});
    if (responses[path]) return route.fulfill({json: responses[path]});
    return route.fulfill({status: 404, json: {error: 'NOT_FOUND'}});
  });
}
test.beforeEach(async ({page}) => { await api(page); });

test('Change tracking tabs separate plans and history and return to the matching tab', async ({page}) => {
  await page.addInitScript(() => localStorage.setItem('taskite-console:isolated:known-changes', JSON.stringify([
    {scope: 'pd', changeId: 'pd-change'},
    {scope: 'cluster', clusterId: 'demo', changeId: 'change-1'},
    {scope: 'subscription', clusterId: 'demo', changeId: '9007199254741052'},
  ].map(record => ({...record, firstSeenAt: '2026-11-01T00:00:00Z', lastSeenAt: '2026-11-01T00:00:00Z'})))));
  await page.route('**/api/v1/pd/changes/active', route => route.fulfill({json: pdChange}));
  await page.route('**/api/v1/broker/clusters/demo/changes/active', route => route.fulfill({json: change}));
  await go(page, '/changes');
  const current = page.locator('.ant-card').filter({has: page.getByText('当前变更', {exact: true})});
  const history = page.locator('.ant-card').filter({has: page.getByText('访问记录 (1)', {exact: true})});
  await expect(page.getByRole('tab')).toHaveText(['PD 拓扑变更', 'Broker 拓扑变更', '订阅创建/删除']);
  await expect(current).toContainText('pd-change');
  await expect(current).toContainText('目标 Voters: pd-0');
  await expect(history).toContainText('pd-change');
  await expect(history).not.toContainText('change-1');
  await expect(page.getByText('已知 ID 查询', {exact: true})).toHaveCount(0);

  await page.getByRole('tab', {name: 'Broker 拓扑变更', exact: true}).click();
  await expect(current).toContainText('change-1');
  await expect(current).not.toContainText('pd-change');
  await expect(history).toContainText('change-1');
  await current.getByRole('button', {name: '查看详情', exact: true}).click();
  await expect(page).toHaveURL(/\/changes\/clusters\/demo\/change-1$/);
  await page.getByRole('button', {name: '返回列表', exact: true}).click();
  await expect(page.getByRole('tab', {name: 'Broker 拓扑变更', exact: true})).toHaveAttribute('aria-selected', 'true');
  await expect(current).toContainText('change-1');

  await page.getByRole('tab', {name: '订阅创建/删除', exact: true}).click();
  await expect(current.locator('.ant-table-row')).toHaveCount(1);
  await expect(current).toContainText('创建');
  await expect(current).toContainText('demo: 1 / 2');
  await expect(current).toContainText('target: 0 / 1');
  await expect(history).toContainText(resource.distributionKey);
  await expect(history).not.toContainText('change-1');
  await current.getByRole('button', {name: '查看详情', exact: true}).click();
  await expect(page.getByRole('heading', {name: '创建任务订阅', exact: true})).toBeVisible();
  await expect(page.getByText('此 Key 对应任务订阅创建分发记录', {exact: false})).toHaveCount(0);
  await page.getByRole('button', {name: '返回列表', exact: true}).click();
  await expect(page.getByRole('tab', {name: '订阅创建/删除', exact: true})).toHaveAttribute('aria-selected', 'true');
});

test('Change tracking ID header queries retained plans and resets filters on tab changes', async ({page}) => {
  await go(page, '/changes');
  await page.getByLabel('按变更 ID筛选', {exact: true}).click();
  await page.getByRole('textbox', {name: '按变更 ID筛选', exact: true}).fill('pd-change');
  await page.getByRole('textbox', {name: '按变更 ID筛选', exact: true}).press('Enter');
  const result = page.locator('.ant-card').filter({has: page.getByText('查询结果', {exact: true})});
  await expect(result).toContainText('pd-change');
  await expect(result).toContainText('已完成');
  await expect(page).toHaveURL(/\/changes\?changeId=pd-change$/);
  await page.reload();
  await expect(result).toContainText('pd-change');
  await result.locator('.ant-table-row-expand-icon').click();
  await expect(page.getByText('成员配置对比', {exact: true})).toBeVisible();
  await expect(page.getByText('操作计划', {exact: true})).toBeVisible();
  await page.getByRole('button', {name: '清除筛选', exact: true}).click();
  await expect(page.getByText('暂无当前变更', {exact: true})).toBeVisible();
  await page.getByLabel('按变更 ID筛选', {exact: true}).click();
  await page.getByRole('textbox', {name: '按变更 ID筛选', exact: true}).fill('not-retained');
  await page.locator('.worker-column-filter').getByRole('button', {name: '筛选', exact: true}).click();
  await expect(page.locator('.ant-alert')).toContainText('NOT_FOUND');
  await expect(result).not.toContainText('pd-change');
  await page.getByRole('tab', {name: '订阅创建/删除', exact: true}).click();
  await expect(page).toHaveURL(/\/changes\?tab=subscription$/);
  await expect(page.locator('.ant-card').first()).toContainText(resource.distributionKey);
  await expect(page.locator('.ant-alert')).toHaveCount(0);
});

test('Change tracking Broker plans aggregate clusters and query IDs with cluster header filters', async ({page}) => {
  const op = {opId: 'op:1', type: 'PARTITION_ADD_LEARNER', targetBroker: 'b1', priority: 100};
  await page.route('**/api/v1/broker/clusters', route => route.fulfill({json: {count: 2,
    clusters: [config, {...config, clusterId: 'subscription'}].map(cluster =>
      ({...cluster, registeredBrokerCount: 1, onlineBrokerCount: 1}))}}));
  await page.route('**/api/v1/broker/clusters/demo/changes/active', route => route.fulfill({json: {...change, plannedChanges: [op]}}));
  await page.route('**/api/v1/broker/clusters/subscription/changes/active', route => route.fulfill({json: change}));
  await page.route('**/api/v1/broker/clusters/subscription/changes/change-1', route => route.fulfill({
    status: 404, json: {error: 'CHANGE_NOT_FOUND', message: 'not in this cluster'},
  }));
  await go(page, '/changes?tab=broker');
  const card = page.locator('.ant-card').first();
  await expect(card.locator('.ant-table-row')).toHaveCount(2);
  await card.locator('.ant-table-row-expand-icon').first().click();
  await expect(card).toContainText('op:1');
  await expect(card).toContainText('PARTITION_ADD_LEARNER');
  await page.getByLabel('按变更 ID筛选', {exact: true}).click();
  await page.getByRole('textbox', {name: '按变更 ID筛选', exact: true}).fill('change-1');
  await page.locator('.worker-column-filter').getByRole('button', {name: '筛选', exact: true}).click();
  await expect(card.locator('.ant-table-row')).toHaveCount(1);
  await expect(card).toContainText('demo');
  await expect(page.locator('.ant-alert')).toHaveCount(0);
  await page.getByLabel('按集群 ID筛选', {exact: true}).click();
  await page.getByRole('textbox', {name: '按集群 ID筛选', exact: true}).fill('subscription');
  await page.locator('.worker-column-filter').getByRole('button', {name: '筛选', exact: true}).click();
  await expect(card).toContainText('暂无符合条件的变更计划');
  await page.getByRole('button', {name: '清除筛选', exact: true}).click();
  await expect(card.locator('.ant-table-row')).toHaveCount(2);
});

test('Change tracking subscription plans deduplicate creation and deletion targets', async ({page}) => {
  const deletionKey = '9007199254741053';
  const deletion = {...resource, distributionKey: deletionKey, subscriptionKey: resource.distributionKey,
    valueType: 'JOB_SUBSCRIPTION_DELETION'};
  await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
    total: 4, subscriptions: [resourceAssets[0], {...resourceAssets[1], status: 'COMPLETED'},
      ...resourceAssets.map(asset => ({...asset, distributionKey: '9007199254741060',
        status: 'DELETING', deletionDistributionKey: deletionKey}))],
  }}));
  await page.route(`**/api/v1/broker/job-subscriptions/${deletionKey}`, route => route.fulfill({json: deletion}));
  await go(page, '/changes?tab=subscription');
  const current = page.locator('.ant-card').first();
  const rows = current.locator('.ant-table-row').filter({has: page.getByRole('button', {name: '查看详情', exact: true})});
  await expect(rows).toHaveCount(2);
  await expect(rows.first()).toContainText('创建');
  await expect(rows.first()).toContainText('target: 0 / 1');
  await expect(rows.last()).toContainText('删除');
  await expect(rows.last()).toContainText(deletionKey);
  await rows.last().locator('.ant-table-row-expand-icon').click();
  await expect(current.locator('.ant-table-expanded-row .ant-table-row')).toHaveCount(2);
  await rows.last().getByRole('button', {name: '查看详情', exact: true}).click();
  await expect(page.getByRole('heading', {name: '删除任务订阅', exact: true})).toBeVisible();
});

test('Job yield history preserves tokens and partition progress across navigation', async ({page}) => {
  await page.route('**/api/v1/workers', route => route.fulfill({
    json: {generatedAt: worker.lastRenewal, total: 1, online: 0, offline: 1,
      workers: [{...worker, status: 'OFFLINE'}]},
  }));
  const yieldId = '9007199254741101';
  const plan = {yieldId, clusterId: 'demo', tenantId: 'a', workerId: worker.workerId,
    workerName: worker.name, jobType: worker.jobType, workerLeaseToken: worker.workerLeaseToken,
    reason: 'WORKER_LEASE_EXPIRED', status: 'PENDING', createdAtEpochMillis: 1793062860000,
    completedAtEpochMillis: 0, releasedJobCount: 12, totalPartitions: 2, acknowledgedPartitions: 1,
    workerSnapshot: {...worker, status: 'OFFLINE'}, partitions: [
      {partitionId: 1, status: 'COMPLETED', releasedJobCount: 12, attempts: 2,
        lastAttemptAtEpochMillis: 1793062860000, nextAttemptAtEpochMillis: 0, acknowledgedAtEpochMillis: 1793062861000, lastError: '', pauseReason: ''},
      {partitionId: 2, status: 'PENDING', releasedJobCount: 0, attempts: 3,
        lastAttemptAtEpochMillis: 1793062860000, nextAttemptAtEpochMillis: 1793062890000, acknowledgedAtEpochMillis: 0, lastError: 'FAILED_PRECONDITION', pauseReason: 'FEATURE_DISABLED'},
    ]};
  await page.route('**/api/v1/workers/yields**', route => {
    const url = new URL(route.request().url());
    return route.fulfill({json: url.pathname.endsWith(`/${yieldId}`) ? plan :
      {generatedAt: worker.lastRenewal, total: 1, yields: [plan]}});
  });
  await go(page, '/workers');
  await page.getByRole('button', {name: 'payment', exact: true}).first().click();
  const drawer = page.locator('.ant-drawer-body');
  await expect(drawer.getByText(worker.workerLeaseToken, {exact: true})).toBeVisible();
  await drawer.getByRole('link', {name: '查看释放记录', exact: true}).click();
  await expect(page.getByRole('heading', {name: '任务释放记录', exact: true})).toBeVisible();
  await expect(page).toHaveURL(new RegExp(`workerLeaseToken=${worker.workerLeaseToken}$`));
  await expect(page.getByRole('link', {name: yieldId})).toBeVisible();
  await page.getByRole('link', {name: yieldId}).click();
  await expect(page.getByRole('heading', {name: '任务释放详情', exact: true})).toBeVisible();
  await expect(page.getByText('Worker 会话快照', {exact: true})).toBeVisible();
  await expect(page.getByText('FEATURE_DISABLED', {exact: true})).toBeVisible();
  await expect(page.getByText(worker.workerLeaseToken, {exact: true})).toBeVisible();
  await page.reload();
  await expect(page.getByText('目标分区确认', {exact: true})).toBeVisible();
  await expect(page.getByText('FEATURE_DISABLED', {exact: true})).toBeVisible();
  await page.getByRole('button', {name: '返回'}).click();
  await expect(page).toHaveURL(new RegExp(`workerLeaseToken=${worker.workerLeaseToken}$`));
  await page.getByRole('button', {name: '按Token筛选', exact: true}).click();
  const filter = page.getByRole('group', {name: 'Token筛选'});
  await expect(filter.getByRole('textbox')).toHaveValue(worker.workerLeaseToken);
  await filter.getByRole('button', {name: '重置', exact: true}).click();
  await expect(page).toHaveURL(/#\/workers\/yields\?cluster=demo&tenant=a&workerId=same-worker$/);
});

for (const theme of ['light', 'dark']) {
  test(`Worker yield entry follows offline registration lifecycle (${theme})`, async ({page}) => {
    const longWorker = {...worker, workerId: 'f76b32982db3ac042268e348c48352abc'};
    let registrations = [longWorker];
    await page.route('**/api/v1/workers', route => route.fulfill({
      json: {generatedAt: worker.lastRenewal, total: registrations.length,
        online: registrations.filter(row => row.status === 'ONLINE').length,
        offline: registrations.filter(row => row.status === 'OFFLINE').length,
        workers: registrations},
    }));
    await go(page, '/workers');
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await page.getByRole('button', {name: 'payment', exact: true}).click();
    const drawer = page.locator('.ant-drawer-body');
    const history = drawer.getByRole('link', {name: '查看释放记录', exact: true});
    await expect(drawer.getByText(worker.workerLeaseToken, {exact: true})).toBeVisible();
    await expect(history).toHaveCount(0);
    await expect(drawer.getByText('任务释放', {exact: true})).toHaveCount(0);
    registrations = [{...longWorker, status: 'OFFLINE'}];
    await page.reload();
    await expect(history).toBeVisible();
    for (const width of [1440, 390]) {
      await page.setViewportSize({width, height: 1000});
      const label = drawer.locator('.ant-descriptions-item-label').filter({hasText: /^任务释放/});
      const labelBounds = await label.boundingBox();
      const linkBounds = await history.boundingBox();
      expect(Math.abs((labelBounds!.y + labelBounds!.height / 2) - (linkBounds!.y + linkBounds!.height / 2))).toBeLessThanOrEqual(2);
      await expect(history.locator('svg.lucide-arrow-up-right')).toHaveCount(1);
      const typography = await history.evaluate(link => {
        const text = getComputedStyle(link);
        const parent = getComputedStyle(link.parentElement!);
        return {font: text.fontSize, weight: text.fontWeight, line: text.lineHeight,
          parentFont: parent.fontSize, parentWeight: parent.fontWeight, parentLine: parent.lineHeight};
      });
      expect([typography.font, typography.weight, typography.line])
        .toEqual([typography.parentFont, typography.parentWeight, typography.parentLine]);
      const columns = await drawer.locator('.worker-detail-fields').evaluateAll(sections =>
        sections.flatMap(section => Array.from(section.querySelectorAll('.ant-descriptions-item-content')).map(content => {
          const bounds = content.getBoundingClientRect();
          const sectionBounds = section.getBoundingClientRect();
          return {left: bounds.left, right: bounds.right, sectionRight: sectionBounds.right};
        })));
      expect(Math.max(...columns.map(value => value.left)) - Math.min(...columns.map(value => value.left))).toBeLessThanOrEqual(1);
      expect(columns.every(value => value.right <= value.sectionRight + 1)).toBeTruthy();
      const identifier = drawer.locator('.worker-registration-details .copyable-id').first();
      await expect(identifier.getByRole('button', {name: `复制 ${longWorker.workerId}`})).toBeVisible();
      const identifierBounds = await identifier.boundingBox();
      expect(identifierBounds!.x + identifierBounds!.width).toBeLessThanOrEqual(columns[1].right + 1);
      await page.locator('.ant-drawer-content-wrapper').screenshot({path: `/tmp/taskite-worker-release-${theme}-${width}.png`});
    }
    registrations = [];
    await page.reload();
    await expect(page.getByText('该注册记录已不在当前查询结果中', {exact: true})).toBeVisible();
    await expect(page.locator('.ant-drawer-content-wrapper')).toBeHidden();
    await expect(page.getByRole('link', {name: '查看释放记录', exact: true})).toHaveCount(0);
  });
}

for (const theme of ['light', 'dark']) {
  test(`Job yield header filters compose, validate tokens and reset empty results (${theme})`, async ({page}) => {
    const plan = {yieldId: '9007199254741101', clusterId: worker.clusterId, tenantId: worker.tenantId,
      workerId: worker.workerId, workerName: worker.name, jobType: worker.jobType,
      workerLeaseToken: worker.workerLeaseToken, status: 'PENDING', reason: 'WORKER_LEASE_EXPIRED',
      createdAtEpochMillis: 1793062860000, completedAtEpochMillis: 0,
      releasedJobCount: 0, totalPartitions: 1, acknowledgedPartitions: 0,
      workerSnapshot: {...worker, status: 'OFFLINE'}, partitions: []};
    const completed = {...plan, yieldId: '9007199254741102', status: 'COMPLETED', acknowledgedPartitions: 1};
    await page.route('**/api/v1/workers/yields**', route => {
      const query = new URL(route.request().url()).searchParams;
      const matches = [['cluster', plan.clusterId], ['tenant', plan.tenantId],
        ['workerId', plan.workerId], ['workerLeaseToken', plan.workerLeaseToken]]
        .every(([key, value]) => !query.has(key) || query.get(key) === value);
      return route.fulfill({json: {generatedAt: worker.lastRenewal, total: matches ? 2 : 0, yields: matches ? [plan, completed] : []}});
    });
    await go(page, '/workers/yields');
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await expect(page.getByRole('link', {name: plan.yieldId})).toBeVisible();
    await expect(page.getByRole('textbox')).toHaveCount(0);
    await expect(page.locator('form')).toHaveCount(0);
    for (const [label, key, value] of [
      ['集群', 'cluster', plan.clusterId], ['租户', 'tenant', plan.tenantId],
      ['Worker ID', 'workerId', plan.workerId], ['Token', 'workerLeaseToken', plan.workerLeaseToken],
    ]) {
      await page.getByRole('button', {name: `按${label}筛选`, exact: true}).click();
      const filter = page.getByRole('group', {name: `${label}筛选`});
      await filter.getByRole('textbox').fill(` ${value} `);
      await filter.getByRole('textbox').press('Enter');
      await expect(page).toHaveURL(new RegExp(`${key}=${value}`));
      await expect(page.getByRole('link', {name: plan.yieldId})).toBeVisible();
    }
    const applied = page.url();
    await page.getByRole('button', {name: '按Token筛选', exact: true}).click();
    const tokenFilter = page.getByRole('group', {name: 'Token筛选'});
    await expect(tokenFilter.getByRole('textbox')).toHaveValue(worker.workerLeaseToken);
    await tokenFilter.getByRole('textbox').fill('1.5');
    await tokenFilter.getByRole('button', {name: '筛选', exact: true}).click();
    await expect(tokenFilter.getByRole('alert')).toHaveText('请输入正整数 Token');
    await expect(page).toHaveURL(applied);
    await tokenFilter.getByRole('textbox').fill('9007199254741100');
    await tokenFilter.getByRole('button', {name: '筛选', exact: true}).click();
    await expect(page.getByText('暂无匹配的任务释放记录', {exact: true})).toBeVisible();
    await page.getByRole('button', {name: '按Token筛选', exact: true}).click();
    await tokenFilter.getByRole('button', {name: '重置', exact: true}).click();
    await expect(page).toHaveURL(/#\/workers\/yields\?cluster=demo&tenant=a&workerId=same-worker$/);
    await expect(page.getByRole('link', {name: plan.yieldId})).toBeVisible();
    await page.reload();
    await page.getByRole('button', {name: '按集群筛选', exact: true}).click();
    await expect(page.getByRole('group', {name: '集群筛选'}).getByRole('textbox')).toHaveValue('demo');
    await page.getByRole('group', {name: '集群筛选'}).getByRole('button', {name: '筛选', exact: true}).click();
    await page.getByRole('button', {name: '按状态筛选', exact: true}).click();
    const statusFilter = page.locator('.ant-table-filter-dropdown').filter({has: page.getByRole('menu')});
    await expect(statusFilter.getByRole('menu')).toBeVisible();
    await statusFilter.getByRole('menuitem', {name: '已完成', exact: true}).click();
    await statusFilter.getByRole('button', {name: /^筛\s*选$/}).click();
    await expect(page).toHaveURL(/workerId=same-worker&status=COMPLETED$/);
    await expect(page.getByRole('link', {name: plan.yieldId})).toHaveCount(0);
    await expect(page.getByRole('link', {name: completed.yieldId})).toBeVisible();
    await page.reload();
    await expect(page.getByRole('link', {name: plan.yieldId})).toHaveCount(0);
    await expect(page.getByRole('link', {name: completed.yieldId})).toBeVisible();
    await page.getByRole('button', {name: '按状态筛选', exact: true}).click();
    await expect(statusFilter.getByRole('menu')).toBeVisible();
    await statusFilter.getByRole('button', {name: /^重\s*置$/}).click();
    await statusFilter.getByRole('button', {name: /^筛\s*选$/}).click();
    await expect(page.getByRole('link', {name: plan.yieldId})).toBeVisible();
    await page.setViewportSize({width: 390, height: 844});
    await page.getByRole('button', {name: '收起导航', exact: true}).click();
    await expect(page.locator('.ant-layout-sider')).toHaveCSS('width', '64px');
    await page.getByRole('button', {name: '按Token筛选', exact: true}).click();
    await expect(tokenFilter.getByRole('textbox')).toBeVisible();
    await expect(tokenFilter.getByRole('button', {name: '重置', exact: true})).toBeVisible();
    await expect.poll(async () => Math.round((await tokenFilter.boundingBox())?.width ?? 0)).toBe(240);
    await expect(page.locator('.ant-dropdown').filter({has: tokenFilter})).toHaveCSS('opacity', '1');
    const popup = await tokenFilter.boundingBox();
    expect(popup!.x).toBeGreaterThanOrEqual(0);
    expect(popup!.x + popup!.width).toBeLessThanOrEqual(390);
    await page.screenshot({path: `/tmp/taskite-yield-header-${theme}-mobile.png`, fullPage: true});
    await tokenFilter.getByRole('button', {name: '重置', exact: true}).click();
    await expect(tokenFilter).toBeHidden();
    await page.setViewportSize({width: 1600, height: 1000});
    await page.reload();
    await expect(page.getByRole('link', {name: plan.yieldId})).toBeVisible();
    await expect(page.locator('.ant-spin-spinning')).toBeHidden();
    await page.screenshot({path: `/tmp/taskite-yield-header-${theme}.png`, fullPage: true});
  });
}

test('successor history preserves generations and links without losing the original plan', async ({page}) => {
  const operation = {opId: 'old:promote', type: 'PROMOTE_LEARNER', targetBroker: 'b4', priority: 10};
  const old = {changeId: 'change-1', partitionId: 1, status: 'SUPERSEDED', terminal: true,
    executionGeneration: '9007199254740993', authorityIndex: '9007199254740995', rootChangeId: 'change-1',
    supersedes: '', supersededBy: 'successor:1:next', blockedReason: 'TARGET_UNAVAILABLE',
    blockedSince: '2026-10-29T01:00:00Z', nextOperationIndex: 1, operationCount: 3,
    remainingOperations: [operation], startedAt: worker.registeredAt, deadline: worker.lastRenewal};
  const successor = {...old, changeId: 'successor:1:next', executionGeneration: '9007199254740994',
    status: 'SUPERSEDING', terminal: false, supersedes: 'change-1', supersededBy: '',
    blockedReason: 'NO_QUORUM'};
  await page.route('**/api/v1/broker/clusters/demo/changes/*', route => route.fulfill({
    contentType: 'application/json',
    body: JSON.stringify({...change, plannedChanges: [operation], pendingChanges: [successor], executionHistory: [old, successor]})
      .replace(/"(executionGeneration|authorityIndex)":"(\d+)"/g, '"$1":$2'),
  }));
  await go(page, '/changes/clusters/demo/change-1');
  await expect(page.getByText('执行历史', {exact: true})).toBeVisible();
  await expect(page.getByText('已被替代', {exact: true})).toBeVisible();
  await expect(page.getByText('9007199254740993', {exact: true})).toBeVisible();
  const history = page.locator('.ant-card').filter({hasText: '执行历史'});
  await history.locator('.ant-table-row-expand-icon').first().click();
  await page.getByRole('button', {name: 'successor:1:next', exact: true}).click();
  await expect(page).toHaveURL(/changes\/clusters\/demo\/successor%3A1%3Anext/);
  await expect(page.locator('.ant-card-head-title').getByText('原始计划', {exact: true})).toBeVisible();
  await expect(history.getByText('已提交配置的在线投票节点不足多数派，等待恢复', {exact: true})).toBeVisible();
});

test('small numeric authority values from HTTP satisfy the exact int64 contract', async ({page}) => {
  const current = {partitionId: 1, changeId: 'child', status: 'SUPERSEDING', terminal: false,
    executionGeneration: 2, authorityIndex: 12, rootChangeId: 'change-1', supersedes: 'old', supersededBy: '',
    blockedReason: 'NO_QUORUM', nextOperationIndex: 0, operationCount: 1, remainingOperations: []};
  await page.route('**/api/v1/broker/clusters/demo/changes/change-1', route => route.fulfill({
    json: {...change, pendingChanges: [current], executionHistory: [current]},
  }));
  await go(page, '/changes/clusters/demo/change-1');
  await expect(page.getByText('执行历史', {exact: true})).toBeVisible();
  await expect(page.getByText('已提交配置的在线投票节点不足多数派，等待恢复', {exact: true})).toBeVisible();
  await expect(page.getByText('响应字段', {exact: false})).toHaveCount(0);
});

const go = (page: Page, path: string) => page.goto(`/console/#${path}`);
for (const theme of ['light', 'dark']) {
  test(`draining Broker badge follows product theme in table and drawer (${theme})`, async ({page}) => {
    await page.route('**/api/v1/broker/clusters/demo/topology', route => route.fulfill({
      json: {...topology, brokers: [{...topology.brokers[0], status: 'DRAINING'}]},
    }));
    await go(page, '/clusters/demo');
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await page.getByRole('tab', {name: 'Broker', exact: true}).click();
    const row = page.locator('.broker-list-table tr[data-row-key="b0"]');
    const badge = row.locator('.ant-tag', {hasText: /^退出中$/});
    await expect(badge).toBeVisible();
    await expect(badge.locator('svg.lucide-arrow-right-circle')).toHaveCount(1);
    await expect(badge.locator('svg.lucide-help-circle')).toHaveCount(0);
    const colors = await page.evaluate(() => {
      const probe = document.createElement('span');
      probe.style.color = 'hsl(var(--primary))';
      probe.style.backgroundColor = 'hsl(var(--primary-muted))';
      document.body.appendChild(probe);
      const style = getComputedStyle(probe);
      const result = {color: style.color, background: style.backgroundColor};
      probe.remove();
      return result;
    });
    await expect(badge).toHaveCSS('color', colors.color);
    await expect(badge).toHaveCSS('background-color', colors.background);
    await expect(badge).toHaveCSS('border-radius', '100px');
    await row.getByRole('button', {name: /详情/}).click();
    const drawerBadge = page.locator('.ant-drawer .ant-tag', {hasText: /^退出中$/});
    await expect(drawerBadge).toBeVisible();
    await expect(drawerBadge).toHaveCSS('color', colors.color);
    await expect(drawerBadge).toHaveCSS('background-color', colors.background);
    await page.screenshot({path: `target/draining-broker-${theme}.png`, animations: 'disabled'});
  });
}
const learnerBroker = (brokerId: string) => ({...topology.brokers[0], brokerId,
  partitions: [{...report, role: 'LEARNER'}]});
const healthyTopology = {...topology,
  brokers: config.voterRoster.map(brokerId => ({...topology.brokers[0], brokerId,
    partitions: Array.from({length: config.partitionSize}, (_, i) => ({...report, partitionId: i + 1,
      role: brokerId === 'b0' ? 'LEADER' : 'FOLLOWER'}))})),
  partitions: Array.from({length: config.partitionSize}, (_, i) => ({...topology.partitions[0],
    partitionId: i + 1, replicas: ['b0', 'b1', 'b2']})),
};
async function withLearnerBrokers(page: Page) {
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...healthyTopology,
    brokers: [...healthyTopology.brokers, learnerBroker('b3'), learnerBroker('b4')],
    partitions: healthyTopology.partitions.map(partition => ({...partition, learners: ['b3', 'b4']})),
  }}));
}
async function choose(page: Page, name: string, value: string) {
  const input = page.getByRole('combobox', {name, exact: true});
  if (await input.getAttribute('aria-expanded') !== 'true') {
    await page.locator('.ant-select').filter({has: input}).locator('.ant-select-selector').click();
  }
  await input.fill(value);
  const listId = await input.getAttribute('aria-controls');
  await page.locator('.ant-select-dropdown').filter({has: page.locator(`[id="${listId}"]`)})
    .locator('.ant-select-item-option-content').getByText(value, {exact: true}).click();
}
const clusterList = (ids: string[]) => ({count: ids.length,
  clusters: ids.map(clusterId => ({...config, clusterId, registeredBrokerCount: 0, onlineBrokerCount: 0}))});
async function rebalance(page: Page) {
  await go(page, '/clusters');
  await page.getByRole('button', {name: 'Rebalance', exact: true}).click();
  await page.getByRole('button', {name: '确认执行'}).click();
}
test('all incoming routes display real HTTP snapshots without runtime errors', async ({page}) => {
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  for (const [path, title] of [['/', '集群总览'], ['/pd', 'PD 管理'], ['/clusters', 'Broker 集群'], ['/workers', 'Worker'],
    ['/subscriptions/mirrors', 'Mirror 列表'], ['/subscriptions/distributions', '任务订阅'], ['/changes', '变更追踪']]) {
    await go(page, path);
    await expect(page.getByRole('heading', {name: title, exact: true})).toBeVisible();
    if (path === '/clusters') {
      await expect(page.getByRole('columnheader', {name: 'Cluster Key', exact: true})).toHaveCount(0);
      await expect(page.getByRole('row').filter({hasText: 'demo'}).getByRole('cell').nth(0)).toContainText('demo');
    }
  }
  expect(errors).toEqual([]);
});
test('overview summary cards stay equal in each row without registered Brokers', async ({page}) => {
  let clusters: ClusterFixture[] = [];
  type ClusterFixture = typeof config & {registeredBrokerCount: number; onlineBrokerCount: number};
  await page.route('**/api/v1/broker/clusters', route => route.fulfill({json: {count: clusters.length, clusters}}));
  await page.route('**/api/v1/workers', route => route.fulfill({json: {
    generatedAt: worker.lastRenewal, total: 0, online: 0, offline: 0, workers: [],
  }}));
  const cases: ClusterFixture[][] = [
    [],
    [{...config, registeredBrokerCount: 0, onlineBrokerCount: 0}],
    [{...config, registeredBrokerCount: 3, onlineBrokerCount: 2}],
    [{...config, registeredBrokerCount: 3, onlineBrokerCount: 3},
      {...config, clusterId: 'demo-2', registeredBrokerCount: 5, onlineBrokerCount: 4}],
    Array.from({length: 24}, (_, i) => ({...config,
      clusterId: i === 23 ? 'cluster-with-a-very-long-name-for-broker-summary' : `cluster-${String(i + 1).padStart(2, '0')}`,
      registeredBrokerCount: i % 4, onlineBrokerCount: i % 4})),
  ];
  await go(page, '/');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    for (const snapshot of cases) {
      clusters = snapshot;
      for (const width of [1440, 900, 390]) {
        await page.setViewportSize({width, height: 1000});
        await page.reload();
        const cards = page.locator('.dashboard-summary-row > .ant-col > .ant-card');
        await expect(cards).toHaveCount(3);
        await expect(cards.nth(1).getByText(`${clusters.length} 个集群`, {exact: true})).toBeVisible();
        await expect(cards.nth(1).locator('.ant-statistic').first().locator('.ant-statistic-content'))
          .toHaveText(String(clusters.reduce((sum, cluster) => sum + cluster.registeredBrokerCount, 0)));
        await expect(cards.nth(1).locator('.cluster-count-entry')).toHaveCount(clusters.length);
        for (const cluster of clusters) {
          await expect(cards.nth(1).locator(`[data-cluster-id="${cluster.clusterId}"] .cluster-count-value`))
            .toHaveText(String(cluster.registeredBrokerCount));
        }
        if (clusters.length > 12) await expect(cards.nth(1).locator('.cluster-count-breakdown')).toHaveClass(/cluster-count-dense/);
        const bounds = await cards.evaluateAll(elements => elements.map(element => {
          const rect = element.getBoundingClientRect();
          return {top: rect.top, bottom: rect.bottom, height: rect.height};
        }));
        for (let i = 0; i < bounds.length; i++) {
          for (let j = i + 1; j < bounds.length; j++) {
            if (Math.abs(bounds[i].top - bounds[j].top) < 2) {
              expect(Math.abs(bounds[i].bottom - bounds[j].bottom)).toBeLessThan(2);
              expect(Math.abs(bounds[i].height - bounds[j].height)).toBeLessThan(2);
            } else {
              expect(bounds[j].top).toBeGreaterThanOrEqual(bounds[i].bottom);
            }
          }
        }
        expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
        if (clusters.length === 0 && width === 1440) {
          await page.mouse.move(0, 0);
          await page.screenshot({path: `/tmp/taskite-overview-empty-brokers-${theme}.png`, fullPage: true, animations: 'disabled'});
        }
        if (clusters.length === 2 && width === 1440) {
          await page.mouse.move(0, 0);
          await page.screenshot({path: `target/overview-broker-clusters-${theme}.png`, fullPage: true, animations: 'disabled'});
        }
        if (clusters.length === 24 && width === 1440) {
          await cards.nth(1).screenshot({path: `target/overview-broker-many-clusters-${theme}.png`, animations: 'disabled'});
        }
      }
    }
  }
});
test('PD follower shows members and configuration when no member change is active', async ({page}) => {
  await page.route('**/api/v1/pd/topology', route => route.fulfill({json: {
    localMemberId: 'pd-1', leaderId: 'pd-0', term: 1, quorum: 2,
    members: [
      {memberId: 'pd-0', role: 'LEADER', self: false, status: 'ONLINE'},
      {memberId: 'pd-1', role: 'FOLLOWER', self: true, status: 'ONLINE'},
      {memberId: 'pd-2', role: 'FOLLOWER', self: false, status: 'ONLINE'},
    ],
  }}));
  await page.route('**/api/v1/pd/configuration', route => route.fulfill({json: {
    epoch: 1, target: {count: 3, voters: ['pd-0', 'pd-1', 'pd-2']},
    actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0', 'pd-1', 'pd-2'], learners: []},
  }}));
  await go(page, '/pd');
  await expect(page.getByRole('heading', {name: 'PD 管理', exact: true})).toBeVisible();
  await expect(page.locator('.pd-members-panel .ant-table-row')).toHaveCount(3);
  const currentNode = page.locator('.pd-members-panel .ant-table-row').filter({hasText: 'pd-1'});
  await expect(currentNode).toContainText('当前节点');
  await expect(currentNode).toContainText('Follower');
  await expect(page.getByRole('region', {name: '目标配置'})).toContainText('pd-2');
  await expect(page.getByText('暂无当前变更', {exact: true})).toBeVisible();
  await expect(page.locator('.ant-alert-warning')).toHaveCount(0);
  await page.reload();
  await expect(page.locator('.pd-members-panel .ant-table-row')).toHaveCount(3);
  await expect(page.getByText('暂无当前变更', {exact: true})).toBeVisible();
});
test('theme and collapsed navigation preserve source design', async ({page}) => {
  await go(page, '/');
  await page.getByRole('button', {name: '切换暗色主题'}).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.reload();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
  await page.getByRole('button', {name: '收起导航'}).click();
  await expect(page.locator('.console-sidebar')).toHaveClass(/ant-layout-sider-collapsed/);
  await expect(page.getByRole('img', {name: 'Taskite', exact: true})).toBeVisible();
  await page.screenshot({path: 'target/migrated-console-dark.png', fullPage: true, animations: 'disabled'});
  for (const theme of ['dark', 'light']) {
    if (theme === 'light') await page.getByRole('button', {name: '切换亮色主题'}).click();
    await go(page, '/pd');
    const pdButton = page.getByRole('button', {name: '修改配置', exact: true});
    await expect(pdButton).toBeVisible();
    const pdColors = await pdButton.evaluate(el => [getComputedStyle(el).backgroundColor, getComputedStyle(el).color]);
    await pdButton.click();
    await expect(page.locator('.pd-member-editor .drawer-object-name')).toHaveText('修改配置');
    await page.locator('.pd-member-editor .ant-drawer-close').click();
    await go(page, '/clusters/demo');
    const brokerButton = page.getByRole('button', {name: '修改配置', exact: true});
    await expect(brokerButton).toBeVisible();
    expect(await brokerButton.evaluate(el => [getComputedStyle(el).backgroundColor, getComputedStyle(el).color])).toEqual(pdColors);
    await page.screenshot({path: `/tmp/taskite-config-button-${theme}.png`, fullPage: true, animations: 'disabled'});
    await go(page, '/clusters');
    const balanceButton = page.getByRole('button', {name: 'Rebalance', exact: true});
    await expect(balanceButton).toBeEnabled();
    await page.screenshot({path: `/tmp/taskite-rebalance-button-${theme}.png`, fullPage: true, animations: 'disabled'});
  }
});
test('real API Raft roles preserve reference badge styles in both themes', async ({page}) => {
  await page.route('**/api/v1/pd/topology', route => route.fulfill({json: {
    localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 2,
    members: [
      {memberId: 'pd-0', role: 'LEADER', self: true},
      {memberId: 'pd-1', role: 'FOLLOWER', self: false},
      {memberId: 'pd-2', role: 'Follower', self: false},
    ],
  }}));
  await go(page, '/pd');
  const leader = page.locator('.pd-members-panel .ant-tag', {hasText: /^Leader$/});
  const followers = page.locator('.pd-members-panel .ant-tag', {hasText: /^Follower$/});
  await expect(leader).toHaveClass(/leader-tag/);
  await expect(followers).toHaveCount(2);
  await expect(leader).toHaveCSS('color', 'rgb(180, 83, 9)');
  await expect(leader).toHaveCSS('background-color', 'rgb(255, 244, 220)');
  const actualLeader = page.locator('.pd-leader-term .ant-tag');
  const actualVoter = page.locator('.pd-config-snapshot').nth(1).locator('.pd-member-tags .ant-tag').first();
  await expect(actualLeader).toHaveCSS('background-color', 'rgb(255, 244, 220)');
  await expect(actualVoter).toHaveCSS('background-color', 'rgb(235, 250, 245)');
  await page.getByRole('button', {name: '切换暗色主题'}).click();
  await expect(leader).toHaveCSS('color', 'rgb(251, 191, 36)');
  await expect(leader).toHaveCSS('background-color', 'rgb(57, 44, 23)');
  await expect(actualLeader).toHaveCSS('background-color', 'rgb(57, 44, 23)');
  await expect(actualVoter).toHaveCSS('background-color', 'rgb(23, 54, 43)');
  await go(page, '/');
  await expect(page.locator('.dashboard-table-panel .ant-tag', {hasText: /^Leader$/})).toHaveCSS('background-color', 'rgb(57, 44, 23)');
  await go(page, '/clusters/demo');
  await page.getByRole('tab', {name: 'Broker', exact: true}).click();
  await page.getByRole('button', {name: '详情', exact: true}).click();
  await expect(page.locator('.ant-drawer .ant-tag', {hasText: /^Leader$/})).toHaveCSS('background-color', 'rgb(57, 44, 23)');
});
test('int64 key survives HTTP parsing and detail reload', async ({page}) => {
  await page.route('**/api/v1/broker/job-subscription-mirrors/9007199254741046', r => r.fulfill({
    contentType: 'application/json', body: '{"mirrorKey":9007199254741046,"sourceClusterId":"demo","targetClusterId":"target","status":"ACTIVE"}',
  }));
  await go(page, '/subscriptions/mirrors/9007199254741046');
  await expect(page.locator('.copyable-id-value', {hasText: '9007199254741046'}).first()).toBeVisible();
  await page.reload();
  await expect(page.locator('.copyable-id-value', {hasText: '9007199254741046'}).first()).toBeVisible();
});
test('multi tenant worker rows and selected drawer survive reload', async ({page}) => {
  await go(page, '/workers');
  await expect(page.locator('.ant-table-row')).toHaveCount(2);
  await page.getByRole('button', {name: 'payment', exact: true}).nth(1).click();
  await expect(page.locator('.ant-drawer')).toContainText('离线，数值来自最后一次上报');
  await page.reload();
  await expect(page.locator('.ant-drawer')).toContainText('离线，数值来自最后一次上报');
});
test('Job Type uses plain bounded text with full hover and focus values in both themes', async ({page}) => {
  const longType = '支付订单回调-' + 'taskite.payment.order.callback.production.'.repeat(15);
  await page.route('**/api/v1/workers', r => r.fulfill({json: {
    generatedAt: worker.lastRenewal, total: 2, online: 2, offline: 0,
    workers: [worker, {...worker, workerId: 'long-worker', name: 'long-worker', jobType: longType}],
  }}));
  await go(page, '/workers');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    for (const width of [1440, 390]) {
      await page.setViewportSize({width, height: 980});
      const text = page.locator('.ant-table .worker-job-type-text').nth(1);
      await expect(text).not.toHaveClass(/ant-tag/);
      await expect(text).toHaveCSS('background-color', 'rgba(0, 0, 0, 0)');
      await expect(text).toHaveCSS('border-width', '0px');
      expect(await text.evaluate(el => el.scrollWidth > el.clientWidth && el.clientWidth <= 144)).toBe(true);
      await text.hover();
      await expect(page.getByRole('tooltip')).toHaveText(longType);
      await page.getByRole('heading', {name: 'Worker', exact: true}).hover();
      await text.focus();
      await expect(page.getByRole('tooltip')).toHaveText(longType);
      if (width === 1440) await page.screenshot({path: `/tmp/taskite-worker-plain-${theme}.png`, animations: 'disabled'});
      await page.keyboard.press('Tab');
      await page.getByRole('button', {name: 'long-worker', exact: true}).click();
      const detail = page.locator('.ant-drawer .worker-job-type-text');
      await expect(detail).toHaveText(longType);
      expect(await detail.evaluate(el => el.scrollWidth > el.clientWidth && el.clientWidth <= 320)).toBe(true);
      await detail.hover();
      await expect(page.getByRole('tooltip')).toHaveText(longType);
      expect(await page.locator('.ant-drawer-content').evaluate(el => el.scrollWidth <= el.clientWidth)).toBe(true);
      await page.locator('.ant-drawer-close').click();
    }
    await page.setViewportSize({width: 1440, height: 980});
  }
});
const fourBrokerConfig = {...config, clusterSize: 4, voterRoster: ['b0', 'b1', 'b2', 'b3']};
const fourBrokerTopology = {...healthyTopology, ...fourBrokerConfig,
  brokers: [...healthyTopology.brokers, {...healthyTopology.brokers[1], brokerId: 'b3'}]};
async function fourBrokers(page: Page) {
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => r.request().method() === 'GET'
    ? r.fulfill({json: fourBrokerConfig}) : r.fallback());
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: fourBrokerTopology}));
}
const fiveBrokerConfig = {...config, clusterSize: 5, partitionSize: 8, voterRoster: ['b0', 'b1', 'b2', 'b3', 'b4']};
const fiveBrokerPartitions = [
  ['b0', 'b1', 'b2'], ['b0', 'b2', 'b3'], ['b2', 'b3', 'b4'], ['b1', 'b3', 'b4'],
  ['b0', 'b1', 'b4'], ['b0', 'b1', 'b2'], ['b0', 'b2', 'b3'], ['b2', 'b3', 'b4'],
].map((voters, index) => ({...healthyTopology.partitions[0], partitionId: index + 1,
  leaderId: voters[0], replicas: voters, learners: []}));
const fiveBrokerTopology = {...healthyTopology, ...fiveBrokerConfig, partitions: fiveBrokerPartitions,
  brokers: fiveBrokerConfig.voterRoster.map(brokerId => ({...topology.brokers[0], brokerId,
    partitions: fiveBrokerPartitions.filter(partition => partition.replicas.includes(brokerId))
      .map(partition => ({...report, partitionId: partition.partitionId,
        role: partition.leaderId === brokerId ? 'LEADER' : 'FOLLOWER',
        election: {...report.election, leaderId: partition.leaderId,
          nodeState: partition.leaderId === brokerId ? 'LEADER' : 'FOLLOWER'}}))})),
};
async function fiveBrokers(page: Page) {
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => r.request().method() === 'GET'
    ? r.fulfill({json: fiveBrokerConfig}) : r.fallback());
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: fiveBrokerTopology}));
}
for (const mode of ['count', 'members', 'draining'] as const) {
  test(`five to three Brokers with eight RF3 partitions submits safely using ${mode}`, async ({page}) => {
    await fiveBrokers(page);
    if (mode === 'draining') {
      await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fiveBrokerTopology,
        brokers: fiveBrokerTopology.brokers.map(b => ({...b, status: ['b3', 'b4'].includes(b.brokerId) ? 'DRAINING' : 'ONLINE'}))}}));
    }
    const writes: unknown[] = [];
    await page.route('**/api/v1/broker/clusters/demo/configuration', r => {
      if (r.request().method() !== 'PATCH') return r.fallback();
      writes.push(r.request().postDataJSON());
      return r.fulfill({status: 202, json: {changeId: 'change-1'}});
    });
    await go(page, '/clusters/demo');
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    if (mode === 'members') {
      await choose(page, '移除 Broker', 'b3');
      await choose(page, '移除 Broker', 'b4');
      await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
    } else {
      await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
    }
    await expect(page.locator('.configuration-validation-error')).not.toBeVisible();
    await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
    await page.getByRole('button', {name: '预览变更'}).click();
    const confirmation = page.getByRole('dialog', {name: '确认配置变更'});
    await expect(confirmation.getByRole('alert')).toContainText('分区迁移与 Leader 切换');
    expect(writes).toEqual([]);
    await confirmation.getByRole('button', {name: mode === 'members' ? '确认移除并提交' : '确认高危变更', exact: true}).click();
    await expect(page).toHaveURL(/changes\/clusters\/demo\/change-1$/);
    expect(writes).toEqual([mode === 'members' ? {brokers: {remove: ['b3', 'b4']}} : {brokers: {count: 3}}]);
  });
}
test('five to three Brokers cannot replace an existing lost quorum with online Learners', async ({page}) => {
  await fiveBrokers(page);
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fiveBrokerTopology,
    brokers: [...fiveBrokerTopology.brokers.map(b => ({...b, status: ['b3', 'b4'].includes(b.brokerId) ? 'OFFLINE' : 'ONLINE'})),
      learnerBroker('b5'), learnerBroker('b6')],
    partitions: fiveBrokerPartitions.map(partition => ({...partition, learners: ['b5', 'b6']})),
  }}));
  let writes = 0;
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await expect(page.locator('.configuration-validation-error').locator('li'))
    .toHaveText(['当前分区的可用投票节点不足以维持多数派，请先恢复原投票节点']);
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  expect(writes).toBe(0);
});
for (const status of ['OFFLINE', 'DRAINING']) {
  test(`five to three Brokers still requires enough ONLINE targets for RF3 when a target is ${status}`, async ({page}) => {
    await fiveBrokers(page);
    await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fiveBrokerTopology,
      brokers: fiveBrokerTopology.brokers.map(b => ({...b, status: b.brokerId === 'b0' ? status : 'ONLINE'}))}}));
    await go(page, '/clusters/demo');
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
    await expect(page.locator('.configuration-validation-error').locator('li'))
      .toHaveText(['目标成员中仅 2 个 Broker 在线，副本因子 3 需要至少 3 个在线节点']);
    await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  });
}
test('five to three Brokers is blocked when one of eight partitions is missing', async ({page}) => {
  await fiveBrokers(page);
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fiveBrokerTopology,
    partitions: fiveBrokerPartitions.slice(0, 7)}}));
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await expect(page.locator('.configuration-validation-error').locator('li'))
    .toHaveText(['分区拓扑不完整，无法校验在线多数派，请刷新后重试']);
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
});
test('five to three Brokers refreshes original voter quorum before PATCH', async ({page}) => {
  await fiveBrokers(page);
  let offline = false, writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fiveBrokerTopology,
    brokers: fiveBrokerTopology.brokers.map(b => ({...b,
      status: offline && ['b3', 'b4'].includes(b.brokerId) ? 'OFFLINE' : 'ONLINE'}))}}));
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await page.getByRole('button', {name: '预览变更'}).click();
  offline = true;
  await page.getByRole('button', {name: '确认高危变更', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('当前分区的可用投票节点不足以维持多数派，请先恢复原投票节点');
  expect(writes).toBe(0);
});
test('removing 4 to 2 with RF3 is blocked during editing and safe combined RF2 clears the error', async ({page}) => {
  await fourBrokers(page);
  let writes = 0;
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '移除 Broker', 'b2');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
  await choose(page, '移除 Broker', 'b3');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
  const error = page.locator('.configuration-validation-error');
  await expect(error).toContainText('目标 Broker 仅 2 个，少于副本因子 3');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await expect(page.locator('.ant-modal')).not.toBeVisible();
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') {
      await page.locator('.broker-config-editor .ant-drawer-close').click();
      await page.getByRole('button', {name: '切换暗色主题'}).click();
      await page.getByRole('button', {name: '修改配置', exact: true}).click();
    }
    await error.scrollIntoViewIfNeeded();
    await page.screenshot({path: `/tmp/taskite-broker-removal-validation-${theme}.png`, animations: 'disabled'});
  }
  await page.getByLabel('副本因子', {exact: true}).fill('2');
  await expect(error).not.toBeVisible();
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(page.getByRole('button', {name: '确认移除并提交', exact: true})).toBeEnabled();
  expect(writes).toBe(0);
});
test('member selection count mismatch is rejected and removing a Learner preserves voter count', async ({page}) => {
  await fourBrokers(page);
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fourBrokerTopology,
    brokers: [...fourBrokerTopology.brokers, learnerBroker('b4')]}}));
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '移除 Broker', 'b4');
  await expect(page.locator('.broker-config-editor')).toContainText('变更后目标 Broker：4');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await expect(page.locator('.configuration-validation-error')).toContainText('目标 Broker 为 4 个，与填写的目标数量 3 不一致');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).fill('unregistered');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Enter');
  await expect(page.locator('.configuration-validation-error')).toContainText('未知 Broker：unregistered');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
});
test('offline voters and new Learners cannot satisfy the existing partition quorum', async ({page}) => {
  await fourBrokers(page);
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fourBrokerTopology,
    brokers: [...fourBrokerTopology.brokers.map(b => ({...b, status: ['b1', 'b2'].includes(b.brokerId) ? 'OFFLINE' : 'ONLINE'})), learnerBroker('b4')]}}));
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '移除 Broker', 'b1');
  await choose(page, '添加 Broker', 'b4');
  await page.getByLabel('副本因子', {exact: true}).fill('2');
  const error = page.locator('.configuration-validation-error');
  await expect(error.locator('li')).toHaveText(['当前分区的可用投票节点不足以维持多数派，请先恢复原投票节点']);
  await expect(error).not.toContainText(/分区 \d/);
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
});
test('number inputs block impossible counts and RF before blur', async ({page}) => {
  await fourBrokers(page);
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const preview = page.getByRole('button', {name: '预览变更'});
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('2');
  await expect(preview).toBeDisabled();
  await expect(page.locator('.configuration-validation-error')).toContainText('少于副本因子 3');
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await expect(preview).toBeEnabled();
  await page.getByLabel('副本因子', {exact: true}).fill('0');
  await expect(preview).toBeDisabled();
  await expect(page.locator('.configuration-validation-error')).toContainText('副本因子必须为大于 0 的整数');
  await page.getByLabel('副本因子', {exact: true}).fill('1');
  await expect(preview).toBeDisabled();
  await expect(page.locator('.configuration-validation-error')).toContainText('不能低于当前多数派 2');
  await page.getByLabel('副本因子', {exact: true}).fill('2');
  await expect(preview).toBeEnabled();
});
for (const confirm of [false, true]) {
  test(`Broker removal reacts to updated topology during ${confirm ? 'confirmation' : 'editing'}`, async ({page}) => {
    await fourBrokers(page);
    let lost = false, writes = 0;
    await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fourBrokerTopology,
      brokers: fourBrokerTopology.brokers.map(b => ({...b, status: lost && b.brokerId === 'b1' ? 'OFFLINE' : 'ONLINE'}))}}));
    page.on('request', r => {if (r.method() === 'PATCH') writes++;});
    await go(page, '/clusters/demo');
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    await choose(page, '移除 Broker', 'b3');
    await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
    if (confirm) await page.getByRole('button', {name: '预览变更'}).click();
    lost = true;
    const panel = page.locator(confirm ? '.ant-modal' : '.broker-config-editor');
    await expect(panel).toContainText('目标成员中仅 2 个 Broker 在线', {timeout: 15000});
    await expect(page.getByRole('button', {name: confirm ? '确认移除并提交' : '预览变更', exact: true})).toBeDisabled();
    expect(writes).toBe(0);
  });
}
test('Broker removal rechecks fresh topology before any PATCH', async ({page}) => {
  await fourBrokers(page);
  let offline = false, writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...fourBrokerTopology,
    brokers: fourBrokerTopology.brokers.map(b => ({...b, status: offline && b.brokerId === 'b1' ? 'OFFLINE' : 'ONLINE'}))}}));
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '移除 Broker', 'b3');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
  await page.getByRole('button', {name: '预览变更'}).click();
  offline = true;
  await page.getByRole('button', {name: '确认移除并提交', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('目标成员中仅 2 个 Broker 在线');
  expect(writes).toBe(0);
});
test('worker cluster tenant name are sent as precise API filters', async ({page}) => {
  let query = '';
  await page.route('**/api/v1/workers?**', r => {query = new URL(r.request().url()).search; return r.fulfill({json: {generatedAt: worker.lastRenewal, total: 0, online: 0, offline: 0, workers: []}});});
  await go(page, '/workers?cluster=demo&tenant=a&name=payment');
  await expect(page.getByText('暂无符合筛选条件的 Worker，可在表头重置筛选')).toBeVisible();
  expect(query).toBe('?cluster=demo&tenant=a&name=payment');
});
test('partition matrix retains missing voters and lag does not determine role', async ({page}) => {
  await go(page, '/clusters/demo?tab=partitions');
  await expect(page.locator('.partition-matrix [data-broker-id="missing-voter"]')).toContainText('Unknown');
  await expect(page.locator('.partition-matrix [data-broker-id="b0"]')).toContainText('Leader');
  await page.locator('.partition-matrix-node').getByText('b0').click();
  await expect(page.locator('.ant-drawer')).toContainText('服务就绪');
});
test('PD NO_CHANGES does not navigate or poll a fake ID', async ({page}) => {
  const requests: string[] = [];
  page.on('request', r => requests.push(r.url()));
  await page.route('**/api/v1/pd/configuration', r => r.request().method() === 'PATCH'
    ? r.fulfill({json: {changeId: 'NO_CHANGES', status: 'NO_CHANGES'}}) : r.fallback());
  await go(page, '/pd');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByRole('spinbutton', {name: '目标数量'}).fill('1');
  await page.getByRole('button', {name: '预览变更'}).click();
  await page.getByRole('button', {name: '确认提交'}).click();
  await expect(page.getByText('无需变更，已刷新配置')).toBeVisible();
  expect(requests.some(u => u.includes('/changes/NO_CHANGES'))).toBe(false);
});
test('combined broker configuration uses one PATCH and server change ID', async ({page}) => {
  await withLearnerBrokers(page);
  const writes: unknown[] = [];
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => {
    if (r.request().method() !== 'PATCH') return r.fallback();
    writes.push(r.request().postDataJSON());
    return r.fulfill({status: 202, json: {changeId: 'change-1'}});
  });
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await page.getByLabel('分区数', {exact: true}).fill('6');
  await page.getByLabel('副本因子', {exact: true}).fill('2');
  await choose(page, '添加 Broker', 'b3');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).fill('b0');
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Enter');
  await page.getByRole('button', {name: '预览变更'}).click();
  const confirmation = page.getByRole('dialog', {name: '确认配置变更'});
  await expect(confirmation.getByRole('alert')).toContainText('降低副本因子会减少数据副本');
  await expect(confirmation.locator('.configuration-change-add .configuration-member-id')).toHaveText(['b3']);
  await expect(confirmation.locator('.configuration-change-remove .configuration-member-id')).toHaveText(['b0']);
  expect(writes).toEqual([]);
  await confirmation.getByRole('button', {name: '确认移除并提交', exact: true}).click();
  await expect(page).toHaveURL(/changes\/clusters\/demo\/change-1$/);
  expect(writes).toEqual([{brokers: {add: ['b3'], remove: ['b0']}, partitions: {count: 6, replicationFactor: 2}}]);
  await expect(page.getByText('执行中', {exact: true})).toBeVisible();
});
test('Broker mixed change summary stays readable across themes and viewport widths', async ({page}) => {
  await withLearnerBrokers(page);
  let writes = 0;
  page.on('request', request => {if (request.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    if (theme === 'light') {
      await choose(page, '添加 Broker', 'b3');
      await choose(page, '添加 Broker', 'b4');
      await choose(page, '移除 Broker', 'b0');
      await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
      await page.getByRole('spinbutton', {name: '副本因子', exact: true}).fill('2');
    }
    await page.getByRole('button', {name: '预览变更'}).click();
    const modal = page.locator('.console-modal');
    await expect(modal).toBeVisible();
    await expect(modal).not.toHaveClass(/ant-zoom/);
    for (const width of [1440, 390]) {
      await page.setViewportSize({width, height: 1000});
      await expect(modal.locator('.configuration-change-add .configuration-member-id')).toHaveText(['b3', 'b4']);
      await expect(modal.locator('.configuration-change-remove .configuration-member-id')).toHaveText(['b0']);
      await expect(modal.getByRole('button', {name: '确认移除并提交', exact: true})).toBeVisible();
      expect(await modal.evaluate(element => element.scrollWidth <= element.clientWidth)).toBe(true);
      const bounds = await modal.boundingBox();
      expect(bounds!.x).toBeGreaterThanOrEqual(0);
      expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(width);
      await page.screenshot({path: `/tmp/taskite-broker-member-confirm-${theme}-${width}.png`, animations: 'disabled'});
    }
    await modal.getByRole('button', {name: /取\s*消/}).click();
    await page.locator('.broker-config-editor .ant-drawer-close').click();
    await page.setViewportSize({width: 1440, height: 1000});
  }
  expect(writes).toBe(0);
});
test('PD add members selects only current Learners and submits their exact ids', async ({page}) => {
  const writes: unknown[] = [];
  await page.route('**/api/v1/pd/topology', route => route.fulfill({json: {
    localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 1,
    members: [
      {memberId: 'pd-0', role: 'LEADER', self: true},
      {memberId: 'pd-1', role: 'LEARNER', self: false},
      {memberId: 'pd-2', role: 'LEARNER', self: false},
      {memberId: 'pd-registered', role: 'REGISTERED', self: false},
    ],
  }}));
  await page.route('**/api/v1/pd/configuration', route => {
    if (route.request().method() === 'PATCH') {
      writes.push(route.request().postDataJSON());
      return route.fulfill({json: {changeId: 'pd-change', status: 'COMPLETED'}});
    }
    return route.fulfill({json: {
      epoch: 1, target: {count: 1, voters: ['pd-0']},
      actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0'], learners: ['pd-1', 'pd-2']},
    }});
  });
  await go(page, '/pd');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const input = page.getByRole('combobox', {name: '添加成员', exact: true});
  await input.click();
  await expect(page.locator('.ant-select-dropdown:visible .ant-select-item-option-content')).toHaveText(['pd-1', 'pd-2']);
  await input.fill('unknown-pd');
  await input.press('Enter');
  await expect(page.locator('.pd-member-editor .ant-form-item').first().locator('.ant-select-selection-item')).toHaveCount(0);
  await input.fill('');
  await choose(page, '添加成员', 'pd-1');
  await choose(page, '添加成员', 'pd-2');
  await input.press('Escape');
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(page.locator('.console-modal .configuration-change-add .configuration-member-id')).toHaveText(['pd-1', 'pd-2']);
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page).toHaveURL(/\/changes\/pd\/pd-change$/);
  expect(writes).toEqual([{add: ['pd-1', 'pd-2'], remove: []}]);
});

test('PD add members is disabled without Learners while removal remains available', async ({page}) => {
  await go(page, '/pd');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await expect(page.getByRole('combobox', {name: '添加成员', exact: true})).toBeDisabled();
  await expect(page.locator('.pd-member-editor')).toContainText('请先启动新节点，等待其加入 Learner 列表');
  await choose(page, '移除成员', 'pd-0');
  await page.getByRole('combobox', {name: '移除成员', exact: true}).press('Escape');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
});

test('PD add members rechecks Learners before submitting a promotion', async ({page}) => {
  let promoted = false;
  const writes: unknown[] = [];
  await page.route('**/api/v1/pd/configuration', route => {
    if (route.request().method() === 'PATCH') {
      writes.push(route.request().postDataJSON());
      return route.fulfill({json: {changeId: 'pd-change', status: 'COMPLETED'}});
    }
    const voters = promoted ? ['pd-0', 'pd-1'] : ['pd-0'];
    return route.fulfill({json: {
      epoch: promoted ? 2 : 1, target: {count: voters.length, voters},
      actual: {leaderId: 'pd-0', term: 1, voters, learners: promoted ? [] : ['pd-1']},
    }});
  });
  await go(page, '/pd');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '添加成员', 'pd-1');
  await page.getByRole('combobox', {name: '添加成员', exact: true}).press('Escape');
  await page.getByRole('button', {name: '预览变更'}).click();
  promoted = true;
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.locator('.console-modal')).toContainText('所选成员已不在 Learner 列表，请重新选择：pd-1');
  expect(writes).toEqual([]);
  await page.locator('.console-modal').getByRole('button', {name: /取\s*消/}).click();
  const addField = page.locator('.pd-member-editor .ant-form-item').first();
  await expect(addField).toContainText('所选成员已不在 Learner 列表');
  await addField.locator('.ant-select-selection-item-remove').click();
  await expect(page.getByRole('combobox', {name: '添加成员', exact: true})).toBeDisabled();
});

test('PD mixed member changes distinguish removals and cancellation preserves the draft without writing', async ({page}) => {
  const writes: unknown[] = [];
  await page.route('**/api/v1/pd/configuration', route => {
    if (route.request().method() !== 'PATCH') return route.fulfill({json: {
      epoch: 1, target: {count: 1, voters: ['pd-0']},
      actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0'], learners: ['pd-new']},
    }});
    writes.push(route.request().postDataJSON());
    return route.fulfill({json: {changeId: 'NO_CHANGES', status: 'NO_CHANGES'}});
  });
  await go(page, '/pd');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '添加成员', 'pd-new');
  await choose(page, '移除成员', 'pd-0');
  await expect(page.locator('.pd-member-editor')).toContainText('此变更包含高危操作');
  await page.getByRole('combobox', {name: '移除成员', exact: true}).press('Escape');
  await page.getByRole('button', {name: '预览变更'}).click();
  const modal = page.locator('.console-modal');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') {
      await modal.getByRole('button', {name: /取\s*消/}).click();
      await page.locator('.pd-member-editor .ant-drawer-close').click();
      await page.getByRole('button', {name: '切换暗色主题'}).click();
      await page.getByRole('button', {name: '修改配置', exact: true}).click();
      await page.getByRole('button', {name: '预览变更'}).click();
    }
    await expect(modal.locator('.confirmation-danger-tag')).toHaveText('高危操作');
    await expect(modal.getByRole('alert')).toContainText('多数派');
    await expect(modal.locator('.configuration-change-add .configuration-member-id')).toHaveText(['pd-new']);
    await expect(modal.locator('.configuration-change-remove .configuration-member-id')).toHaveText(['pd-0']);
    await expect(modal.getByRole('button', {name: '确认移除并提交', exact: true})).toHaveClass(/ant-btn-dangerous/);
    await expect(modal).not.toHaveClass(/ant-zoom/);
    await page.screenshot({path: `/tmp/taskite-pd-member-confirm-${theme}.png`, animations: 'disabled'});
  }
  expect(writes).toEqual([]);
  await modal.getByRole('button', {name: /取\s*消/}).click();
  await expect(page.locator('.pd-member-editor .configuration-removal-select .ant-select-selection-item')).toContainText('pd-0');
  await page.locator('.pd-member-editor .configuration-removal-select .ant-select-selection-item-remove').click();
  await expect(page.locator('.pd-member-editor .configuration-editor-risk')).toHaveCount(0);
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(modal.getByRole('alert')).toHaveCount(0);
  await expect(modal.locator('.confirmation-danger-tag')).toHaveCount(0);
  await modal.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.getByText('无需变更，已刷新配置', {exact: true})).toBeVisible();
  expect(writes).toEqual([{add: ['pd-new'], remove: []}]);
});
for (const scenario of [
  {name: 'PD target count', path: '/pd', field: '目标数量', current: 3, warning: '多数派'},
  {name: 'Broker target count', path: '/clusters/demo', field: '目标 Broker 数量', current: 3, warning: '分区迁移'},
  {name: 'Broker replication factor', path: '/clusters/demo', field: '副本因子', current: 3, warning: '降低故障容忍能力'},
]) {
  test(`${scenario.name} reduction is high risk without an explicit member removal`, async ({page}) => {
    let writes = 0;
    page.on('request', request => {if (request.method() === 'PATCH') writes++;});
    if (scenario.path === '/pd') await page.route('**/api/v1/pd/configuration', route => route.fulfill({json: {
      epoch: 1, target: {count: 3, voters: ['pd-0', 'pd-1', 'pd-2']},
      actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0', 'pd-1', 'pd-2'], learners: []},
    }}));
    if (scenario.path !== '/pd') {
      await withLearnerBrokers(page);
      if (scenario.field === '目标 Broker 数量') {
        await page.route('**/api/v1/broker/clusters/demo/configuration', route => route.fulfill({json: {...config, replicationFactor: 2}}));
        await page.route('**/api/v1/broker/clusters/demo/topology', route => route.fulfill({json: {...healthyTopology,
          replicationFactor: 2, brokers: [...healthyTopology.brokers, learnerBroker('b3')],
          partitions: healthyTopology.partitions.map(p => ({...p, replicas: ['b0', 'b1']})),
        }}));
      }
    }
    await go(page, scenario.path);
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    await page.getByRole('spinbutton', {name: scenario.field, exact: true}).fill('2');
    await page.getByRole('button', {name: '预览变更'}).click();
    const modal = page.locator('.console-modal');
    await expect(modal.getByRole('alert')).toContainText(scenario.warning);
    await expect(modal.getByRole('button', {name: '确认高危变更', exact: true})).toHaveClass(/ant-btn-dangerous/);
    await expect(modal.locator('.configuration-value-before')).toHaveText(String(scenario.current));
    await expect(modal.locator('.configuration-value-after')).toHaveText('2');
    await modal.getByRole('button', {name: /取\s*消/}).click();
    expect(writes).toBe(0);
    await page.getByRole('spinbutton', {name: scenario.field, exact: true}).fill('4');
    if (scenario.field === '副本因子') await page.getByRole('spinbutton', {name: '目标 Broker 数量', exact: true}).fill('4');
    await page.getByRole('button', {name: '预览变更'}).click();
    await expect(modal.getByRole('alert')).toHaveCount(0);
    await expect(modal.getByRole('button', {name: '确认提交', exact: true})).not.toHaveClass(/ant-btn-dangerous/);
  });
}
test('Broker endpoints stay on one line with ellipsis and full hover text across widths and themes', async ({page, context}) => {
  const endpoints = [
    {host: '127.0.0.1', text: '127.0.0.1:26501'},
    {host: 'broker-02.internal.taskite.example', text: 'broker-02.internal.taskite.example:26501'},
    {host: '2001:db8:1234:5678:90ab:cdef:1234:5678', text: '[2001:db8:1234:5678:90ab:cdef:1234:5678]:26501'},
    {host: '2605:0340:cd50:2000:ffff:ffff:ffff:ffff', text: '[2605:0340:cd50:2000:ffff:ffff:ffff:ffff]:26501'},
    {host: '[2001:db8::1]', text: '[2001:db8::1]:26501'},
    {host: 'fe80::1%eth0', text: '[fe80::1%eth0]:26501'},
  ];
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
    brokers: endpoints.map((endpoint, index) => ({...topology.brokers[0], brokerId: `taskite-broker-production-00${index}`, host: endpoint.host})),
  }}));
  await go(page, '/clusters/demo?tab=brokers');
  const table = page.locator('.broker-list-table');
  await expect(table.locator('.ant-table-row')).toHaveCount(endpoints.length);
  for (const width of [1440, 1100, 768, 390]) {
    await page.setViewportSize({width, height: 1000});
    if (width === 390) await page.getByRole('button', {name: '收起导航'}).click();
    for (const theme of ['light', 'dark']) {
      if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
      const layout = await table.evaluate(el => [...el.querySelectorAll('.ant-table-row > td')].map(cell => {
        const css = getComputedStyle(cell);
        const content = cell.querySelector('.number-tag');
        return {nowrap: css.whiteSpace, width: cell.getBoundingClientRect().width,
          contentHeight: content?.getBoundingClientRect().height ?? 0, lineHeight: content ? parseFloat(getComputedStyle(content).lineHeight) : 0};
      }));
      expect(layout.every(cell => cell.nowrap === 'nowrap')).toBe(true);
      expect(layout.filter(cell => cell.lineHeight > 0).every(cell => cell.contentHeight <= cell.lineHeight + 2)).toBe(true);
      expect(layout[3].width).toBeGreaterThanOrEqual(100);
      for (const [index, endpoint] of endpoints.entries()) {
        const value = table.locator('.network-endpoint-value').nth(index);
        await expect(value).toHaveText(endpoint.text);
        await expect(value).toHaveCSS('white-space', 'nowrap');
        await expect(value).toHaveCSS('text-overflow', 'ellipsis');
        expect(await value.evaluate(el => {
          const cell = el.closest('td')!.getBoundingClientRect();
          const box = el.getBoundingClientRect();
          return box.height <= parseFloat(getComputedStyle(el).lineHeight) + 1
            && box.left >= cell.left && box.right <= cell.right && box.top >= cell.top && box.bottom <= cell.bottom;
        })).toBe(true);
      }
      const ipv6 = table.locator('.network-endpoint-value').nth(2);
      expect(await ipv6.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true);
      await ipv6.hover();
      await expect(page.getByRole('tooltip', {name: endpoints[2].text, exact: true})).toBeVisible();
      await table.locator('.network-endpoint-copy').nth(2).hover();
      await expect(page.getByRole('tooltip', {name: endpoints[2].text, exact: true})).toHaveCount(0);
      // Table resize handling finishes after the viewport changes.
      await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      if (width === 1440) await page.screenshot({path: `/tmp/taskite-console-ipv6-brokers-${theme}.png`, fullPage: true, animations: 'disabled'});
      if (theme === 'dark') await page.getByRole('button', {name: '切换亮色主题'}).click();
    }
  }
  await page.setViewportSize({width: 1440, height: 1000});
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  const row = table.locator('.ant-table-row').nth(2);
  await row.getByRole('button', {name: `复制 ${endpoints[2].text}`, exact: true}).click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(endpoints[2].text);
  await row.getByRole('button', {name: '详情', exact: true}).click();
  const drawer = page.locator('.ant-drawer');
  await expect(drawer.locator('.network-endpoint-value')).toHaveText(endpoints[2].text);
  await page.setViewportSize({width: 390, height: 1000});
  await expect(drawer.locator('.network-endpoint-value')).toBeVisible();
  await expect(drawer.locator('.network-endpoint-value')).toHaveCSS('white-space', 'nowrap');
  await drawer.locator('.network-endpoint-value').focus();
  await expect(page.getByRole('tooltip', {name: endpoints[2].text, exact: true})).toBeVisible();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await drawer.getByRole('button', {name: `复制 ${endpoints[2].text}`, exact: true}).click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(endpoints[2].text);
});

test('PD member addresses and dashboard keep complete IPv6 endpoints and ports', async ({page, context}) => {
  const host = '2605:0340:cd50:2000:ffff:ffff:ffff:ffff';
  const httpEndpoint = `[${host}]:19001`;
  await page.route('**/api/v1/pd/topology', route => route.fulfill({json: {
    localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 1,
    members: [
      {memberId: 'pd-0', host, httpPort: 19001, grpcPort: 26501, raftPort: 26502, role: 'VOTER', status: 'ONLINE', self: true},
      {memberId: 'pd-1', host: `[${host}]`, httpPort: 19001, grpcPort: 26501, raftPort: 26502, role: 'VOTER', status: 'ONLINE', self: false},
      {memberId: 'pd-2', endpoint: '[2001:db8::2]:26502', role: 'VOTER', self: false},
    ],
  }}));
  await page.route('**/api/v1/pd/health', route => route.fulfill({json: {
    status: 'UP', nodeId: 'pd-0', role: 'LEADER', leaderId: 'pd-0', host,
    port: 19001, grpcPort: 26501, term: 1, quorum: 1, peerCount: 3, commitIndex: 2, lastApplied: 2, lastLogIndex: 2,
  }}));
  await go(page, '/pd');
  const table = page.locator('.pd-members-panel');
  await expect(table.locator('.network-endpoint-value')).toHaveText([httpEndpoint, httpEndpoint, '[2001:db8::2]:26502']);
  for (const width of [1440, 768, 390]) {
    await page.setViewportSize({width, height: 1000});
    if (width === 390) await page.getByRole('button', {name: '收起导航'}).click();
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    expect(await table.locator('.network-endpoint-value').evaluateAll(elements => elements.every(el => {
      const box = el.getBoundingClientRect();
      const cell = el.closest('td')!.getBoundingClientRect();
      return box.height <= parseFloat(getComputedStyle(el).lineHeight) + 1
        && box.right <= cell.right && box.bottom <= cell.bottom;
    }))).toBe(true);
  }
  await page.setViewportSize({width: 1440, height: 1000});
  await context.grantPermissions(['clipboard-read', 'clipboard-write']);
  await table.locator('.network-endpoint-value').first().hover();
  await expect(page.getByRole('tooltip', {name: httpEndpoint, exact: true})).toBeVisible();
  await table.getByRole('button', {name: `复制 ${httpEndpoint}`, exact: true}).first().click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(httpEndpoint);
  await page.screenshot({path: '/tmp/taskite-console-ipv6-pd.png', fullPage: true, animations: 'disabled'});
  await go(page, '/');
  for (const width of [1440, 768, 390]) {
    await page.setViewportSize({width, height: 1000});
    const endpoint = page.locator('.network-endpoint-value');
    await expect(endpoint).toHaveText(httpEndpoint);
    await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await expect(endpoint).toHaveCSS('white-space', 'nowrap');
    expect(await endpoint.evaluate(el => el.getBoundingClientRect().height <= parseFloat(getComputedStyle(el).lineHeight) + 1)).toBe(true);
  }
  await page.getByRole('button', {name: `复制 ${httpEndpoint}`, exact: true}).click();
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(httpEndpoint);
});

test('ordinary HTTP copies full endpoints, IDs and resource content without Clipboard API', async ({page, context}) => {
  const origin = 'http://taskite.test:14173';
  await context.route(`${origin}/**`, async route => {
    const url = new URL(route.request().url());
    url.hostname = '127.0.0.1';
    await route.fulfill({response: await route.fetch({url: url.toString()})});
  });
  const endpoint = '[2605:0340:cd50:2000:ffff:ffff:ffff:ffff]:26501';
  await page.route('**/api/v1/broker/clusters/demo/topology', route => route.fulfill({
    json: {...topology, brokers: [{...topology.brokers[0], host: '2605:0340:cd50:2000:ffff:ffff:ffff:ffff'}]},
  }));
  await page.goto(`${origin}/console/#/clusters/demo?tab=brokers`);
  expect(await page.evaluate(() => ({secure: isSecureContext, clipboard: typeof navigator.clipboard})))
    .toEqual({secure: false, clipboard: 'undefined'});
  await context.grantPermissions(['clipboard-read'], {origin: 'http://127.0.0.1:14173'});
  const reader = await context.newPage();
  await api(reader);
  await reader.goto('/console/');
  const verifyCopy = async (button: ReturnType<Page['getByRole']>, value: string) => {
    await page.bringToFront();
    await button.click();
    await expect(page.getByText(/复制失败/)).toHaveCount(0);
    await expect(page.locator('body > textarea')).toHaveCount(0);
    await expect(button).toBeFocused();
    await reader.bringToFront();
    await expect.poll(() => reader.evaluate(() => navigator.clipboard.readText())).toBe(value);
  };
  await verifyCopy(page.locator('.network-endpoint-copy'), endpoint);
  await verifyCopy(page.locator('.broker-list-table .copyable-id button'), 'b0');
  await page.goto(`${origin}/console/#/subscriptions/distributions/9007199254741052`);
  await verifyCopy(page.getByRole('button', {name: '复制原文', exact: true}), JSON.stringify(resource.subscription, null, 2));
  await expect(page.getByText('已复制任务订阅原文', {exact: true})).toBeVisible();
  await reader.close();
});

test('denied Clipboard API falls back to copying the actual full endpoint', async ({page, context}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {value: {
      writeText: async () => { throw new DOMException('Permission denied', 'NotAllowedError'); },
    }});
  });
  await go(page, '/clusters/demo?tab=brokers');
  await context.grantPermissions(['clipboard-read']);
  const reader = await context.newPage();
  await api(reader);
  await reader.goto('/console/');
  await page.bringToFront();
  const button = page.locator('.network-endpoint-copy');
  await button.click();
  await expect(button).toHaveAttribute('aria-label', '已复制');
  await expect(button).toBeFocused();
  await expect(page.locator('body > textarea')).toHaveCount(0);
  await reader.bringToFront();
  await expect.poll(() => reader.evaluate(() => navigator.clipboard.readText())).toBe('127.0.0.1:26501');
  await reader.close();
});

test('copy reports failure when both clipboard mechanisms are unavailable', async ({page}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, 'clipboard', {value: undefined});
    document.execCommand = () => false;
  });
  await go(page, '/clusters/demo?tab=brokers');
  const button = page.getByRole('button', {name: '复制 127.0.0.1:26501', exact: true});
  await button.click();
  await expect(page.getByText('复制失败，请手动选择地址复制', {exact: true})).toBeVisible();
  await expect(button).toHaveAttribute('aria-label', '复制 127.0.0.1:26501');
  await expect(button).toBeFocused();
  await expect(page.locator('body > textarea')).toHaveCount(0);
});
test('partition role legends match cells with readable blue palette in both themes', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
    brokers: [{...topology.brokers[0], partitions: [
      report, {...report, partitionId: 2, role: 'FOLLOWER'}, {...report, partitionId: 3, role: 'LEARNER'},
    ]}],
    partitions: [...topology.partitions,
      {...topology.partitions[0], partitionId: 2, replicas: ['b0'], leaderId: 'missing-voter'},
      {...topology.partitions[0], partitionId: 3, replicas: ['missing-voter'], learners: ['b0'], leaderId: 'missing-voter'},
    ],
  }}));
  await go(page, '/clusters/demo?tab=partitions');
  await expect(page.locator('.partition-role-legend-item')).toHaveCount(4);
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    const palette = await page.locator('.partition-ownership').evaluate(el => {
      const luminance = (color: string) => {
        const rgb = color.match(/[\d.]+/g)!.slice(0, 3).map(Number).map(v => {
          const c = v / 255; return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
        });
        return rgb[0] * 0.2126 + rgb[1] * 0.7152 + rgb[2] * 0.0722;
      };
      return ['leader', 'follower', 'learner', 'unknown'].map(role => {
        const legend = getComputedStyle(el.querySelector(`.partition-role-legend-item.partition-role-${role}`)!);
        const cell = getComputedStyle(el.querySelector(`.partition-role-cell.partition-role-${role}`)!);
        const fg = luminance(cell.color), bg = luminance(cell.backgroundColor);
        return {role, legend: [legend.color, legend.backgroundColor, legend.borderColor, legend.borderStyle],
          cell: [cell.color, cell.backgroundColor, cell.borderColor, cell.borderStyle],
          contrast: (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05)};
      });
    });
    for (const role of palette) {
      expect(role.legend).toEqual(role.cell);
      expect(role.contrast).toBeGreaterThanOrEqual(4.5);
    }
    expect(new Set(palette.map(role => role.cell[1])).size).toBe(4);
    await page.screenshot({path: `target/broker-partitions-${theme}.png`, fullPage: true, animations: 'disabled'});
  }
  await page.setViewportSize({width: 768, height: 1000});
  await expect(page.locator('.partition-role-legend-item')).toHaveCount(4);
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});
test('Broker configuration drawer keeps draft when returning from preview and validates intersection', async ({page}) => {
  await withLearnerBrokers(page);
  await page.setViewportSize({width: 1440, height: 1000});
  let writes = 0;
  page.on('request', r => { if (r.method() === 'PATCH') writes++; });
  await go(page, '/clusters/demo?tab=config');
  await expect(page.getByRole('tab', {name: '配置', exact: true})).toHaveCount(0);
  await expect(page.getByRole('tab', {name: '概览', exact: true})).toHaveAttribute('aria-selected', 'true');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const editor = page.locator('.broker-config-editor');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('4');
  for (const name of ['添加 Broker', '移除 Broker']) {
    await choose(page, name, 'b3');
  }
  await expect(editor).toContainText('添加和移除列表存在交集');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await editor.locator('.ant-select').first().locator('.ant-select-selection-item-remove').click();
  await editor.locator('.configuration-removal-select .ant-select-selection-item-remove').click();
  await page.getByRole('combobox', {name: '移除 Broker', exact: true}).press('Escape');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
  await page.screenshot({path: 'target/broker-config-drawer-light.png', fullPage: true, animations: 'disabled'});
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(page.locator('.ant-modal .configuration-change-update .configuration-value-before')).toHaveText('3');
  await expect(page.locator('.ant-modal .configuration-change-update .configuration-value-after')).toHaveText('4');
  await page.locator('.ant-modal').getByRole('button', {name: /取\s*消/}).click();
  await expect(page.getByLabel('目标 Broker 数量', {exact: true})).toHaveValue('4');
  await editor.locator('.ant-drawer-footer').getByRole('button', {name: /取\s*消/}).click();
  await page.getByRole('button', {name: '切换暗色主题'}).click();
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await expect(page.getByLabel('目标 Broker 数量', {exact: true})).toHaveValue('4');
  await page.screenshot({path: 'target/broker-config-drawer-dark.png', fullPage: true, animations: 'disabled'});
  expect(writes).toBe(0);
});
test('active Broker change disables configuration editor entry point', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/changes/active', r => r.fulfill({json: change}));
  await go(page, '/clusters/demo?tab=brokers');
  await expect(page.getByRole('button', {name: '修改配置', exact: true})).toBeDisabled();
  await expect(page.getByRole('tab', {name: '配置', exact: true})).toHaveCount(0);
});
test('Broker configuration NO_CHANGES closes preview and resets draft without fake tracking', async ({page}) => {
  await withLearnerBrokers(page);
  const requests: string[] = [];
  page.on('request', r => requests.push(r.url()));
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => r.request().method() === 'PATCH'
    ? r.fulfill({json: {changeId: 'NO_CHANGES', status: 'NO_CHANGES'}}) : r.fallback());
  await go(page, '/clusters/demo?tab=brokers');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('4');
  await page.getByRole('button', {name: '预览变更'}).click();
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.locator('.ant-modal')).not.toBeVisible();
  await expect(page).toHaveURL(/clusters\/demo\?tab=brokers$/);
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await expect(page.getByLabel('目标 Broker 数量', {exact: true})).toBeEmpty();
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  expect(requests.some(u => u.includes('/changes/NO_CHANGES'))).toBe(false);
});
test('interrupted Broker configuration submission remains unknown without duplicate writes', async ({page}) => {
  await withLearnerBrokers(page);
  let writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => {
    if (r.request().method() !== 'PATCH') return r.fallback();
    writes++; return r.abort('failed');
  });
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('4');
  await page.getByRole('button', {name: '预览变更'}).click();
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('结果未知');
  await expect(page.getByRole('button', {name: '确认提交', exact: true})).toBeDisabled();
  expect(writes).toBe(1);
});
test('cluster information stays inline and Broker number pills remain visible in both themes', async ({page}) => {
  await page.setViewportSize({width: 1440, height: 1000});
  await go(page, '/clusters/demo');
  await expect(page.locator('.cluster-information .ant-descriptions-item-container')).toHaveCount(8);
  await expect(page.locator('.cluster-information .ant-descriptions-item-label')).toHaveText([
    'Cluster ID', 'Cluster Key', '目标 Broker', '在线 Broker',
    '分区数', '副本因子', 'Topology Version', '最后更新',
  ]);
  const informationRows = page.locator('.cluster-information .ant-descriptions-row');
  for (const [width, columns] of [[3440, 4], [900, 2], [390, 1], [1440, 4]]) {
    await page.setViewportSize({width, height: 1000});
    await expect(informationRows).toHaveCount(8 / columns);
    for (const row of await informationRows.all()) {
      await expect(row.locator('.ant-descriptions-item-container')).toHaveCount(columns);
    }
  }
  await expect(page.locator('.cluster-information .ant-descriptions-item-container').filter({hasText: 'Cluster Key'})).toContainText('1');
  const rows = await page.locator('.cluster-information .ant-descriptions-item-container').evaluateAll(elements =>
    elements.map(el => {
      const label = el.querySelector('.ant-descriptions-item-label')!.getBoundingClientRect();
      const value = el.querySelector('.ant-descriptions-item-content')!.getBoundingClientRect();
      return {labelCenter: label.y + label.height / 2, valueCenter: value.y + value.height / 2, inline: value.x >= label.right};
    }));
  expect(rows).toHaveLength(8);
  expect(rows.every(row => row.inline && Math.abs(row.labelCenter - row.valueCenter) < 2)).toBe(true);
  await page.screenshot({path: '/tmp/taskite-cluster-information-inline.png', fullPage: true, animations: 'disabled'});
  await page.getByRole('tab', {name: 'Broker', exact: true}).click();
  const row = page.locator('.broker-list-table .ant-table-row').first();
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await row.hover();
    const pills = await row.locator('.number-tag').evaluateAll(elements => elements.map(el => {
      const css = getComputedStyle(el);
      return {bg: css.backgroundColor, cellBg: getComputedStyle(el.parentElement!).backgroundColor,
        border: css.borderStyle, borderWidth: css.borderWidth, weight: css.fontWeight};
    }));
    expect(pills).toHaveLength(2);
    for (const pill of pills) {
      expect(pill.bg).not.toBe(pill.cellBg);
      expect(pill.border).toBe('solid');
      expect(pill.borderWidth).toBe('1px');
      expect(pill.weight).toBe('600');
    }
    await page.screenshot({path: `/tmp/taskite-broker-pills-${theme}.png`, fullPage: true, animations: 'disabled'});
  }
});
test('Broker config snapshot precedes editor and identical numeric values disable preview', async ({page}) => {
  await page.setViewportSize({width: 1440, height: 1000});
  let writes = 0;
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const editor = page.locator('.broker-config-editor');
  const snapshot = page.getByRole('complementary', {name: '当前 Broker 配置'});
  await expect(snapshot.locator('.broker-value-pill')).toHaveText(['3', '5', '3']);
  const snapshotBounds = await snapshot.boundingBox();
  const firstFieldBounds = await page.getByLabel('目标 Broker 数量', {exact: true}).boundingBox();
  expect(snapshotBounds!.y + snapshotBounds!.height).toBeLessThan(firstFieldBounds!.y);
  const preview = page.getByRole('button', {name: '预览变更'});
  await expect(preview).toBeDisabled();
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('3');
  await page.getByLabel('分区数', {exact: true}).fill('5');
  await page.getByLabel('副本因子', {exact: true}).fill('3');
  await expect(preview).toBeDisabled();
  await page.getByLabel('分区数', {exact: true}).fill('6');
  await expect(preview).toBeEnabled();
  await preview.click();
  await expect(page.locator('.ant-modal .configuration-change-update .configuration-change-label')).toHaveText(['分区数']);
  await expect(page.locator('.ant-modal .configuration-value-before')).toHaveText('5');
  await expect(page.locator('.ant-modal .configuration-value-after')).toHaveText('6');
  await page.locator('.ant-modal').getByRole('button', {name: /取\s*消/}).click();
  await page.getByLabel('分区数', {exact: true}).fill('5');
  await expect(preview).toBeDisabled();
  await page.screenshot({path: '/tmp/taskite-broker-snapshot-top-light.png', fullPage: true, animations: 'disabled'});
  await editor.locator('.ant-drawer-footer').getByRole('button', {name: /取\s*消/}).click();
  await page.getByRole('button', {name: '切换暗色主题'}).click();
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await expect(preview).toBeDisabled();
  await page.screenshot({path: '/tmp/taskite-broker-snapshot-top-dark.png', fullPage: true, animations: 'disabled'});
  expect(writes).toBe(0);
});
test('partition decrease reports an error and restores current count on blur and Enter', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => r.fulfill({json: {...config, partitionSize: 20}}));
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const input = () => page.getByLabel('分区数', {exact: true});
  const preview = page.getByRole('button', {name: '预览变更'});
  await input().fill('19');
  await expect(preview).toBeDisabled();
  await page.getByLabel('副本因子', {exact: true}).click();
  await expect(input()).toHaveValue('20');
  await expect(page.getByText('分区数只能增加，已恢复为当前配置（20）', {exact: true})).toBeVisible();
  await expect(preview).toBeDisabled();
  await input().fill('3');
  await input().press('Enter');
  await expect(input()).toHaveValue('20');
  await expect(preview).toBeDisabled();
  await input().fill('');
  await input().pressSequentially('50');
  await expect(input()).toHaveValue('50');
  await expect(preview).toBeEnabled();
  await input().press('Enter');
  await expect(input()).toHaveValue('50');
  await expect(page.getByText(/已恢复为当前配置/)).toHaveCount(0);
  await input().fill('20');
  await expect(preview).toBeDisabled();
});
test('configuration refreshed during preview disables a now redundant submission', async ({page}) => {
  let currentConfig = config;
  let writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => {
    if (r.request().method() === 'PATCH') writes++;
    return r.fulfill({json: currentConfig});
  });
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByLabel('分区数', {exact: true}).fill('6');
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(page.getByRole('button', {name: '确认提交', exact: true})).toBeEnabled();
  currentConfig = {...config, partitionSize: 6};
  await expect(page.getByRole('button', {name: '确认提交', exact: true})).toBeDisabled({timeout: 10000});
  expect(writes).toBe(0);
});
test('wide partition matrix fixed column stays opaque while hovering after horizontal scroll', async ({page}) => {
  const ids = Array.from({length: 30}, (_, index) => index + 1);
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
    brokers: [
      {...topology.brokers[0], partitions: ids.map(id => ({...report, partitionId: id}))},
      {...topology.brokers[0], brokerId: 'b1', partitions: ids.map(id => ({...report, role: 'FOLLOWER', partitionId: id}))},
    ],
    partitions: ids.map(id => ({...topology.partitions[0], partitionId: id, replicas: ['b0', 'b1']})),
  }}));
  await page.setViewportSize({width: 1280, height: 1000});
  await go(page, '/clusters/demo?tab=partitions');
  const scroller = page.getByRole('region', {name: '分区归属矩阵，可横向滚动'});
  await expect(page.locator('.partition-matrix tbody tr')).toHaveCount(2);
  await scroller.hover();
  await page.mouse.wheel(2400, 0);
  await expect.poll(() => scroller.evaluate(el => el.scrollLeft)).toBeGreaterThan(0);
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    for (const id of ['b0', 'b1']) {
      const row = page.locator(`.partition-matrix [data-broker-id="${id}"]`);
      await row.locator('td[data-partition-id="30"]').hover();
      const backgrounds = await row.evaluate(el => {
        const first = el.querySelector('th')!;
        const css = getComputedStyle(first);
        const bounds = first.getBoundingClientRect();
        const node = document.elementFromPoint(bounds.right - 8, bounds.y + bounds.height / 2);
        return {color: css.backgroundColor, bodyColor: getComputedStyle(el.querySelector('td')!).backgroundColor,
          topIsFixedColumn: node === first || first.contains(node), firstX: bounds.x,
          scrollX: el.closest('.partition-matrix-scroll')!.getBoundingClientRect().x};
      });
      expect(backgrounds.color).toMatch(/^rgb\(/);
      expect(backgrounds.color).toBe(backgrounds.bodyColor);
      expect(backgrounds.topIsFixedColumn).toBe(true);
      expect(Math.abs(backgrounds.firstX - backgrounds.scrollX)).toBeLessThan(3);
    }
    await page.screenshot({path: `/tmp/taskite-partition-fixed-hover-${theme}.png`, fullPage: true, animations: 'disabled'});
  }
});
test('quorum recovery sends string confirmation and fencing proof', async ({page}) => {
  let body: unknown;
  await page.route('**/api/v1/broker/clusters/demo/partitions/1/quorum-recovery', r => {
    body = r.request().postDataJSON();
    return r.fulfill({status: 202, contentType: 'application/json',
      body: '{"clusterId":"demo","changeId":"change-1","partitionId":1,"survivor":"b0","recoveryEpoch":9223372036854775807,"status":"IN_PROGRESS"}'});
  });
  await go(page, '/clusters/demo?tab=partitions');
  await page.getByText('分区详情与高级操作', {exact: true}).click();
  await page.getByRole('button', {name: '高级操作', exact: true}).click();
  await page.getByLabel('Fencing Proof').fill('isolated old replicas');
  await page.getByLabel('二次确认').fill('demo/1');
  await page.getByRole('button', {name: '提交恢复'}).click();
  await expect(page).toHaveURL(/change-1$/);
  expect(body).toEqual({confirm: 'true', fencingProof: 'isolated old replicas'});
  await expect(page.getByText(/recoveryEpoch 9223372036854775807/)).toBeVisible();
});
test('interrupted write stays unknown and is not resubmitted', async ({page}) => {
  let writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/rebalance', r => {writes++; return r.abort('failed');});
  await rebalance(page);
  await expect(page.locator('.ant-modal')).toContainText('结果未知');
  await expect(page.getByRole('button', {name: '确认执行'})).toBeDisabled();
  expect(writes).toBe(1);
});
test('malformed acceptance displays protocol error and known change ID', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/rebalance', r => r.fulfill({status: 202, json: {changeId: 'change-1', status: 42}}));
  await rebalance(page);
  await expect(page.locator('.ant-modal')).toContainText('PROTOCOL_ERROR');
  await expect(page.locator('.ant-modal')).toContainText('change-1');
  await expect(page.getByRole('button', {name: '确认执行'})).toBeDisabled();
});
test('cancel rechecks specified change and retains original plan', async ({page}) => {
  let cancelled = false;
  await page.route('**/api/v1/broker/clusters/demo/changes/change-1', r => {
    if (r.request().method() === 'DELETE') {cancelled = true; return r.fulfill({status: 202, json: {clusterId: 'demo', changeId: 'change-1', status: 'CANCELLED'}});}
    return r.fulfill({json: {...change, status: cancelled ? 'CANCELLED' : 'IN_PROGRESS', plannedChanges: [{opId: 'original', type: 'MOVE', targetBroker: 'b0', priority: 100}]}});
  });
  await go(page, '/changes/clusters/demo/change-1');
  await page.getByRole('button', {name: '取消变更', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('取消不回滚');
  await page.getByRole('button', {name: '确认取消'}).click();
  await expect(page.getByText('已取消', {exact: true})).toBeVisible();
  await expect(page.getByText('original', {exact: true})).toBeVisible();
});
test('reconciling recovery plan explains cancellation restriction without DELETE', async ({page}) => {
  let deletes = 0;
  const id = 'recover:broker-4:3:8cad8e25-9ef6-47e5-8c89-e8fbea386b9f';
  await page.route('**/api/v1/broker/clusters/demo/changes/*', r => {
    if (r.request().method() === 'DELETE') deletes++;
    return r.fulfill({json: {...change, changeId: id, status: 'SUPERSEDING',
      changeType: 'PARTITION', cancellable: false, cancellationReason: 'CHANGE_RECONCILING'}});
  });
  await go(page, `/changes/clusters/demo/${encodeURIComponent(id)}`);
  await expect(page.getByRole('button', {name: '取消变更', exact: true})).toBeDisabled();
  await expect(page.getByText('暂不可取消', {exact: true})).toBeVisible();
  await expect(page.getByText('正在核验已提交的 Raft 配置', {exact: false})).toBeVisible();
  expect(deletes).toBe(0);
});
test('cancel recheck blocks a newly superseding plan', async ({page}) => {
  let reconciling = false;
  let deletes = 0;
  await page.route('**/api/v1/broker/clusters/demo/changes/change-1', r => {
    if (r.request().method() === 'DELETE') deletes++;
    return r.fulfill({json: {...change, changeType: 'PARTITION',
      status: reconciling ? 'SUPERSEDING' : 'WAITING_READY', cancellable: !reconciling,
      cancellationReason: reconciling ? 'CHANGE_RECONCILING' : ''}});
  });
  await go(page, '/changes/clusters/demo/change-1');
  await page.getByRole('button', {name: '取消变更', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('自动调度仍可能生成新的恢复计划');
  reconciling = true;
  await page.getByRole('button', {name: '确认取消'}).click();
  await expect(page.locator('.ant-modal')).toContainText('正在核验已提交的 Raft 配置');
  expect(deletes).toBe(0);
});
test('cancellation error code appears once and preserves the complete change ID', async ({page}) => {
  const id = 'recover:broker-4:3:8cad8e25-9ef6-47e5-8c89-e8fbea386b9f';
  await page.route('**/api/v1/broker/clusters/demo/changes/*', r => r.request().method() === 'DELETE'
    ? r.fulfill({status: 404, json: {error: 'CHANGE_NOT_FOUND', message: `CHANGE_NOT_FOUND: ${id}`}})
    : r.fulfill({json: {...change, changeId: id, changeType: 'PARTITION'}}));
  await go(page, `/changes/clusters/demo/${encodeURIComponent(id)}`);
  await page.getByRole('button', {name: '取消变更', exact: true}).click();
  await page.getByRole('button', {name: '确认取消'}).click();
  await expect(page.locator('.ant-modal .ant-alert-message')).toHaveText(`CHANGE_NOT_FOUND: ${id}`);
});
test('distribution keeps exact key and does not complete inconsistent zero targets', async ({page}) => {
  await page.route('**/api/v1/broker/job-subscriptions/9007199254741052', r => r.fulfill({
    contentType: 'application/json', body: JSON.stringify({...resource, valueType: 'FUTURE_VALUE_TYPE', status: 'FUTURE_STATE',
      targets: [{clusterId: 'target', expectedPartitions: 0, acknowledgedPartitions: 1, pendingPartitions: [], confirmedPartitionIds: [], status: 'FUTURE_STATE'}]})
      .replace('"distributionKey":"9007199254741052"', '"distributionKey":9007199254741052'),
  }));
  await go(page, '/subscriptions/distributions/9007199254741052');
  await expect(page).toHaveURL(/9007199254741052$/);
  await expect(page.getByText('无期望分区')).toBeVisible();
  await expect(page.getByText('计数不一致，请核对原始状态')).toBeVisible();
  await expect(page.getByRole('button', {name: '重新下发', exact: true})).toBeDisabled();
});

test('resource list filters columns locally and restores pagination after detail', async ({page}) => {
  const queries: string[] = [];
  const assets = Array.from({length: 25}, (_, i) => ({...resourceAssets[0], distributionKey: String(BigInt(resource.distributionKey) + BigInt(i))}));
  await page.route('**/api/v1/broker/job-subscriptions', route => {
    queries.push(new URL(route.request().url()).search);
    return route.fulfill({json: {total: assets.length + 1, subscriptions: [...assets, resourceAssets[1]]}});
  });
  await page.route('**/api/v1/broker/job-subscriptions/*', route => route.fulfill({
    json: {...resource, distributionKey: new URL(route.request().url()).pathname.split('/').pop()},
  }));
  await go(page, '/subscriptions/distributions');
  await expect(page.getByRole('tab', {name: '普通任务', exact: true})).toBeVisible();
  await expect(page.locator('.filter-toolbar')).toHaveCount(0);
  await expect(page.locator('.ant-table-thead th').first()).toHaveText('分发 Key');
  for (const [label, value] of [['任务类型', 'charge'], ['任务订阅名称', 'ORDER'], ['所属租户', 'a'], ['所属集群', 'demo']]) {
    await page.locator('.ant-table-thead').getByLabel(`按${label}筛选`).click();
    const dropdown = page.getByRole('group', {name: `${label}筛选`, exact: true});
    await dropdown.getByRole('textbox', {name: `按${label}筛选`}).fill(value);
    await dropdown.getByRole('button', {name: '筛选', exact: true}).click();
  }
  await expect(page.locator('.resource-statistics .metric-strip')).toHaveCount(1);
  await expect(page.locator('.resource-statistics .metric-value').first()).toHaveText('26');
  await expect(page.locator('[data-cluster-id="demo"] .cluster-count-value').first()).toHaveText('25');
  await expect(page.locator('[data-cluster-id="target"] .cluster-count-value').first()).toHaveText('1');
  await expect(page.locator('.ant-card-head-title').getByText('任务订阅 · 25', {exact: true})).toBeVisible();
  await page.locator('.ant-pagination-item-2').click();
  await page.getByRole('button', {name: '详情', exact: true}).first().click();
  await expect(page.getByRole('heading', {name: '任务订阅详情', exact: true})).toBeVisible();
  await page.getByRole('button', {name: '返回列表', exact: true}).click();
  await expect(page.locator('.ant-pagination-item-active')).toHaveText('2');
  await expect(page).toHaveURL(/jobType=charge/);
  await expect(page).not.toHaveURL(/assetClusterId/);
  await page.reload();
  await expect(page.locator('.ant-pagination-item-active')).toHaveText('2');
  await page.locator('.ant-table-thead').getByLabel('按任务类型筛选').click();
  const dropdown = page.getByRole('group', {name: '任务类型筛选', exact: true});
  await dropdown.getByRole('textbox').fill('order');
  await dropdown.getByRole('button', {name: '筛选', exact: true}).click();
  await expect(page.locator('.ant-table-row')).toHaveCount(0);
  await expect(page.locator('.resource-statistics .metric-value').first()).toHaveText('26');
  await expect(page.locator('[data-cluster-id="demo"] .cluster-count-value').first()).toHaveText('25');
  await page.getByRole('button', {name: '清除筛选', exact: true}).click();
  await expect(page.locator('.ant-table-row')).toHaveCount(20);
  expect(queries.length).toBeGreaterThan(0);
  expect(queries.every(query => query === '')).toBe(true);
});

test('resource statistics separate deployment and mirror and omit deleted assets', async ({page}) => {
  await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
    total: 3, subscriptions: [
      {...resourceAssets[0], status: 'COMPLETED', targets: [{...resource.targets[0], status: 'COMPLETED', acknowledgedPartitions: 2, pendingPartitions: [], confirmedPartitionIds: [1, 2]}]},
      resourceAssets[1],
      {...resourceAssets[0], distributionKey: '100', name: 'deleted', status: 'DELETED'},
    ],
  }}));
  await go(page, '/subscriptions/distributions');
  await expect(page.locator('.resource-statistics .metric-value')).toHaveText(['2', '1', '1', '1']);
  await expect(page.locator('[data-cluster-id="demo"] .cluster-count-value')).toHaveText(['1', '1', '0', '1']);
  await expect(page.locator('[data-cluster-id="target"] .cluster-count-value')).toHaveText(['1', '0', '1', '0']);
  await expect(page.locator('.ant-table-row')).toHaveCount(2);
  await expect(page.getByRole('button', {name: 'deleted', exact: true})).toHaveCount(0);
  const mirrorRow = page.locator('.ant-table-row').filter({hasText: 'Mirror'});
  await mirrorRow.getByRole('button', {name: '详情', exact: true}).click();
  await expect(page.locator('.ant-descriptions').getByRole('button', {name: 'target', exact: true})).toBeVisible();
  await expect(page.locator('.resource-information .ant-tag').getByText('Mirror', {exact: true})).toBeVisible();
  await expect(page.locator('.resource-information .resource-text').getByText('demo', {exact: true})).toBeVisible();
  await page.getByRole('button', {name: '返回列表', exact: true}).click();
  await expect(page.locator('.ant-table-row')).toHaveCount(2);
});

for (const theme of ['light', 'dark']) {
  test(`resource statistics merge totals and show all clusters at adaptive density (${theme})`, async ({page}) => {
    let clusterCount = 2;
    const longCluster = 'cluster-with-a-very-long-name-for-resource-statistics';
    await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
      total: clusterCount * 2,
      subscriptions: Array.from({length: clusterCount}, (_, i) => {
        const clusterId = i === clusterCount - 1 ? longCluster : `cluster-${String(i + 1).padStart(2, '0')}`;
        return [
          {...resourceAssets[0], clusterId, distributionKey: String(100 + i * 2), status: 'COMPLETED'},
          {...resourceAssets[1], clusterId, distributionKey: String(101 + i * 2)},
        ];
      }).flat(),
    }}));
    await page.setViewportSize({width: 1920, height: 1080});
    await go(page, '/subscriptions/distributions');
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    const statistics = page.getByRole('region', {name: '任务订阅统计', exact: true});
    let normalEntryHeight = 0;
    for (const [count, density] of [[2, 'normal'], [8, 'compact'], [24, 'dense']] as const) {
      clusterCount = count;
      if (count > 2) await page.getByRole('button', {name: '刷新', exact: true}).click();
      await expect(statistics.locator('.metric-value')).toHaveText([String(count * 2), String(count), String(count), String(count)]);
      await expect(statistics.locator('.cluster-count-breakdown').first()).toHaveClass(new RegExp(`cluster-count-${density}`));
      await expect(statistics.locator('.metric-strip')).toHaveCount(1);
      await expect(statistics.locator('[data-cluster-id]')).toHaveCount(count * 4);
      await expect(statistics.locator(`[data-cluster-id="${longCluster}"] .cluster-count-value`)).toHaveText(['2', '1', '1', '1']);
      const primaryColor = await statistics.locator('.metric-value').first().evaluate(el => getComputedStyle(el).color);
      await expect(statistics.locator('.metric-value').nth(1)).toHaveCSS('color', primaryColor);
      const infoColor = await page.evaluate(() => {
        const probe = document.createElement('span');
        probe.style.color = 'hsl(var(--info))';
        document.body.appendChild(probe);
        const color = getComputedStyle(probe).color;
        probe.remove();
        return color;
      });
      await expect(statistics.locator('.metric-value').nth(2)).toHaveCSS('color', infoColor);
      for (const [index, color] of [[1, primaryColor], [2, infoColor]] as const) {
        for (const value of await statistics.locator('.metric-item').nth(index).locator('.cluster-count-value').all()) {
          await expect(value).toHaveCSS('color', color);
        }
      }
      const height = await statistics.locator('.cluster-count-entry').first().evaluate(el => el.getBoundingClientRect().height);
      if (count === 2) normalEntryHeight = height;
      else expect(height).toBeLessThan(normalEntryHeight);
    }
    for (const width of [1920, 1280, 720]) {
      await page.setViewportSize({width, height: 1080});
      const layout = await statistics.locator('.cluster-count-entry').evaluateAll(entries => entries.map(entry => {
        const box = entry.getBoundingClientRect();
        const item = entry.closest('.metric-item')!.getBoundingClientRect();
        return {width: box.width, left: box.left, right: box.right, bottom: box.bottom,
          itemLeft: item.left, itemRight: item.right, itemBottom: item.bottom};
      }));
      expect(layout).toHaveLength(96);
      for (const entry of layout) {
        expect(entry.width).toBeGreaterThan(60);
        expect(entry.left).toBeGreaterThanOrEqual(entry.itemLeft);
        expect(entry.right).toBeLessThanOrEqual(entry.itemRight);
        expect(entry.bottom).toBeLessThanOrEqual(entry.itemBottom);
      }
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      if (width !== 1280) await statistics.screenshot({path: `target/resource-statistics-many-${theme}-${width}.png`, animations: 'disabled'});
    }
    const longName = statistics.locator(`[data-cluster-id="${longCluster}"] .cluster-count-name`).first();
    expect(await longName.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true);
    await longName.hover();
    await expect(page.getByRole('tooltip', {name: longCluster, exact: true})).toBeVisible();
  });
}

test('resource details display original text and mirror progress with navigation both ways', async ({page}) => {
  await go(page, `/subscriptions/distributions/${resource.distributionKey}`);
  await expect(page.getByText('已确认分区：', {exact: true})).toHaveCount(2);
  await expect(page.locator('pre')).toHaveText(JSON.stringify(resource.subscription, null, 2));
  await page.getByRole('button', {name: '查看 Mirror', exact: true}).click();
  await expect(page).toHaveURL(/subscriptions\/mirrors\/42$/);
  await page.getByRole('button', {name: '查看任务订阅分发', exact: true}).click();
  await expect(page).toHaveURL(/clusterId=target&sourceClusterId=demo&creationType=MIRROR/);
  await expect(page.locator('.ant-table-row')).toHaveCount(1);
  await expect(page.locator('.ant-table-row').getByText('Mirror', {exact: true})).toBeVisible();
  await expect(page.locator('.ant-table-row').getByText('0 / 1', {exact: true})).toBeVisible();
});

test('resource redistribution sends exact generation and refreshes acknowledgements', async ({page}) => {
  let generation = '9007199254741053';
  let accepted = false;
  let received: unknown;
  await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}`, route => route.fulfill({
    contentType: 'application/json', body: JSON.stringify({...resource, generation,
      targets: accepted ? resource.targets.map(t => ({...t, acknowledgedPartitions: 0,
        confirmedPartitionIds: [], pendingPartitions: Array.from({length: t.expectedPartitions}, (_, i) => i + 1)})) : resource.targets})
      .replace(/"generation":"(\d+)"/, '"generation":$1'),
  }));
  await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}/redistribute`, route => {
    received = route.request().postDataJSON(); accepted = true; generation = '9007199254741054';
    return route.fulfill({status: 202, json: {distributionKey: resource.distributionKey, generation, lastRedistributedAt: 1793400000000, status: 'DISTRIBUTING'}});
  });
  await go(page, `/subscriptions/distributions/${resource.distributionKey}`);
  await page.getByRole('button', {name: '重新下发', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('demo、target');
  await page.getByRole('button', {name: '确认下发', exact: true}).click();
  await expect(page.locator('.ant-modal')).not.toBeVisible();
  expect(received).toEqual({expectedGeneration: '9007199254741053'});
  await expect(page.getByText('已受理重新下发', {exact: false})).toBeVisible();
  await expect(page.getByText('9007199254741054', {exact: true})).toBeVisible();
  await expect(page.getByText('暂无', {exact: true})).toHaveCount(2);
});

test('resource redistribution handles rejected and unknown results without resubmitting', async ({page}) => {
  let attempts = 0;
  await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}/redistribute`, route => {
    attempts++;
    return attempts === 1 ? route.fulfill({status: 409, json: {error: 'JOB_SUBSCRIPTION_DISTRIBUTION_GENERATION_CONFLICT', message: '订阅轮次已变化'}})
      : route.abort('failed');
  });
  await go(page, `/subscriptions/distributions/${resource.distributionKey}`);
  await page.getByRole('button', {name: '重新下发', exact: true}).click();
  await page.getByRole('button', {name: '确认下发', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('订阅轮次已变化');
  await page.getByRole('button', {name: '确认下发', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('重新下发结果未知');
  await expect(page.getByRole('button', {name: '确认下发', exact: true})).toBeDisabled();
  expect(attempts).toBe(2);
});

test('completed resource continues polling and deleted resources disable redistribution', async ({page}) => {
  let deleted = false;
  await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}`, route => route.fulfill({
    json: {...resource, status: deleted ? 'DELETED' : 'COMPLETED', targets: resource.targets.map(t =>
      ({...t, status: 'COMPLETED', acknowledgedPartitions: t.expectedPartitions, pendingPartitions: [],
        confirmedPartitionIds: Array.from({length: t.expectedPartitions}, (_, i) => i + 1)}))},
  }));
  await go(page, `/subscriptions/distributions/${resource.distributionKey}`);
  await expect(page.getByRole('button', {name: '重新下发', exact: true})).toBeEnabled();
  deleted = true;
  await expect(page.getByText('已删除', {exact: true})).toHaveCount(2, {timeout: 7000});
  await expect(page.getByRole('button', {name: '重新下发', exact: true})).toBeDisabled();
});

for (const theme of ['light', 'dark']) {
  test(`resource list and detail use product layout (${theme})`, async ({page}) => {
    const processId = 'order-process-with-a-very-long-name-for-resource-overflow-check';
    const tenantId = 'tenant-with-a-very-long-name-for-resource-overflow-check';
    const completedTargets = resource.targets.map(target => ({...target, status: 'COMPLETED',
      acknowledgedPartitions: target.expectedPartitions, pendingPartitions: [],
      confirmedPartitionIds: Array.from({length: target.expectedPartitions}, (_, i) => i + 1)}));
    await page.route('**/api/v1/broker/job-subscriptions', route => route.fulfill({json: {
      total: 2, subscriptions: resourceAssets.map(asset => ({...asset, jobType: processId, tenantId,
        status: 'COMPLETED', targets: completedTargets.filter(target => target.clusterId === asset.clusterId)})),
    }}));
    await page.route(`**/api/v1/broker/job-subscriptions/${resource.distributionKey}`, route => route.fulfill({
      json: {...resource, jobType: processId, tenantId, status: 'COMPLETED', targets: completedTargets},
    }));
    await page.setViewportSize({width: 1920, height: 1080});
    const palette = (el: Element) => {
      const style = getComputedStyle(el);
      return {color: style.color, background: style.backgroundColor, border: style.borderColor,
        borderWidth: style.borderWidth, radius: style.borderRadius, fontWeight: style.fontWeight};
    };
    await go(page, '/clusters/demo');
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    await page.getByRole('tab', {name: 'Broker', exact: true}).click();
    const brokerPill = page.locator('.broker-list-table .number-tag').first();
    await expect(brokerPill).toBeVisible();
    const productPillPalette = await brokerPill.evaluate(palette);
    await go(page, '/workers');
    const workerStatus = page.locator('.ant-table-row .ant-tag-success').first();
    await expect(workerStatus).toBeVisible();
    const productStatusPalette = await workerStatus.evaluate(palette);
    await go(page, '/subscriptions/distributions');
    await expect(page.getByRole('button', {name: 'order', exact: true})).toHaveCount(2);
    await expect(page.locator('.resource-statistics .metric-strip')).toHaveCount(1);
    await expect(page.locator('.ant-table-thead').getByText('任务类型', {exact: true})).toBeVisible();
    const row = page.locator('.ant-table-row').first();
    for (const pill of await page.locator('.resource-list-table .resource-pill').all()) {
      expect(await pill.evaluate(palette)).toEqual(productPillPalette);
    }
    expect(await row.locator('.ant-tag-success').evaluate(palette)).toEqual(productStatusPalette);
    for (const [selector, value] of [['.resource-text-job', processId], ['.resource-text-tenant', tenantId]]) {
      const text = row.locator(selector);
      await expect(text.locator('.ant-tag')).toHaveCount(0);
      expect(await text.evaluate(el => el.scrollWidth > el.clientWidth)).toBe(true);
      await text.hover();
      await expect(page.getByRole('tooltip', {name: value, exact: true})).toBeVisible();
      await page.getByRole('heading', {name: '任务订阅', exact: true}).hover();
      await expect(page.getByRole('tooltip', {name: value, exact: true})).not.toBeVisible();
    }
    await page.context().grantPermissions(['clipboard-read', 'clipboard-write']);
    await row.getByRole('button', {name: `复制 ${resource.distributionKey}`, exact: true}).click();
    await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe(resource.distributionKey);
    await expect(page).toHaveURL(/subscriptions\/distributions$/);
    await page.screenshot({path: `target/resource-list-${theme}.png`, animations: 'disabled', fullPage: true});
    await page.locator('.ant-table-row').nth(1).getByRole('link', {name: resource.distributionKey, exact: true}).click();
    await expect(page).toHaveURL(/assetClusterId=target/);
    await expect(page.locator('.resource-information').getByRole('button', {name: 'target', exact: true})).toBeVisible();
    await page.getByRole('button', {name: '返回列表', exact: true}).click();
    await row.getByRole('link', {name: resource.distributionKey, exact: true}).click();
    await expect(page).toHaveURL(/assetClusterId=demo/);
    await expect(page.locator('pre')).toBeVisible();
    const header = page.locator('.page-header');
    await expect(header.getByRole('button', {name: '重新下发', exact: true})).toBeVisible();
    const information = page.locator('.resource-information');
    await expect(information.locator('.ant-descriptions-item-label').getByText('任务类型', {exact: true})).toBeVisible();
    await expect(information.getByText('请求标识', {exact: true})).toHaveCount(0);
    const checkInlineFields = async () => {
      const fields = await information.locator('.ant-descriptions-item-container').evaluateAll(items =>
        items.map(item => {
          const label = item.querySelector('.ant-descriptions-item-label')!.getBoundingClientRect();
          const content = item.querySelector('.ant-descriptions-item-content')!.getBoundingClientRect();
          return {label: label.top, content: content.top, left: content.left, right: label.right};
        }));
      expect(fields.length).toBe(11);
      for (const field of fields) {
        expect(Math.abs(field.label - field.content)).toBeLessThan(8);
        expect(field.left).toBeGreaterThanOrEqual(field.right - 1);
      }
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    };
    await checkInlineFields();
    for (const pill of await page.locator('.resource-detail-page .resource-pill').all()) {
      expect(await pill.evaluate(palette)).toEqual(productPillPalette);
    }
    for (const status of await page.locator('.resource-detail-page .ant-tag-success').all()) {
      expect(await status.evaluate(palette)).toEqual(productStatusPalette);
    }
    const target = page.locator('.distribution-target').first();
    await expect(target.getByRole('button', {name: 'demo', exact: true})).toBeVisible();
    await page.screenshot({path: `target/resource-detail-${theme}.png`, animations: 'disabled', fullPage: true});
    await page.setViewportSize({width: 720, height: 1000});
    await checkInlineFields();
    await page.screenshot({path: `target/resource-detail-narrow-${theme}.png`, animations: 'disabled', fullPage: true});
  });
}
test('404 known change never implies completion', async ({page}) => {
  await go(page, '/changes/pd/not-retained');
  await expect(page.getByText('记录未找到或读取失败，不能据此判断操作完成。')).toBeVisible();
});
test('follower console keeps mutation entry points and sends writes to the current HTTP node', async ({page}) => {
  await page.route('**/api/v1/pd/topology', r => r.fulfill({json: {
    localMemberId: 'pd-1', leaderId: 'pd-0', term: 1, quorum: 1,
    members: [{memberId: 'pd-0', role: 'LEADER', self: false}, {memberId: 'pd-1', role: 'FOLLOWER', self: true}],
  }}));
  let submitted = false;
  await page.route('**/api/v1/broker/clusters/demo/rebalance', r => {
    submitted = r.request().method() === 'POST';
    return r.fulfill({json: {changeId: 'NO_CHANGES', status: 'NO_CHANGES'}});
  });
  await go(page, '/pd');
  await expect(page.getByRole('button', {name: '修改配置', exact: true})).toBeEnabled();
  await expect(page.getByText('只读模式', {exact: true})).toHaveCount(0);
  await go(page, '/clusters');
  await expect(page.getByRole('button', {name: '创建集群', exact: true})).toBeEnabled();
  await page.getByRole('button', {name: 'Rebalance', exact: true}).click();
  await page.getByRole('button', {name: '确认执行'}).click();
  await expect(page.getByText('无需变更，已刷新配置')).toBeVisible();
  expect(submitted).toBe(true);
});
test('Mirror delete consumes 204 and refreshes list', async ({page}) => {
  let deleted = false;
  await page.route('**/api/v1/broker/job-subscription-mirrors', r => r.fulfill({json: {mirrors: deleted ? [] : [{mirrorKey: 42, sourceClusterId: 'demo', targetClusterId: 'target', status: 'ACTIVE'}]}}));
  await page.route('**/api/v1/broker/job-subscription-mirrors/42', r => {deleted = true; return r.fulfill({status: 204});});
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '删除', exact: true}).click();
  await page.getByRole('button', {name: '确认删除'}).click();
  await expect(page.getByText('暂无 Mirror 关系')).toBeVisible();
});
test('domain conflict preserves error code leader and associated changes', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/rebalance', r => r.fulfill({status: 409, json: {
    error: 'CHANGE_IN_PROGRESS', message: '已有变更', pendingChangeIds: ['change-1'], leaderEndpoint: 'localhost:26501'}}));
  await rebalance(page);
  await expect(page.locator('.ant-modal')).toContainText('CHANGE_IN_PROGRESS');
  await expect(page.locator('.ant-modal')).toContainText('change-1');
  await expect(page.locator('.ant-modal')).toContainText('localhost:26501');
});
test('protocol failure preserves last good worker snapshot', async ({page}) => {
  let invalid = false;
  await page.route('**/api/v1/workers', r => r.fulfill({json: invalid ? {total: 'bad'} : {generatedAt: worker.lastRenewal, total: 1, online: 1, offline: 0, workers: [worker]}}));
  await go(page, '/workers');
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toBeVisible();
  invalid = true;
  await page.getByRole('button', {name: '刷新', exact: true}).click();
  await expect(page.getByText(/PROTOCOL_ERROR/)).toBeVisible();
  await expect(page.getByText(/数据可能已过时/)).toBeVisible();
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toBeVisible();
});
test('no registered broker renders waiting view and allows cluster deletion', async ({page}) => {
  await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: {count: 1, clusters: [{...config, registeredBrokerCount: 0, onlineBrokerCount: 0}]}}));
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({status: 503, json: {error: 'NO_BROKER_REGISTERED'}}));
  await page.route('**/api/v1/broker/clusters/demo', r => r.fulfill({status: 204}));
  await go(page, '/clusters/demo');
  await expect(page.getByText('等待 Broker 注册', {exact: true})).toBeVisible();
  await expect(page.getByRole('button', {name: '修改配置', exact: true})).toBeDisabled();
  await go(page, '/clusters');
  await page.getByRole('button', {name: '删除', exact: true}).click();
  await expect(page.getByText('该集群作为 Source 或 Target 的所有 Mirror 关系也将一并删除。', {exact: true})).toBeVisible();
  await page.getByPlaceholder('输入 demo').fill('demo');
  await page.getByRole('button', {name: '确认删除'}).click();
  await expect(page.locator('.ant-modal')).not.toBeVisible();
});
test('known history remains isolated by deployment', async ({page}) => {
  await go(page, '/changes/pd/pd-change');
  await expect(page.getByText('已完成', {exact: true})).toBeVisible();
  await go(page, '/changes');
  await expect(page.getByText('访问记录 (1)')).toBeVisible();
  await page.route('**/api/v1/console/bootstrap', r => r.fulfill({json: {productName: 'Taskite Console', consoleBasePath: '/console/', apiBasePath: '/api/v1',
    buildVersion: 'test', deploymentId: 'another-deployment', nodeId: 'pd-0'}}));
  await page.reload();
  await expect(page.getByRole('heading', {name: '变更追踪'})).toBeVisible();
  await expect(page.getByText('访问记录 (1)')).toHaveCount(0);
});
test('priority map broker names matching long fields remain integer values', async ({page}) => {
  await page.route('**/api/v1/broker/clusters/demo/changes/change-1', r => r.fulfill({json: {...change, status: 'COMPLETED',
    currentTopology: [{partitionId: 1, status: 'COMPLETED', desiredVoters: ['term'], currentVoters: ['term'], learners: [],
      desiredPrimary: 'term', priorities: {term: 100, epoch: 1}}]}}));
  await go(page, '/changes/clusters/demo/change-1');
  await page.locator('.ant-table-row-expand-icon').click();
  await expect(page.getByText('Priorities：{"term":100,"epoch":1}')).toBeVisible();
});
test('Mirror ACTIVE badge uses Worker style and endpoints remain symmetric in both themes', async ({page}) => {
  const mirror = {mirrorKey: 42, sourceClusterId: 'source-cluster-with-a-long-identifier', targetClusterId: 'target', status: 'ACTIVE'};
  await page.route('**/api/v1/broker/job-subscription-mirrors/42', r => r.fulfill({json: mirror}));
  await go(page, '/subscriptions/mirrors');
  const tag = page.locator('.ant-table-row .ant-tag').filter({hasText: /^活跃$/});
  await expect(tag).toHaveClass(/ant-tag-success/);
  await expect(tag).toContainText('活跃');
  await expect(tag.locator('svg')).toHaveCount(1);
  await go(page, '/subscriptions/mirrors/42');
  for (const dark of [false, true]) {
    if (dark) await page.getByRole('button', {name: '切换暗色主题'}).click();
    const endpoints = page.locator('.mirror-endpoint');
    await expect(endpoints).toHaveCount(2);
    const source = (await endpoints.nth(0).boundingBox())!;
    const target = (await endpoints.nth(1).boundingBox())!;
    const arrow = (await page.locator('.mirror-route-arrow svg').boundingBox())!;
    expect(Math.abs(source.width - target.width)).toBeLessThan(1);
    expect(Math.abs(source.y - target.y)).toBeLessThan(1);
    expect(Math.abs(arrow.x + arrow.width / 2 - (source.x + source.width + target.x) / 2)).toBeLessThan(1);
    await expect(page.locator('.object-badges .ant-tag')).toHaveClass(/ant-tag-success/);
    await page.screenshot({path: `target/mirror-detail-${dark ? 'dark' : 'light'}.png`, fullPage: true, animations: 'disabled'});
  }
  await page.setViewportSize({width: 480, height: 900});
  const endpoints = page.locator('.mirror-endpoint');
  const source = (await endpoints.nth(0).boundingBox())!;
  const target = (await endpoints.nth(1).boundingBox())!;
  expect(target.y).toBeGreaterThan(source.y + source.height);
  expect(Math.abs(source.x - target.x)).toBeLessThan(1);
  await expect.poll(() => page.evaluate(() =>
    document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
test('Worker header filters preserve other URL fields and can reset from an empty result', async ({page}) => {
  await page.route('**/api/v1/workers?**', r => r.fulfill({json: {generatedAt: worker.lastRenewal, total: 0, online: 0, offline: 0, workers: []}}));
  await go(page, '/workers?cluster=demo&worker=selected-key');
  await expect(page.locator('.filter-toolbar')).toHaveCount(0);
  const filter = (label: string) => page.locator('.ant-table-filter-trigger').filter({has: page.locator(`[aria-label="按${label}筛选"]`)});
  await filter('租户').click();
  await page.getByRole('textbox', {name: '按租户筛选'}).fill(' a ');
  await page.getByRole('button', {name: '筛选', exact: true}).click();
  await expect(page).toHaveURL(/cluster=demo&worker=selected-key&tenant=a$/);
  await expect(page.getByText('暂无符合筛选条件的 Worker，可在表头重置筛选')).toBeVisible();
  await filter('名称').click();
  await page.getByRole('textbox', {name: '按名称筛选'}).fill('payment');
  await page.getByRole('textbox', {name: '按名称筛选'}).press('Enter');
  await expect(page).toHaveURL(/tenant=a&name=payment$/);
  await filter('集群').click();
  await page.getByRole('group', {name: '集群筛选'}).getByRole('button', {name: '重置', exact: true}).click();
  await expect(page).toHaveURL(/worker=selected-key&tenant=a&name=payment$/);
  await page.reload();
  await expect(filter('租户')).toHaveClass(/active/);
  await expect(filter('名称')).toHaveClass(/active/);
  await expect(filter('集群')).not.toHaveClass(/active/);
});
test('return navigation shows the cached snapshot until the background refresh finishes', async ({page}) => {
  let hold = false;
  let release!: () => void;
  const gate = new Promise<void>(resolve => {release = resolve;});
  await page.route('**/api/v1/workers', async r => {
    const delayed = hold;
    if (delayed) await gate;
    await r.fulfill({json: {generatedAt: worker.lastRenewal, total: 1, online: 1, offline: 0,
      workers: [{...worker, name: delayed ? 'updated-worker' : 'payment'}]}});
  });
  await go(page, '/workers');
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toBeVisible();
  await page.getByRole('menuitem', {name: 'Broker 集群', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Broker 集群', exact: true})).toBeVisible();
  hold = true;
  await page.getByRole('menuitem', {name: 'Worker', exact: true}).click();
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toBeVisible();
  await expect(page.locator('.ant-skeleton')).toHaveCount(0);
  await expect(page.locator('.console-content > div')).toHaveCSS('animation-name', 'none');
  release();
  await expect(page.getByRole('button', {name: 'updated-worker', exact: true})).toBeVisible();
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toHaveCount(0);
});
test('a new Worker filter never displays rows from another cached query', async ({page}) => {
  let release!: () => void;
  const gate = new Promise<void>(resolve => {release = resolve;});
  await page.route('**/api/v1/workers?**', async r => {
    await gate;
    await r.fulfill({json: {generatedAt: worker.lastRenewal, total: 0, online: 0, offline: 0, workers: []}});
  });
  await go(page, '/workers');
  await expect(page.getByRole('button', {name: 'payment', exact: true}).first()).toBeVisible();
  await page.locator('.ant-table-filter-trigger').filter({has: page.locator('[aria-label="按租户筛选"]')}).click();
  await page.getByRole('textbox', {name: '按租户筛选'}).fill('missing');
  await page.getByRole('button', {name: '筛选', exact: true}).click();
  await expect(page).toHaveURL(/tenant=missing$/);
  await expect(page.getByRole('button', {name: 'payment', exact: true})).toHaveCount(0);
  release();
  await expect(page.getByText('暂无符合筛选条件的 Worker，可在表头重置筛选')).toBeVisible();
});
test('an authoritative 404 discards cached Mirror detail', async ({page}) => {
  let missing = false;
  await page.route('**/api/v1/broker/job-subscription-mirrors/42', r => missing
    ? r.fulfill({status: 404, json: {error: 'MIRROR_NOT_FOUND'}})
    : r.fulfill({json: {mirrorKey: 42, sourceClusterId: 'demo', targetClusterId: 'target', status: 'ACTIVE'}}));
  await go(page, '/subscriptions/mirrors/42');
  await expect(page.locator('.mirror-route')).toBeVisible();
  await page.getByRole('menuitem', {name: 'Worker', exact: true}).click();
  await expect(page.getByRole('heading', {name: 'Worker', exact: true})).toBeVisible();
  missing = true;
  await page.goBack();
  await expect(page.getByText(/MIRROR_NOT_FOUND/)).toBeVisible();
  await expect(page.locator('.mirror-route')).toHaveCount(0);
});
test('navigation font remains lighter than hover and selected items', async ({page}) => {
  await go(page, '/workers');
  const item = page.getByRole('menuitem', {name: 'Broker 集群', exact: true});
  await page.getByRole('heading', {name: 'Worker', exact: true}).hover();
  await expect(item).toHaveCSS('font-weight', '500');
  await expect(item.locator('.ant-menu-title-content')).toHaveCSS('font-size', '14px');
  await item.hover();
  await expect(item).toHaveCSS('font-weight', '600');
  await expect(page.getByRole('menuitem', {name: 'Worker', exact: true})).toHaveCSS('font-weight', '600');
});
test('PD voters need no promotion even when heartbeat readiness is false', async ({page}) => {
  await page.route('**/api/v1/pd/topology', r => r.fulfill({json: {
    localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 2,
    members: [
      {memberId: 'pd-0', role: 'LEADER', self: true, readyForPromotion: false},
      {memberId: 'pd-1', role: 'FOLLOWER', self: false, readyForPromotion: false},
      {memberId: 'pd-2', role: 'Follower', self: false},
    ],
  }}));
  await page.route('**/api/v1/pd/configuration', r => r.fulfill({json: {
    epoch: 1, target: {count: 3, voters: ['pd-0', 'pd-1', 'pd-2']},
    actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0', 'pd-1', 'pd-2'], learners: []},
  }}));
  await go(page, '/pd');
  await expect(page.locator('.pd-members-panel .ant-tag', {hasText: /^无需提升$/})).toHaveCount(3);
  await expect(page.getByText('未就绪', {exact: true})).toHaveCount(0);
  await expect(page.locator('.pd-promotion-complete')).toHaveCount(3);
});
test('PD Learner promotion uses current operation progress instead of heartbeat readiness', async ({page}) => {
  await page.setViewportSize({width: 1440, height: 1000});
  await page.route('**/api/v1/pd/topology', r => r.fulfill({json: {
    localMemberId: 'pd-0', leaderId: 'pd-0', term: 1, quorum: 1,
    members: [
      {memberId: 'pd-0', role: 'LEADER', self: true, readyForPromotion: false, commitIndex: 100, lastApplied: 100},
      {memberId: 'pd-1', role: 'LEARNER', self: false, readyForPromotion: false, commitIndex: 96, lastApplied: 96},
      {memberId: 'pd-2', role: 'LEARNER', self: false, readyForPromotion: true, commitIndex: 100, lastApplied: 100},
    ].map((member, index) => ({...member, status: 'ONLINE', host: '127.0.0.1',
      httpPort: 19001 + index, grpcPort: 26501 + index * 10, raftPort: 26502 + index * 10})),
  }}));
  await page.route('**/api/v1/pd/configuration', r => r.fulfill({json: {
    epoch: 1, target: {count: 3, voters: ['pd-0', 'pd-1', 'pd-2']},
    actual: {leaderId: 'pd-0', term: 1, voters: ['pd-0'], learners: ['pd-1', 'pd-2']},
  }}));
  let status = 'WAITING_CATCH_UP';
  await page.route('**/api/v1/pd/changes/active', r => r.fulfill({json: {
    ...pdChange, status,
    currentOperation: {type: 'MEMBER_PROMOTE_VOTER', memberId: 'pd-1', status: 'IN_PROGRESS', barrierIndex: 100},
  }}));
  await go(page, '/pd');
  await expect(page.locator('.pd-page > .metric-strip .metric-value').nth(0)).toHaveText('1');
  await expect(page.locator('.pd-page > .metric-strip .metric-value').nth(1)).toHaveText('3');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    const backgrounds: string[] = [];
    for (const phase of ['WAITING_CATCH_UP', 'IN_PROGRESS']) {
      status = phase;
      await page.reload();
      const label = phase === 'WAITING_CATCH_UP' ? '追平中' : '提升中';
      await expect(page.locator('.pd-members-panel .ant-table-row', {hasText: 'pd-1'})).toContainText(label);
      await expect(page.locator('.pd-members-panel .ant-table-row', {hasText: 'pd-2'})).toContainText('等待提升');
      await expect(page.locator('.pd-members-panel .ant-table-row', {hasText: 'pd-0'})).toContainText('无需提升');
      const pills = await page.locator('.pd-promotion-pill').evaluateAll(elements => {
        const luminance = (color: string) => {
          const rgb = color.match(/[\d.]+/g)!.slice(0, 3).map(Number).map(v => {
            const c = v / 255; return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
          });
          return rgb[0] * 0.2126 + rgb[1] * 0.7152 + rgb[2] * 0.0722;
        };
        return elements.map(el => {
          const style = getComputedStyle(el);
          const fg = luminance(style.color), bg = luminance(style.backgroundColor);
          return {background: style.backgroundColor, rowBackground: getComputedStyle(el.closest('td')!).backgroundColor,
            borderColor: style.borderColor, borderWidth: style.borderWidth, radius: style.borderRadius,
            weight: style.fontWeight, contrast: (Math.max(fg, bg) + 0.05) / (Math.min(fg, bg) + 0.05)};
        });
      });
      expect(pills).toHaveLength(3);
      expect(new Set(pills.map(pill => pill.background)).size).toBe(3);
      for (const pill of pills) {
        expect(pill.background).not.toBe(pill.rowBackground);
        expect(pill.borderColor).not.toBe(pill.background);
        expect(pill.borderWidth).toBe('1px');
        expect(pill.radius).toBe('100px');
        expect(pill.weight).toBe('600');
        expect(pill.contrast).toBeGreaterThanOrEqual(4.5);
      }
      backgrounds.push(...pills.map(pill => pill.background));
      await page.mouse.move(0, 0);
      await page.locator('.pd-members-panel').screenshot({
        path: `/tmp/taskite-pd-promotion-${phase}-${theme}.png`, animations: 'disabled',
      });
    }
    expect(new Set(backgrounds).size).toBe(4);
  }
});
test('Broker add lists online spare and all-Learner nodes and cannot create arbitrary IDs', async ({page}) => {
  const second = {...learnerBroker('b4'), status: 'online', partitions: [{...report, role: 'learner'}]};
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
    brokers: [...healthyTopology.brokers, learnerBroker('b3'), second,
      {...learnerBroker('offline'), status: 'OFFLINE'},
      {...learnerBroker('mixed'), partitions: [{...report, role: 'LEARNER'}, {...report, partitionId: 2, role: 'FOLLOWER'}]},
      {...learnerBroker('empty'), partitions: []},
      {...learnerBroker('unknown'), partitions: [{...report, role: 'FUTURE_ROLE'}]},
      learnerBroker('already-voter'), learnerBroker('already-leader')],
    partitions: [{...topology.partitions[0], replicas: ['b0', 'already-voter'], leaderId: 'already-leader',
      learners: ['b3', 'b4']}],
  }}));
  let body: unknown;
  await page.route('**/api/v1/broker/clusters/demo/configuration', r => {
    if (r.request().method() !== 'PATCH') return r.fallback();
    body = r.request().postDataJSON();
    return r.fulfill({json: {changeId: 'NO_CHANGES', status: 'NO_CHANGES'}});
  });
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  const input = page.getByRole('combobox', {name: '添加 Broker', exact: true});
  await input.click();
  await expect(page.locator('.ant-select-dropdown:visible .ant-select-item-option')).toHaveText(['b3', 'b4', 'empty']);
  await input.fill('unregistered,manual');
  await expect(page.getByText('没有匹配的节点', {exact: true})).toBeVisible();
  await input.press('Enter');
  await expect(page.locator('.broker-config-editor .ant-select').first().locator('.ant-select-selection-item')).toHaveCount(0);
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await choose(page, '添加 Broker', 'b3');
  await choose(page, '添加 Broker', 'b4');
  await choose(page, '添加 Broker', 'empty');
  await page.getByRole('button', {name: '预览变更'}).click();
  await expect(page.locator('.ant-modal .configuration-change-add .configuration-member-id')).toHaveText(['b3', 'b4', 'empty']);
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.locator('.ant-modal')).not.toBeVisible();
  expect(body).toEqual({brokers: {add: ['b3', 'b4', 'empty']}});
});
test('Broker add explains empty candidates and blocks unreachable counts without blocking partition changes', async ({page}) => {
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await page.getByRole('combobox', {name: '添加 Broker', exact: true}).click();
  await expect(page.getByText('暂无可添加的在线备用节点', {exact: true})).toBeVisible();
  await page.getByRole('combobox', {name: '添加 Broker', exact: true}).press('Escape');
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('4');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeDisabled();
  await expect(page.locator('.configuration-validation-error')).toContainText('无法达到目标 Broker 数量 4');
  await page.getByLabel('目标 Broker 数量', {exact: true}).fill('');
  await page.getByLabel('分区数', {exact: true}).fill('6');
  await expect(page.getByRole('button', {name: '预览变更'})).toBeEnabled();
});
for (const preview of [false, true]) {
  test(`Broker candidate losing Learner eligibility disables ${preview ? 'confirmation' : 'preview'}`, async ({page}) => {
    let lost = false, writes = 0;
    await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
      brokers: [...healthyTopology.brokers, {...learnerBroker('b3'),
        partitions: [{...report, role: lost ? 'FOLLOWER' : 'LEARNER'}]}],
    }}));
    page.on('request', r => {if (r.method() === 'PATCH') writes++;});
    await go(page, '/clusters/demo');
    await page.getByRole('button', {name: '修改配置', exact: true}).click();
    await choose(page, '添加 Broker', 'b3');
    if (preview) await page.getByRole('button', {name: '预览变更'}).click();
    lost = true;
    const panel = page.locator(preview ? '.ant-modal' : '.broker-config-editor');
    await expect(panel).toContainText('所选 Broker 已不满足添加条件', {timeout: 15000});
    await expect(page.getByRole('button', {name: preview ? '确认提交' : '预览变更', exact: true})).toBeDisabled();
    expect(writes).toBe(0);
  });
}
test('Broker submit rechecks latest online status and blocks an obsolete selection', async ({page}) => {
  let offline = false, writes = 0;
  await page.route('**/api/v1/broker/clusters/demo/topology', r => r.fulfill({json: {...topology,
    brokers: [...healthyTopology.brokers, {...learnerBroker('b3'), status: offline ? 'OFFLINE' : 'ONLINE'}],
  }}));
  page.on('request', r => {if (r.method() === 'PATCH') writes++;});
  await go(page, '/clusters/demo');
  await page.getByRole('button', {name: '修改配置', exact: true}).click();
  await choose(page, '添加 Broker', 'b3');
  await page.getByRole('button', {name: '预览变更'}).click();
  offline = true;
  await page.getByRole('button', {name: '确认提交', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('INVALID_BROKER_SELECTION');
  expect(writes).toBe(0);
});
test('Mirror creation selects existing clusters, excludes the opposite endpoint and submits exact IDs', async ({page}) => {
  await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: clusterList(['demo', 'target', 'extra'])}));
  let body: unknown;
  await page.route('**/api/v1/broker/job-subscription-mirrors', r => {
    if (r.request().method() !== 'POST') return r.fallback();
    body = r.request().postDataJSON();
    return r.fulfill({json: {mirrorKey: 42, ...r.request().postDataJSON(), status: 'ACTIVE'}});
  });
  await page.route('**/api/v1/broker/job-subscription-mirrors/42', r => r.fulfill({
    json: {mirrorKey: 42, sourceClusterId: 'demo', targetClusterId: 'target', status: 'ACTIVE'},
  }));
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  await choose(page, '源集群 ID', 'demo');
  await page.getByRole('combobox', {name: '目标集群 ID', exact: true}).click();
  await expect(page.locator('.ant-select-dropdown:visible .ant-select-item-option')).toHaveText(['target', 'extra']);
  await choose(page, '目标集群 ID', 'target');
  await page.locator('.ant-select').filter({has: page.getByRole('combobox', {name: '源集群 ID', exact: true})})
    .locator('.ant-select-selector').click();
  await expect(page.locator('.ant-select-dropdown:visible .ant-select-item-option')).toHaveText(['demo', 'extra']);
  await page.getByRole('combobox', {name: '源集群 ID', exact: true}).press('Escape');
  await page.screenshot({path: '/tmp/taskite-mirror-select-light.png', fullPage: true, animations: 'disabled'});
  await page.getByRole('button', {name: '创建', exact: true}).click();
  await expect(page).toHaveURL(/subscriptions\/mirrors\/42$/);
  expect(body).toEqual({sourceClusterId: 'demo', targetClusterId: 'target'});
});
test('Mirror creation cannot turn arbitrary search text into a selected cluster', async ({page}) => {
  await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: clusterList(['demo', 'target'])}));
  let writes = 0;
  page.on('request', r => {if (r.method() === 'POST') writes++;});
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
  await choose(page, '源集群 ID', 'demo');
  const input = page.getByRole('combobox', {name: '目标集群 ID', exact: true});
  await input.fill('nonexistent-target');
  await expect(page.getByText('没有可选的目标集群', {exact: true})).toBeVisible();
  await input.press('Enter');
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  expect(writes).toBe(0);
});
for (const ids of [[], ['demo']]) {
  test(`Mirror creation requires two existing clusters with ${ids.length} registered`, async ({page}) => {
    await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: clusterList(ids)}));
    await go(page, '/subscriptions/mirrors');
    await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
    await expect(page.locator('.ant-modal')).toContainText('至少需要两个现有集群才能创建 Mirror');
    await expect(page.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  });
}
test('Mirror cluster list failure disables selection and can be refreshed', async ({page}) => {
  let failed = true;
  await page.route('**/api/v1/broker/clusters', r => failed
    ? r.fulfill({status: 503, json: {error: 'UNAVAILABLE', message: '集群列表暂不可用'}})
    : r.fulfill({json: clusterList(['demo', 'target'])}));
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('UNAVAILABLE');
  await expect(page.getByRole('combobox', {name: '源集群 ID', exact: true})).toBeDisabled();
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  failed = false;
  await page.locator('.ant-modal').getByRole('button', {name: '刷新', exact: true}).click();
  await choose(page, '源集群 ID', 'demo');
  await choose(page, '目标集群 ID', 'target');
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeEnabled();
});
test('Mirror polling clears a deleted selected cluster and requires a new choice', async ({page}) => {
  let deleted = false;
  await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: clusterList(deleted ? ['demo', 'extra'] : ['demo', 'target', 'extra'])}));
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
  await choose(page, '源集群 ID', 'demo');
  await choose(page, '目标集群 ID', 'target');
  deleted = true;
  await expect(page.locator('.ant-modal')).toContainText('所选集群已不存在，请重新选择', {timeout: 15000});
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeDisabled();
  await choose(page, '目标集群 ID', 'extra');
  await expect(page.getByRole('button', {name: '创建', exact: true})).toBeEnabled();
});
test('Mirror submit rereads clusters and does not POST when an endpoint was deleted', async ({page}) => {
  let deleted = false, writes = 0;
  await page.route('**/api/v1/broker/clusters', r => r.fulfill({json: clusterList(deleted ? ['demo'] : ['demo', 'target'])}));
  page.on('request', r => {if (r.method() === 'POST') writes++;});
  await go(page, '/subscriptions/mirrors');
  await page.getByRole('button', {name: '创建 Mirror', exact: true}).click();
  await choose(page, '源集群 ID', 'demo');
  await choose(page, '目标集群 ID', 'target');
  deleted = true;
  await page.getByRole('button', {name: '创建', exact: true}).click();
  await expect(page.locator('.ant-modal')).toContainText('INVALID_CLUSTER_SELECTION');
  expect(writes).toBe(0);
});
test('PD change detail groups snapshots and current operation with compact metadata across themes and widths', async ({page}) => {
  const operation = {type: 'MEMBER_REMOVE', memberId: 'meta-0', status: 'IN_PROGRESS'};
  await page.route('**/api/v1/pd/changes/pd-change', r => r.fulfill({json: {...pdChange,
    changeId: '0b7548b6-a3b6-4f2a-a837-24c6868e3c0c', status: 'IN_PROGRESS', targetCount: 2,
    targetVoters: ['meta-1', 'meta-2'], actualVoters: ['meta-0', 'meta-1', 'meta-2'],
    currentOperation: operation, operations: [operation],
  }}));
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  await go(page, '/changes/pd/pd-change');
  const details = page.locator('.pd-change-details');
  await expect(details.locator('.pd-change-information')).toContainText('Source Epoch');
  await expect(details.locator('.pd-change-information')).toContainText('Target Epoch');
  await expect(details.locator('.pd-change-current')).toContainText('移除成员');
  await expect(details.locator('.pd-change-current')).toContainText('meta-0');
  await expect(details.locator('.pd-change-operations th')).toHaveText(['操作类型', '成员节点', '执行状态', '追平屏障', '错误信息']);
  await expect(details.locator('.pd-change-operations .ant-tag[title="MEMBER_REMOVE"]')).toHaveText('移除成员');
  await expect(page.getByRole('button', {name: '取消变更', exact: true})).toHaveCount(0);
  for (const width of [2048, 1440, 1100, 768, 390]) {
    await page.setViewportSize({width, height: 1058});
    if (width === 390) await page.getByRole('button', {name: '收起导航'}).click();
    for (const theme of ['light', 'dark']) {
      if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
      await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
      const layout = await details.evaluate(el => {
        const box = (selector: string) => {const r = el.querySelector(selector)!.getBoundingClientRect(); return {x:r.x,y:r.y,w:r.width,h:r.height};};
        return {target:box('[aria-label="目标成员配置"]'), actual:box('[aria-label="实际成员状态"]'),
          members:box('.pd-change-membership'), current:box('.pd-change-current'), epoch:box('.pd-change-epoch'),
          fits:document.documentElement.scrollWidth <= innerWidth};
      });
      expect(layout.fits, JSON.stringify({width, theme, layout})).toBe(true);
      if (width >= 768) {
        expect(Math.abs(layout.target.w-layout.actual.w)).toBeLessThan(2);
        expect(Math.abs(layout.target.y-layout.actual.y)).toBeLessThan(2);
      } else expect(layout.actual.y).toBeGreaterThan(layout.target.y + layout.target.h);
      if (width >= 1200) {
        expect(Math.abs(layout.members.y-layout.current.y)).toBeLessThan(2);
        expect(Math.abs(layout.members.h-layout.current.h)).toBeLessThan(2);
        expect(layout.current.x).toBeGreaterThan(layout.members.x + layout.members.w);
        expect(layout.epoch.h).toBeLessThan(40);
      } else expect(layout.current.y).toBeGreaterThan(layout.members.y + layout.members.h);
      const numeric = details.locator('.pd-change-value').first();
      await expect(numeric).toHaveCSS('font-weight', '600');
      await expect(numeric).toHaveCSS('border-radius', '100px');
      if ([2048, 1440, 390].includes(width)) {
        await page.mouse.move(0, 0);
        await page.screenshot({path: `/tmp/taskite-pd-change-${width}-${theme}.png`, fullPage:width === 390, animations:'disabled'});
      }
      if (theme === 'dark') await page.getByRole('button', {name: '切换亮色主题'}).click();
    }
  }
  expect(errors).toEqual([]);
});
test('PD change polling updates actual membership and execution without replacing the original target', async ({page}) => {
  let completed = false;
  const operation = {type: 'MEMBER_REMOVE', memberId: 'meta-0', status: 'IN_PROGRESS'};
  await page.route('**/api/v1/pd/changes/pd-change', r => r.fulfill({json: {...pdChange,
    status: completed ? 'COMPLETED' : 'IN_PROGRESS', targetCount: 2, targetVoters: ['meta-1','meta-2'],
    actualVoters: completed ? ['meta-1','meta-2'] : ['meta-0','meta-1','meta-2'],
    currentOperation: completed ? undefined : operation,
    operations: [{...operation, status: completed ? 'COMPLETED' : 'IN_PROGRESS'}],
  }}));
  await go(page, '/changes/pd/pd-change');
  await expect(page.locator('[aria-label="目标成员配置"] .pd-change-member')).toHaveText(['meta-1','meta-2']);
  await expect(page.locator('[aria-label="实际成员状态"] .pd-change-member')).toHaveText(['meta-0','meta-1','meta-2']);
  completed = true;
  await expect(page.locator('.object-badges')).toContainText('已完成');
  await expect(page.locator('[aria-label="目标成员配置"] .pd-change-member')).toHaveText(['meta-1','meta-2']);
  await expect(page.locator('[aria-label="实际成员状态"] .pd-change-member')).toHaveText(['meta-1','meta-2']);
  await expect(page.locator('.pd-change-current')).toHaveCount(0);
  await expect(page.locator('.pd-change-operations')).toContainText('已完成');
});
test('PD change detail keeps long identifiers exact, errors readable and empty plans explicit', async ({page}) => {
  const memberId = 'taskite-placement-driver-production-member-with-a-very-long-identifier-01';
  const failure = 'CONFIGURATION_CHANGE_REJECTED_WITH_A_LONG_DETAIL_' + 'detail'.repeat(30);
  let empty = false;
  await page.route('**/api/v1/pd/changes/pd-change', r => r.fulfill({json: {...pdChange,
    sourceEpoch:'9007199254741046', targetEpoch:'9223372036854775807', status:'FAILED', error:failure,
    targetVoters:empty ? [] : [memberId], actualVoters:[], actualLearners:empty ? [] : [memberId],
    currentOperation:empty ? undefined : {type:'FUTURE_MEMBER_OPERATION',memberId,status:'FAILED',barrierIndex:'9007199254741046',error:failure},
    operations:empty ? [] : [{type:'FUTURE_MEMBER_OPERATION',memberId,status:'FAILED',barrierIndex:'9007199254741046',error:failure}],
  }}));
  await page.setViewportSize({width:390,height:1058});
  await go(page, '/changes/pd/pd-change');
  await expect(page.locator('.pd-change-information')).toContainText('9223372036854775807');
  await expect(page.locator('.pd-change-current')).toContainText('9007199254741046');
  await expect(page.locator('.pd-change-current')).toContainText('FUTURE_MEMBER_OPERATION');
  await expect(page.locator('.pd-change-current').getByRole('button',{name:`复制 ${memberId}`,exact:true})).toBeVisible();
  await expect(page.locator('.pd-change-operation-error')).toContainText(failure);
  await expect(page.locator('.pd-change-error-text')).toHaveText(failure);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({path:'/tmp/taskite-pd-change-failure-mobile.png',fullPage:true,animations:'disabled'});
  empty = true;
  await page.reload();
  await expect(page.getByText('暂无操作计划', {exact:true})).toBeVisible();
  await expect(page.locator('[aria-label="目标成员配置"]')).toContainText('无');
  await expect(page.locator('[aria-label="实际成员状态"] .pd-secondary-line')).toHaveText(['无','无']);
  await expect(page.locator('.pd-change-current')).toHaveCount(0);
});
test('PD change operation capsules share product colors across current operation and plan in both themes', async ({page}) => {
  const operations = [
    {type: 'MEMBER_ADD_LEARNER', memberId: 'meta-3', status: 'COMPLETED'},
    {type: 'MEMBER_PROMOTE_VOTER', memberId: 'meta-3', status: 'COMPLETED'},
    {type: 'MEMBER_REMOVE', memberId: 'meta-0', status: 'IN_PROGRESS'},
    {type: 'FUTURE_MEMBER_OPERATION', memberId: 'meta-4', status: 'PENDING'},
  ];
  await page.route('**/api/v1/pd/changes/pd-change', r => r.fulfill({json: {
    ...pdChange, status: 'IN_PROGRESS', currentOperation: operations[2], operations,
  }}));
  await page.setViewportSize({width: 1440, height: 1058});
  await go(page, '/changes/pd/pd-change');
  for (const theme of ['light', 'dark']) {
    if (theme === 'dark') await page.getByRole('button', {name: '切换暗色主题'}).click();
    const plan = page.locator('.pd-change-operations .pd-operation-type');
    await expect(plan).toHaveText(['添加 Learner', '提升为 Voter', '移除成员', 'FUTURE_MEMBER_OPERATION']);
    await expect(plan.nth(0)).toHaveClass(/ant-tag-blue/);
    await expect(plan.nth(1)).toHaveClass(/ant-tag-success/);
    await expect(plan.nth(2)).toHaveClass(/ant-tag-error/);
    await expect(plan.nth(3)).not.toHaveClass(/ant-tag-(blue|success|error)/);
    const colors = await plan.evaluateAll(elements => elements.map(element => {
      const css = getComputedStyle(element);
      return {color: css.color, background: css.backgroundColor, radius: css.borderRadius};
    }));
    expect(new Set(colors.map(color => color.background)).size).toBe(4);
    expect(colors.every(color => color.radius === '100px')).toBe(true);
    const current = page.locator('.pd-change-current .pd-operation-type');
    await expect(current).toHaveCSS('color', colors[2].color);
    await expect(current).toHaveCSS('background-color', colors[2].background);
    const systemRed = await page.locator('html').evaluate(element =>
      getComputedStyle(element).getPropertyValue('--destructive').trim());
    expect(await current.evaluate(element => getComputedStyle(element).color))
      .toBe(await current.evaluate((_, value) => {
        const canvas = document.createElement('canvas');
        const context = canvas.getContext('2d')!;
        context.fillStyle = `hsl(${value})`;
        const hex = context.fillStyle;
        return `rgb(${parseInt(hex.slice(1,3),16)}, ${parseInt(hex.slice(3,5),16)}, ${parseInt(hex.slice(5,7),16)})`;
      }, systemRed));
    await page.mouse.move(0, 0);
    await page.screenshot({path: `/tmp/taskite-pd-operation-colors-${theme}.png`, animations: 'disabled'});
  }
});
