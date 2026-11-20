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
import {test, expect} from '@playwright/test';
const createdClusters: string[] = [];
let createdMirrorKey: string | undefined;
test.afterEach(async ({request}) => {
  if (createdMirrorKey) expect([204, 404]).toContain((await request.delete(`/api/v1/broker/job-subscription-mirrors/${createdMirrorKey}`)).status());
  for (const id of createdClusters) expect([204, 404]).toContain((await request.delete(`/api/v1/broker/clusters/${id}`)).status());
});

test('isolated PD supports migrated UI cluster and Mirror lifecycle', async ({page, request}) => {
  test.setTimeout(90000);
  const bootstrap = await (await request.get('/api/v1/console/bootstrap')).json();
  expect(bootstrap.deploymentId).toMatch(/^console-smoke-/);
  expect(bootstrap).not.toHaveProperty('grafana');
  expect(bootstrap).not.toHaveProperty('uiReadOnly');
  const source = `console-smoke-${Date.now()}-source`, target = source.replace('-source', '-target');
  createdClusters.push(source, target);
  const errors: string[] = [];
  page.on('pageerror', e => errors.push(e.message));
  let mirrorKey: string | undefined;
  try {
    for (const clusterId of [source, target]) {
      await page.goto('/console/#/clusters');
      await page.getByRole('button', {name: '创建集群', exact: true}).first().click();
      await page.getByLabel('Cluster ID', {exact: true}).fill(clusterId);
      for (const label of ['目标 Broker 数', '分区数', '副本因子']) await page.getByLabel(label, {exact: true}).fill('1');
      await page.getByRole('button', {name: '创建', exact: true}).click();
      await expect(page).toHaveURL(new RegExp(`#/clusters/${clusterId}$`));
      await expect(page.getByRole('heading', {name: clusterId, exact: true})).toBeVisible();
      await expect(page.getByText('等待 Broker 注册', {exact: true})).toBeVisible();
      await page.reload();
      await expect(page.getByRole('heading', {name: clusterId, exact: true})).toBeVisible();
    }
    await page.goto('/console/#/');
    await expect(page.getByRole('heading', {name: '集群总览'})).toBeVisible();
    await page.screenshot({path: 'target/live-console-overview.png', fullPage: true, animations: 'disabled'});
    await page.goto('/console/#/subscriptions/mirrors');
    await page.getByRole('button', {name: '创建 Mirror', exact: true}).first().click();
    for (const [label, value] of [['源集群 ID', source], ['目标集群 ID', target]]) {
      const select = page.getByRole('combobox', {name: label, exact: true});
      await select.click();
      await select.fill(value);
      await page.locator('.ant-select-dropdown:visible .ant-select-item-option-content').getByText(value, {exact: true}).click();
    }
    await page.getByRole('button', {name: '创建', exact: true}).click();
    await expect(page).toHaveURL(/#\/subscriptions\/mirrors\/\d+$/);
    mirrorKey = new URL(page.url()).hash.split('/').at(-1)!;
    createdMirrorKey = mirrorKey;
    await expect(page.locator('.copyable-id-value', {hasText: mirrorKey}).first()).toBeVisible();
    await expect(page.locator('.object-badges .ant-tag')).toHaveClass(/ant-tag-success/);
    await expect(page.locator('.mirror-endpoint')).toHaveCount(2);
    await page.screenshot({path: 'target/live-mirror-detail-light.png', fullPage: true, animations: 'disabled'});
    await page.getByRole('button', {name: '切换暗色主题'}).click();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');
    await page.screenshot({path: 'target/live-mirror-detail-dark.png', fullPage: true, animations: 'disabled'});
    await page.reload();
    await expect(page.locator('.copyable-id-value', {hasText: mirrorKey}).first()).toBeVisible();
    await page.goto('/console/#/subscriptions/mirrors');
    await page.locator('.ant-table-row', {hasText: mirrorKey}).getByRole('button', {name: '删除'}).click();
    await page.getByRole('button', {name: '确认删除'}).click();
    await expect(page.getByText('暂无 Mirror 关系')).toBeVisible();
    for (const clusterId of [source, target]) {
      await page.goto('/console/#/clusters');
      const row = page.locator('.ant-table-row').filter({has: page.getByRole('button', {name: `复制 ${clusterId}`, exact: true})});
      await row.getByRole('button', {name: '删除'}).click();
      await page.getByPlaceholder(`输入 ${clusterId}`).fill(clusterId);
      await page.getByRole('button', {name: '确认删除'}).click();
      await expect(row).toHaveCount(0);
    }
    await page.goto('/console/#/pd');
    await expect(page.getByRole('heading', {name: 'PD 管理'})).toBeVisible();
    await expect(page.getByText('暂无当前变更')).toBeVisible();
    await page.goto('/console/#/workers');
    await expect(page.getByText('暂无 Worker 注册记录')).toBeVisible();
    await expect(page.locator('.filter-toolbar')).toHaveCount(0);
    await page.getByRole('button', {name: '按租户筛选', exact: true}).click();
    await page.getByRole('textbox', {name: '按租户筛选'}).fill('console-smoke-tenant');
    await page.getByRole('button', {name: '筛选', exact: true}).click();
    await expect(page).toHaveURL(/tenant=console-smoke-tenant$/);
    await expect(page.getByText('暂无符合筛选条件的 Worker，可在表头重置筛选')).toBeVisible();
    expect(errors).toEqual([]);
  } finally { /* afterEach cleans records with its own timeout even when a UI step fails. */ }
});
