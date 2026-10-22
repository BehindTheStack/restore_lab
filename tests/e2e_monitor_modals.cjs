// Read-only browser smoke test. Requires Playwright and an initialized monitor.
'use strict';
const assert = require('node:assert/strict');
const {chromium} = require('playwright');

async function main() {
  if (process.env.LAB_MONITOR_BROWSER_TEST !== '1') throw new Error('Set LAB_MONITOR_BROWSER_TEST=1; this only reads monitor evidence.');
  const browser = await chromium.launch({headless: true,
    ...(process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE ? {executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE} : {})});
  try {
    const page = await browser.newPage({viewport: {width: 1365, height: 900}});
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    const base = process.env.LAB_MONITOR_URL || 'http://127.0.0.1:8082';
    const response = await page.goto(base + '/?runs_page=2&tables_page=1');
    assert.equal(response.status(), 200);
    assert.match(response.headers()['content-security-policy'], /script-src 'self'/);
    const originalURL = page.url();
    const table = page.locator('#tables a[data-modal]').first();
    await table.waitFor();
    const tableHref = await table.getAttribute('href');
    await table.click();
    const modal = page.locator('#details-modal');
    await page.locator('#modal-content .ddl-code').first().waitFor();
    assert.equal(await modal.evaluate(node => node.open), true);
    assert.match(await page.locator('#modal-content').innerText(), /CREATE TABLE/);
    await page.screenshot({path: '/tmp/kafka-monitor-ddl-modal.png'});
    for (let i = 0; i < 5; i++) {
      await page.keyboard.press('Tab');
      assert.equal(await page.evaluate(() => !!document.activeElement.closest('#details-modal')), true);
    }
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !document.getElementById('details-modal').open);
    assert.equal(await page.evaluate(() => document.activeElement.getAttribute('href')), tableHref);

    const run = page.locator('#runs a[data-modal][href^="/run/"]').first();
    const runHref = await run.getAttribute('href');
    await run.click();
    await page.locator('#modal-content .detail-table').waitFor();
    assert.match(await page.locator('#modal-title').innerText(), /Execução/);
    await page.locator('#modal-content a[data-modal][href^="/ddl"]').first().click();
    await page.locator('#modal-content .ddl-code').first().waitFor();
    assert.equal(await page.locator('#modal-back').isVisible(), true);
    await page.locator('#modal-back').click();
    await page.locator('#modal-content .detail-table').waitFor();
    assert.match(await page.locator('#modal-title').innerText(), /Execução/);
    let navigations = 0;
    page.on('framenavigated', () => navigations++);
    await page.waitForTimeout(21000);
    assert.equal(navigations, 0, 'Auto refresh must pause while a modal is open');
    assert.equal(await modal.evaluate(node => node.open), true);
    assert.equal(page.url(), originalURL);
    await page.locator('#modal-close').click();
    assert.equal(await page.evaluate(() => document.activeElement.getAttribute('href')), runHref);

    await page.setViewportSize({width: 390, height: 844});
    await table.click();
    await page.locator('#modal-content .ddl-code').first().waitFor();
    const box = await modal.boundingBox();
    assert.ok(box.x >= 0 && box.x + box.width <= 390, 'Dialog must fit a mobile viewport');
    await page.mouse.click(2, 2);
    await page.waitForFunction(() => !document.getElementById('details-modal').open);

    await page.route(/\/ddl\?.*format=json/, route => route.fulfill({status: 503, contentType: 'application/json', body: '{}'}));
    await table.click();
    await page.locator('#modal-content [role="alert"]').waitFor();
    assert.equal(await modal.evaluate(node => node.open), true);
    await page.keyboard.press('Escape');
    assert.equal(page.url(), originalURL);
    assert.deepEqual(errors, []);
    console.log('PASS: DDL/run modals, historical drill-down/back, focus trap/restore, Escape, backdrop, paused refresh, pagination, mobile and errors');
  } finally {
    await browser.close();
  }
}
main().catch(error => {console.error(error);process.exitCode = 1;});
