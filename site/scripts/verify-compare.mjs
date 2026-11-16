import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';

const base = process.env.SITE_URL ?? 'http://127.0.0.1:8486';
const report = process.env.REPORT_DIR ?? 'shots/compare';
const results = JSON.parse(await readFile(process.env.COMPARE_PREVIEW_RESULTS ?? 'data/compare-results.json', 'utf8'));
await mkdir(report, { recursive: true });
const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
async function assertBarWidths(page) {
  await page.waitForFunction(() => Array.from(document.querySelectorAll('.rv .bar > i')).filter(bar => bar.getClientRects().length && bar.closest('.rv').getBoundingClientRect().top < innerHeight && bar.closest('.rv').getBoundingClientRect().bottom > 0).every(bar => {
    const expected = parseFloat(bar.style.getPropertyValue('--w')) / 100;
    const actual = bar.getBoundingClientRect().width / bar.parentElement.getBoundingClientRect().width;
    return Math.abs(actual - expected) <= .01;
  }));
  const errors = await page.locator('.rv .bar > i:visible').evaluateAll(bars => bars.filter(bar => {
    const bounds = bar.closest('.rv').getBoundingClientRect();
    if (bounds.top >= innerHeight || bounds.bottom <= 0) return false;
    return Math.abs(bar.getBoundingClientRect().width / bar.parentElement.getBoundingClientRect().width - parseFloat(bar.style.getPropertyValue('--w')) / 100) > .01;
  }).length);
  assert.equal(errors, 0, 'Each rendered bar must match value/axis within 1% after reveal');
}
let cases = 0;
try {
  for (const [width, motion] of [[1440, 'no-preference'], [390, 'no-preference'], [390, 'reduce']]) {
    const context = await browser.newContext({ viewport: { width, height: 960 }, reducedMotion: motion, permissions: ['clipboard-read', 'clipboard-write'] });
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(Number(process.env.SITE_BROWSER_TIMEOUT_MS ?? 30_000));
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
    const response = await page.goto(`${base}/compare`);
    assert.equal(response.status(), 200);
    await page.evaluate(() => document.fonts.ready);
    await page.waitForFunction(() => {
      const c = document.querySelector('#compare-map');
      return c.width > 100 && c.getContext('2d').getImageData(0, 0, c.width, c.height).data.some((value, i) => i % 4 !== 3 && value > 0);
    });
    await assertBarWidths(page);
    assert.equal(await page.locator('.draft-notice').count(), results.draft ? 1 : 0);
    assert.equal(await page.locator('.product-card').count(), new Set(results.systems.map(system => system.id)).size);
    const keys = new Set(results.systems.flatMap(system => Object.keys(system.metrics)));
    const covered = await page.locator('#results .metric-panel').evaluateAll(panels => [...new Set(panels.flatMap(panel => [panel.dataset.metric, panel.dataset.companion].filter(Boolean)))]);
    assert.deepEqual(covered.sort(), [...keys].sort());
    assert.equal(await page.locator('a[aria-current="page"]').textContent(), 'Compare');
    assert(await page.locator('nav[aria-label="Main"] a[href="/compare"]').isVisible());
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `Page overflow at ${width}`);
    const overflowing = await page.locator('.panel, .product-card').evaluateAll(elements => elements.filter(element => element.scrollWidth > element.clientWidth + 1).map(element => element.className));
    assert.deepEqual(overflowing, [], `Panel overflow at ${width}`);
    if (results.draft && !results.synthetic_design_preview) assert.equal(await page.locator('.bar i, .whisker').count(), 0, 'Placeholder must never draw fabricated bars');
    assert.equal(await page.locator('.synthetic-watermark').count(), results.synthetic_design_preview ? 1 : 0);
    assert.equal(await page.locator('.score-row:visible').first().locator('.score-cell:visible').count(), results.website.overview_metrics.length, 'Every overview column is selected initially');
    const suffix = `${width}${motion === 'reduce' ? '-reduced' : ''}`;
    if (width === 390) {
      const last = await page.locator('.score-row:visible').last().boundingBox();
      assert(last.y + last.height <= 960, 'All three headline rows fit in the first phone screen');
    }
    if (results.synthetic_design_preview) {
      assert.equal(await page.locator('.score-row[data-system="fyrr"] .score-cell[data-score-metric="recall_at_10"] .relative').textContent(), '-7.0 pp');
    }
    await page.screenshot({ path: `${report}/hero-${suffix}.png` });
    for (const panel of await page.locator('.story-section .metric-panel:visible').all()) {
      await panel.scrollIntoViewIfNeeded();
      await assertBarWidths(page);
    }
    await page.locator('#lighthouse').scrollIntoViewIfNeeded();
    await page.waitForFunction(() => {
      const canvas = document.getElementById('lighthouse');
      return canvas.getContext('2d').getImageData(0, 0, canvas.width, canvas.height).data.some((value, i) => i % 4 === 3 && value > 0);
    });
    await page.screenshot({ path: `${report}/footer-${suffix}.png` });
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: `${report}/full-${suffix}.png`, fullPage: true });
    await page.locator('.comparison-controls').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.screenshot({ path: `${report}/controls-${suffix}.png` });
    await page.locator('#story-speed').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.waitForTimeout(1500);
    await assertBarWidths(page);
    await page.screenshot({ path: `${report}/speed-${suffix}.png` });
    await page.locator('#story-footprint').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.waitForTimeout(1500);
    await page.screenshot({ path: `${report}/footprint-${suffix}.png` });
    await page.locator('#products').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.screenshot({ path: `${report}/products-${suffix}.png` });
    await page.locator('#details > summary').click();
    await page.locator('#detail').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.screenshot({ path: `${report}/details-${suffix}.png` });
    if (results.synthetic_design_preview) {
      assert.equal(await page.locator('#detail-category tr:visible').nth(1).locator('td.is-winner').count(), 2, 'Synthetic tie is explicit');
      assert.match(await page.locator('#detail-category tr:visible').nth(1).innerText(), /best · tied/);
    }
    for (const panel of await page.locator('#results .metric-panel:visible').all()) {
      await panel.scrollIntoViewIfNeeded();
      await assertBarWidths(page);
    }
    const c8 = page.getByRole('tab', { name: 'Concurrency 8' });
    await c8.click();
    assert.equal(await c8.getAttribute('aria-selected'), 'true');
    assert(await page.locator('#view-warm_c8_p50_ms').isVisible());
    const recall = page.locator('#detail-retrieval').getByRole('tab', { name: 'nDCG@10', exact: true });
    await recall.click();
    await recall.press('Home');
    assert.equal(await page.locator('#tab-recall_at_10').getAttribute('aria-selected'), 'true', 'Tab keyboard navigation');
    await page.locator('[data-select-system][value="fyrr"]').uncheck();
    assert.equal(await page.locator('.chart-row[data-system="fyrr"]:visible').count(), 0);
    assert(new URL(page.url()).searchParams.get('systems').includes('onyx'));
    await page.getByRole('button', { name: 'Copy selection link' }).click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), await page.locator('.permalink input').inputValue());
    await page.reload();
    assert.equal(await page.locator('[data-select-system][value="fyrr"]').isChecked(), false, 'Permalink restores systems');
    assert.equal(await page.getByRole('tab', { name: 'Concurrency 8' }).getAttribute('aria-selected'), 'true', 'Permalink restores concurrency');
    await page.locator('[data-select-system][value="fyrr"]').check();
    if (results.systems.some(system => system.variant === 'jev-rerank')) {
      const variant = page.locator('[data-select-system][value="fyrr@jev-rerank"]');
      assert.equal(await variant.isChecked(), false);
      await variant.check();
      assert(await page.locator('#detail-speed .chart-row[data-system="fyrr@jev-rerank"]:visible').count() > 0, 'Optional variant has a separate latency row');
      assert.equal(await page.locator('.hero-evidence [data-system="fyrr@jev-rerank"]').count(), 0, 'Headline only compares defaults');
      await variant.uncheck();
    }
    await page.locator('.metrics-select summary').click();
    await page.locator('[data-select-metric][value="answer_correctness"]').uncheck();
    assert.equal(await page.locator('#detail-quality [data-metric="answer_correctness"]').isVisible(), false);
    const details = page.locator('#detail-quality .metric-panel:not([hidden]) details').first();
    await details.locator('summary').click();
    assert(await details.getAttribute('open') !== null);
    await page.screenshot({ path: `${report}/uncertainty-${suffix}.png` });
    await page.getByRole('link', { name: 'Download results JSON' }).click();
    assert.equal(JSON.parse(await page.locator('body').innerText()).draft, results.draft);
    if (motion === 'reduce') {
      await page.goto(`${base}/compare`);
      await page.evaluate(() => document.fonts.ready);
      await page.waitForTimeout(1500);
      const before = await page.locator('#compare-map').evaluate(canvas => canvas.toDataURL());
      await page.waitForTimeout(300);
      assert.equal(await page.locator('#compare-map').evaluate(canvas => canvas.toDataURL()), before, 'Reduced motion map is static');
      await page.locator('#lighthouse').scrollIntoViewIfNeeded();
      await page.waitForTimeout(100);
      const lighthouse = await page.locator('#lighthouse').evaluate(canvas => canvas.toDataURL());
      await page.waitForTimeout(1600);
      assert.equal(await page.locator('#lighthouse').evaluate(canvas => canvas.toDataURL()), lighthouse, 'Reduced motion lighthouse is static');
      const sea = await page.locator('#sea').evaluate(canvas => canvas.toDataURL());
      await page.waitForTimeout(300);
      assert.equal(await page.locator('#sea').evaluate(canvas => canvas.toDataURL()), sea, 'Reduced motion footer ships are static');
    }
    await page.goto(`${base}/`);
    assert(await page.getByRole('link', { name: 'Compare', exact: true }).isVisible(), 'Homepage links to Compare on mobile too');
    assert.deepEqual(errors, [], `Browser errors at ${width}/${motion}`);
    await context.close();
    cases++;
    console.log(`PASS ${width}px / ${motion}: routing, data, no overflow, settled bar widths within 1%, tabs and keyboard, filters, clipboard permalink roundtrip, details, screenshots${motion === 'reduce' ? ', static map and lighthouse' : ''}`);
  }
} finally { await browser.close(); }
console.log(`Comparison browser cases: ${cases} passed, 0 failed, 0 ignored. Screenshots: ${report}`);
