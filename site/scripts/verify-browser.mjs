import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { navigation } from '../lib/navigation.ts';

const base = process.env.SITE_URL ?? 'http://127.0.0.1:8486';
const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 900 }, permissions: ['clipboard-read', 'clipboard-write'] });
    const page = await context.newPage();
    page.setDefaultTimeout(15_000);
    page.setDefaultNavigationTimeout(30_000);
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
    for (const [, entries] of navigation) for (const [file] of entries) {
      const url = file.replace(/README\.md$/, 'index.md').replace(/\.md$/, '').replace(/\/index$/, '');
      const response = await page.goto(`${base}/docs/${url}`);
      assert.equal(response.status(), 200, url);
      await page.locator('h1').waitFor();
      await page.waitForTimeout(100);
      assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `Overflow: ${url} at ${width}`);
    }
    await page.goto(`${base}/docs/operations/`);
    await page.waitForURL('**/docs/operations');
    await page.goto(`${base}/docs/connectors/index.html`);
    await page.waitForURL('**/docs/connectors');
    await page.goto(`${base}/docs/quickstart.html`);
    await page.waitForURL('**/docs/quickstart');
    await page.locator('.header, .band__title').evaluateAll(elements => Promise.all(elements.flatMap(element => element.getAnimations().map(animation => animation.finished))));
    await page.screenshot({ path: `${process.env.REPORT_DIR ?? '../target'}/fumadocs-quickstart-${width}.png` });
    if (width >= 992) {
      const firstHeading = page.locator('.toc a').first();
      const headingUrl = new URL(await firstHeading.getAttribute('href'), page.url());
      const headingId = decodeURIComponent(headingUrl.hash.slice(1));
      assert(headingId, 'The table of contents links to a heading');
      await firstHeading.click();
      await page.waitForURL(headingUrl.href);
      await page.waitForFunction(id => {
        const top = document.getElementById(id).getBoundingClientRect().top;
        return top >= 48 && top < 160;
      }, headingId);
    }
    if (width < 992) {
      const toggle = page.locator('button[aria-controls="docs-nav"]');
      assert.equal(await toggle.getAttribute('aria-expanded'), 'false');
      await toggle.click();
      assert.equal(await toggle.getAttribute('aria-expanded'), 'true');
    }
    await page.getByRole('button', { name: /^Search docs/ }).click();
    // An unauthored character must use the original full variable font.
    const fullFont = page.waitForResponse(response => response.url().endsWith('/geist-sans-full.woff2'));
    await page.getByRole('combobox').fill('café');
    assert.equal((await fullFont).status(), 200);
    await page.evaluate(async () => { await document.fonts.ready; });
    await page.getByRole('combobox').fill('permissions');
    const result = page.getByRole('dialog').getByRole('option').filter({ hasText: /GitHub/ }).first();
    await result.waitFor();
    await result.click();
    await page.waitForURL('**/docs/connectors/github*');
    await page.goto(`${base}/docs/configuration`);
    const block = page.locator('figure.shiki').first();
    const expectedCode = await block.locator('pre code').textContent();
    await page.evaluate(() => navigator.clipboard.writeText(''));
    await block.getByRole('button', { name: /copy/i }).click();
    const copiedCode = await page.evaluate(() => navigator.clipboard.readText());
    assert.equal(copiedCode.trimEnd(), expectedCode.trimEnd(), 'Copy keeps the exact code text');
    await page.screenshot({ path: `${process.env.REPORT_DIR ?? '../target'}/fumadocs-${width}.png` });
    assert.deepEqual(errors, [], `Browser errors at ${width}`);
    await context.close();
    console.log(`${width}px: 20 docs pages, legacy URL, Unicode font loading, search, result navigation, ${width >= 992 ? 'desktop table of contents, ' : ''}exact code copying passed`);
  }
} finally { await browser.close(); }
