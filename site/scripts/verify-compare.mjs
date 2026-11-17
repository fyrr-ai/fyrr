import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { mkdir, readFile } from 'node:fs/promises';

const base = process.env.SITE_URL ?? 'http://127.0.0.1:8486';
const report = process.env.REPORT_DIR ?? 'shots/compare';
const elementShotStyle = '.header,.skip{visibility:hidden!important}';
const results = JSON.parse(await readFile(process.env.COMPARE_PREVIEW_RESULTS ?? 'data/compare-results.json', 'utf8'));
await mkdir(report, { recursive: true });
let browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
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
  const stackErrors = await page.locator('.bar > i:has(.ingest-segment):visible').evaluateAll(bars => bars.filter(bar => {
    const bounds = bar.getBoundingClientRect();
    if (!bounds.width || bounds.top >= innerHeight || bounds.bottom <= 0) return false;
    const width = Array.from(bar.children).reduce((sum, stage) => sum + stage.getBoundingClientRect().width, 0);
    return Math.abs(width / bounds.width - 1) > .01;
  }).length);
  assert.equal(stackErrors, 0, 'Ingest stages must fill their measured total within 1%');
  const crowdedAxes = await page.locator('.metric-panel--compact:visible .axis').evaluateAll(axes => axes.filter(axis => {
    const labels = Array.from(axis.children).filter(label => getComputedStyle(label).visibility !== 'hidden').map(label => label.getBoundingClientRect());
    return labels.some((label, index) => index > 0 && label.left < labels[index - 1].right + 1);
  }).length);
  assert.equal(crowdedAxes, 0, 'Visible compact axis labels must not overlap');
}
async function assertNavigation(page) {
  const layout = await page.locator('.header__row').evaluate(row => {
    const logo = row.querySelector('.logo').getBoundingClientRect();
    const links = Array.from(row.querySelectorAll('.nav__item--keep,.nav__gh')).filter(link => link.getClientRects().length).map(link => ({ text: link.textContent.trim(), left: link.getBoundingClientRect().left, right: link.getBoundingClientRect().right, height: link.getBoundingClientRect().height }));
    return { width: innerWidth, logoRight: logo.right, links };
  });
  assert(layout.links.some(link => link.text === 'Compare'));
  assert(layout.links.some(link => link.text === 'Use cases'));
  assert(layout.links.every(link => link.left >= layout.logoRight + 4 && link.right <= layout.width && (link.text === 'GitHub' || link.height <= 24)), 'Shared navigation stays on one line and clear of the logo');
}
let cases = 0;
async function assertDiagnosticsUnranked(page) {
  const errors = await page.locator('.relative').evaluateAll((labels, metrics) => labels.filter(label => {
    const parent = label.closest('[data-metric], [data-score-metric], [data-detail-metric]');
    const key = parent?.dataset.metric ?? parent?.dataset.scoreMetric ?? parent?.dataset.detailMetric;
    const meta = metrics[key];
    return meta && (meta.comparable === false || meta.scope?.endsWith('-only')) && (label.textContent !== 'diagnostic · unranked' || label.classList.contains('is-best') || label.closest('.is-winner'));
  }).map(label => label.textContent), results.website.metrics);
  assert.deepEqual(errors, [], 'Diagnostics must never gain a ratio or winner label, including after filtering');
}
try {
  const staticContext = await browser.newContext({ viewport: { width: 390, height: 960 }, javaScriptEnabled: false });
  const staticPage = await staticContext.newPage();
  const staticRequests = [];
  staticPage.on('request', request => { if (/notebook-/.test(request.url())) staticRequests.push(request.url()); });
  await staticPage.goto(`${base}/compare`);
  assert.equal(await staticPage.locator('.score-row').count(), new Set(results.systems.map(system => system.id)).size);
  assert(await staticPage.locator('#story-speed-c1 .primary-value').first().isVisible());
  assert.equal(await staticPage.locator('[data-detail-metric]').count(), 0);
  assert.equal(staticRequests.length, 0);
  await staticPage.screenshot({ path: `${report}/static-390.png` });
  await staticContext.close();
  cases++;
  console.log('PASS static 390px: overview and story charts complete without JavaScript; no notebook transfer');
  for (const [width, motion] of [[1440, 'no-preference'], [390, 'no-preference'], [390, 'reduce']]) {
    // Release native renderer and screenshot resources between independent cases.
    await browser.close();
    browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
    const context = await browser.newContext({ viewport: { width, height: 960 }, reducedMotion: motion, permissions: ['clipboard-read', 'clipboard-write'] });
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(Number(process.env.SITE_BROWSER_TIMEOUT_MS ?? 30_000));
    const errors = [];
    const notebookRequests = [];
    const notebookResponses = [];
    page.on('request', request => { if (/notebook-.*\.json(?:\.gz)?$/.test(new URL(request.url()).pathname)) notebookRequests.push(request.url()); });
    page.on('response', response => { if (/notebook-.*\.json$/.test(new URL(response.url()).pathname)) notebookResponses.push(response); });
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
    await assertDiagnosticsUnranked(page);
    assert.equal(await page.locator('.draft-notice').count(), results.draft ? 1 : 0);
    if (!results.draft) {
      assert.equal(await page.getByText(/Preliminary|owner review pending/i).count(), 0, 'Published page has no review banner or preliminary captions');
      assert.equal(await page.locator('meta[name="robots"][content*="noindex"]').count(), 0, 'Approved publication is indexable');
    }
    assert(await page.getByRole('link', { name: 'Use cases', exact: true }).isVisible());
    await assertNavigation(page);
    for (const [id, count] of [['fyrr', 1], ['onyx', 12], ['pipeshub@parallel-partitions', 15]]) {
      const row = page.locator(`.score-row[data-system="${id}"]`);
      if (await row.count()) assert.equal(await row.locator('[data-score-metric="service_count"]').getAttribute('data-value'), String(count));
    }
    assert.match(await page.locator('.score-row[data-system="fyrr"] [data-score-metric="service_count"] .score-secondary').textContent(), /reader; writer = scheduled job; plus S3/);

    assert.equal(await page.locator('.product-card').count(), new Set(results.systems.map(system => system.id)).size);
    assert.equal(await page.locator('#detail-quality').count(), 0, 'Answer scoring is excluded');
    assert(!Object.keys(results.website.metrics).some(key => /^(?:answer_|native_answer)/.test(key)));
    assert.deepEqual(await page.locator('.score-head [data-score-metric]').evaluateAll(cells => cells.map(cell => cell.dataset.scoreMetric)), ['monthly_cost_usd', 'service_count', 'idle_rss_bytes', 'first_ask_c1_p50_ms', 'ingest_wall_s', 'recall_at_10'], 'Overview leads with cost');
    assert.deepEqual(await page.locator('.story-section').evaluateAll(sections => sections.map(section => section.id)), ['story-footprint', 'story-speed', 'story-ingest', 'story-quality'], 'Stories lead with cost');
    assert(await page.locator('#story-footprint').evaluate(element => element.classList.contains('story-section--lead')));
    if (results.website.lambda_projection) {
      assert.equal(await page.locator('#lambda-projection').count(), 1);
      assert.equal(await page.locator('#lambda-projection .metric-panel').count(), 0, 'Lambda projection never becomes a benchmark chart');
      assert.match(await page.locator('#lambda-projection').textContent(), /35\.756 s/);
      assert.match(await page.locator('#lambda-projection').textContent(), /300 synthetic documents/);
      assert.equal(await page.locator('#lambda-projection a[href="/docs/lambda"]').count(), 1);
    }
    const keys = new Set(results.systems.flatMap(system => Object.keys(system.metrics)));
    assert.equal(await page.locator('#results').count(), 0, 'Notebook charts are absent from the initial HTML');
    assert.equal(await page.locator('[data-detail-metric]').count(), 0, 'No per-question rows in the first response');
    assert.equal(notebookRequests.length, 0, 'Opening the page and story views must not fetch the notebook');
    assert.equal(await page.locator('details[open]').count(), 0, 'Every supporting disclosure starts closed');
    assert.equal(await page.locator('.story-section .metric-panel:visible').count(), 5, 'Only four compact story views are exposed');
    assert.equal(await page.locator('.score-run-note:visible, .score-coverage:visible, .cost-card:visible, .feature-table:visible').count(), 0);

    assert(await page.locator('#story-speed-c1 .metric-panel').first().isVisible(), 'Static story charts remain complete');
    assert.equal(await page.locator('a[aria-current="page"]').textContent(), 'Compare');
    assert(await page.locator('nav[aria-label="Main"] a[href="/compare"]').isVisible());
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `Page overflow at ${width}`);
    const overflowing = await page.locator('.panel, .product-card').evaluateAll(elements => elements.filter(element => element.scrollWidth > element.clientWidth + 1).map(element => element.className));
    assert.deepEqual(overflowing, [], `Panel overflow at ${width}`);
    if (results.draft && !results.synthetic_design_preview && !results.systems.some(system => Object.values(system.metrics).some(metric => metric.status === 'measured'))) assert.equal(await page.locator('.bar i, .whisker').count(), 0, 'Placeholder must never draw fabricated bars');
    assert.equal(await page.locator('.synthetic-watermark').count(), results.synthetic_design_preview ? 1 : 0);
    assert.equal(await page.locator('.score-row:visible').first().locator('.score-cell:visible').count(), results.website.overview_metrics.length, 'Every overview column is selected initially');
    const headlineKeys = await page.locator('.score-row').evaluateAll(rows => rows.map(row => row.dataset.system));
    const headlineRuns = headlineKeys.map(key => results.systems.find(system => (!system.variant || system.variant === 'default' ? system.id : system.id + '@' + system.variant) === key));
    assert(headlineRuns.every(Boolean));
    assert.equal(new Set(headlineRuns.map(system => system.id)).size, headlineRuns.length, 'Exactly one headline representative per product');
    assert.equal(headlineRuns.length, new Set(results.systems.map(system => system.id)).size);
    assert.equal(await page.locator('.score-row:visible .score-cell[data-score-metric="ingest_wall_s"] .score-secondary').count(), headlineRuns.length, 'Every representative row exposes ingest throughput');
    const memoryRuns = headlineRuns.filter(system => system.metrics.idle_rss_bytes?.status === 'measured');
    if (memoryRuns.length > 1 && memoryRuns.every(system => { const metric = system.metrics.idle_rss_bytes; return metric.n === metric.expected && metric.n === metric.attempted && metric.missing === 0; })) assert.equal(await page.locator('.score-row:visible .score-cell[data-score-metric="idle_rss_bytes"] .is-best').count(), 1, 'Independent memory sampling windows can differ in count without becoming incomplete');
    assert.equal(await page.locator('#story-ingest .metric-panel').count(), 1, 'Ingest story has one wall-time chart');
    assert.equal(await page.locator('#ingest-notes .ingest-cost').count(), headlineRuns.length, 'Embedding cost is a secondary line for each representative product');
    assert.equal(await page.locator('#cost-production > .cost-assumptions-grid > .cost-card').count(), headlineRuns.length, 'Production cost assumptions accompany every representative');
    assert.equal(await page.locator('#cost-light .cost-card').count(), headlineRuns.length);
    assert.equal(await page.locator('#cost-team .cost-card').count(), headlineRuns.length);
    assert.equal(await page.locator('#minimum-cost-details').getAttribute('open'), null);
    assert.equal(await page.locator('#cost-light').isVisible(), false);
    assert.equal(await page.locator('#cost-team').isVisible(), false);
    assert.equal(await page.locator('.score-row:visible [data-score-metric="first_ask_c1_p50_ms"] .score-secondary').count(), headlineRuns.length, 'Repeated question is visible beneath each overview query speed');
    assert.equal(await page.locator('#story-speed-c1 .metric-panel:visible').count(), 2, 'New and repeated questions are paired in the first story state');
    assert.equal(await page.locator('#speed-notes-c8').isVisible(), false, 'Concurrency 1 is the static first view');
    assert.equal(await page.locator('#search-notes .cache-summary [data-system]').count(), headlineRuns.length, 'Cache provenance accompanies every representative');
    for (const system of headlineRuns) {
      const key = !system.variant || system.variant === 'default' ? system.id : system.id + '@' + system.variant;
      if (system.variant && system.variant !== 'default') {
        assert(await page.locator(`.score-note a[href="#${system.id === 'pipeshub' ? 'pipes-notes' : 'run-notes-' + system.id}"]`).count() > 0, 'Non-default headline configuration has a one-click marker');
        assert.equal(await page.locator(`[data-select-system][value="${key}"]`).isChecked(), true);
        assert.equal(await page.locator(`.score-row[data-system="${key}"] .default-ingest-baseline`).count(), 0, 'Other configuration baselines belong in the diagnostic table');
        const segments = (system.ingest_diagnostics ?? []).filter(row => row.configuration_id.startsWith('full-final-segment') && Number.isFinite(row.docs_per_min));
        if (system.metrics.ingest_wall_s?.value == null && segments.length) {
          const cell = page.locator(`.score-row[data-system="${key}"] [data-score-metric="ingest_wall_s"]`);
          const range = [...new Set([Math.round(Math.min(...segments.map(row => row.docs_per_min))), Math.round(Math.max(...segments.map(row => row.docs_per_min)))])].map(value => value.toLocaleString('en')).join('–');
          assert.equal((await cell.locator('.score-value').innerText()).replace('†', ''), `~${range} docs/min`);
          assert.match(await cell.locator('.score-secondary').innerText(), /Segments; full time n\/e/);
          assert.equal(await cell.locator('.bar i').count(), 0, 'Interrupted observation rates cannot become completed-ingest bars');
        }
      }
      if (system.background_activity?.enrichment_active) {
        assert.match(await page.locator(`#${system.id === 'pipeshub' ? 'pipes-notes' : 'run-notes-' + system.id} .background-activity`).textContent(), /Background document enrichment active.*Enrichment completed:/);
      }
      const coverage = system.ingest_coverage;
      if (!coverage || !['submitted', 'accepted', 'rejected'].every(key => Number.isInteger(coverage[key]))) continue;
      const partial = Number.isInteger(coverage.searchable) && Number.isInteger(coverage.pending);
      const label = `${coverage.accepted.toLocaleString('en')} / ${coverage.submitted.toLocaleString('en')} accepted · ${coverage.rejected.toLocaleString('en')} rejected${partial ? ` · ${coverage.searchable.toLocaleString('en')} / ${coverage.submitted.toLocaleString('en')} searchable · ${coverage.pending.toLocaleString('en')} pending` : ''}`;
      assert((await page.locator(`#${system.id === 'pipeshub' ? 'pipes-notes' : 'run-notes-' + system.id}`).textContent()).includes(label));
      assert.equal(await page.locator(`.score-row[data-system="${key}"] .score-coverage`).count(), 0, 'Overview counts are behind the footnote');

    }
    const suffix = `${width}${motion === 'reduce' ? '-reduced' : ''}`;
    if (width === 390 && headlineRuns.every(system => !system.variant || system.variant === 'default')) {
      const last = await page.locator('.score-row:visible').last().boundingBox();
      assert(last.y + last.height <= 960, 'All three headline rows fit in the first phone screen');
    }
    if (results.synthetic_design_preview) {
      assert.equal(await page.locator('.score-row[data-system="fyrr"] .score-cell[data-score-metric="recall_at_10"] .relative').textContent(), '-7.0 pp');
    }
    await page.screenshot({ path: `${report}/hero-${suffix}.png` });
    await page.locator('.scoreboard').screenshot({ path: `${report}/overview-${suffix}.png`, style: elementShotStyle });
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
    await page.screenshot({ path: `${report}/full-${suffix}.png`, fullPage: true, style: '.skip{visibility:hidden!important}' });
    await page.evaluate(() => { location.hash = 'minimum-cost-details'; });
    await page.waitForFunction(() => document.getElementById('cost-notes').open && document.getElementById('minimum-cost-details').open);
    await page.reload();
    await page.waitForFunction(() => document.getElementById('cost-notes').open && document.getElementById('minimum-cost-details').open);
    assert(new URL(page.url()).searchParams.get('open').includes('minimum-cost-details'), 'Nested disclosure state survives its permalink');
    await page.locator('#cost-notes > summary').click();
    await page.waitForFunction(() => !document.getElementById('minimum-cost-details').open && location.hash === '');
    if (await page.locator('#pipes-notes').count()) {
      await page.locator('.score-note a[href="#pipes-notes"]').click();
      await page.waitForFunction(() => document.getElementById('pipes-notes').open);
      assert(new URL(page.url()).searchParams.get('open').includes('pipes-notes'));
      await page.reload();
      await page.waitForFunction(() => document.getElementById('pipes-notes').open);
      assert(await page.locator('#pipes-notes .partial-coverage').isVisible(), 'Footnote deep link restores the matching caveat');
      await page.locator('#pipes-notes > summary').click();
    }
    await page.locator('#comparison-options > summary').click();
    await page.locator('.comparison-controls').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.screenshot({ path: `${report}/controls-${suffix}.png` });
    await page.locator('#story-speed').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.waitForTimeout(1500);
    await assertBarWidths(page);
    await page.screenshot({ path: `${report}/speed-${suffix}.png` });
    for (const panel of await page.locator('#story-speed-c1 .metric-panel').all()) {
      await panel.scrollIntoViewIfNeeded();
      await assertBarWidths(page);
    }
    await page.locator('#story-speed-c1').screenshot({ path: `${report}/speed-pair-${suffix}.png`, style: elementShotStyle });
    const queryBefore = await page.locator('.score-row .score-cell[data-score-metric="first_ask_c1_p50_ms"] .score-value').allTextContents();
    if (!(await page.locator('#search-notes').evaluate(element => element.open))) await page.locator('#search-notes > summary').click();
    await page.locator('#speed-notes-tab-c8').click();
    assert.equal(await page.locator('#speed-notes-c8 .metric-panel:visible').count(), 2, 'One concurrency toggle switches both timing measures');
    assert(await page.locator('#speed-notes-c8 [data-metric="first_ask_c8_p50_ms"]').isVisible());
    assert(await page.locator('#speed-notes-c8 [data-metric="warm_c8_p50_ms"]').isVisible());
    assert.deepEqual(await page.locator('.score-row .score-cell[data-score-metric="first_ask_c1_p50_ms"] .score-value').allTextContents(), queryBefore, 'Story repeat/C8 selection cannot replace headline first-ask speed');
    await page.locator('#speed-notes-tab-c1').click();
    await page.locator('#story-ingest').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await assertBarWidths(page);
    await page.screenshot({ path: `${report}/ingest-${suffix}.png` });
    await page.locator('#ingest-notes > summary').click();
    for (const diagnostic of await page.locator('.ingest-diagnostics').all()) await diagnostic.screenshot({ path: `${report}/ingest-diagnostics-${suffix}.png`, style: elementShotStyle });
    await page.locator('#ingest-notes > summary').click();
    await page.locator('#cost-notes > summary').click();
    await page.locator('#story-footprint').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.waitForTimeout(1500);
    await page.screenshot({ path: `${report}/footprint-${suffix}.png` });
    await page.locator('#cost-production').screenshot({ path: `${report}/production-cost-${suffix}.png`, style: elementShotStyle });
    const overviewBeforeTeam = await page.locator('.score-row').allTextContents();
    await page.locator('#minimum-cost-details > summary').click();
    await page.locator('#cost-tab-team').click();
    assert(await page.locator('#cost-team').isVisible());
    assert.equal(await page.locator('#cost-light').isVisible(), false);
    assert.deepEqual(await page.locator('.score-row').allTextContents(), overviewBeforeTeam, 'Team cost view cannot replace headline values');
    await page.locator('#cost-tab-light').click();
    await page.locator('#minimum-cost-details').screenshot({ path: `${report}/minimum-cost-${suffix}.png`, style: elementShotStyle });
    await page.locator('#minimum-cost-details > summary').click();
    if (results.website.lambda_projection) {
      await page.locator('#lambda-notes > summary').click();
      await page.locator('#lambda-projection').screenshot({ path: `${report}/lambda-${suffix}.png`, style: elementShotStyle });
      await page.locator('#lambda-notes > summary').click();
    }
    await page.locator('#cost-notes > summary').click();
    await page.locator('#products').evaluate(element => window.scrollTo(0, element.offsetTop - 64));
    await page.screenshot({ path: `${report}/products-${suffix}.png` });
    assert.equal(notebookRequests.length, 0, 'Story interactions do not load per-question data');
    if (width === 1440) await page.route('**/notebook-*.json', route => route.fulfill({ status: 200, contentType: 'application/json', body: '{"version":0}' }));
    await page.locator('#details > summary').click();
    if (width === 1440) {
      await page.locator('#details[data-load-state="failed"]').waitFor();
      assert(await page.locator('#notebook-retry').isVisible());
      assert.equal(await page.locator('#results').count(), 0, 'An invalid payload must not masquerade as missing scores');
      await page.unroute('**/notebook-*.json');
      await page.locator('#notebook-retry').click();
    }
    await page.locator('#details[data-load-state="ready"]').waitFor();
    assert.equal(notebookRequests.length, width === 1440 ? 2 : 1, 'One successful notebook request, with explicit retry after failure');
    const transport = await notebookResponses.at(-1).allHeaders();
    assert(['br', 'gzip'].includes(transport['content-encoding']), 'Notebook JSON must be compressed by the real static-asset runtime');
    for (const [id, count] of [['fyrr', 1], ['onyx', 12], ['pipeshub@parallel-partitions', 15]]) {
      const row = page.locator(`#detail-footprint [data-metric="service_count"] .chart-row[data-system="${id}"]`);
      if (await row.count()) assert.equal(await row.getAttribute('data-value'), String(count), 'Services chart matches the overview definition');
    }
    const covered = await page.locator('#results .metric-panel').evaluateAll(panels => [...new Set(panels.flatMap(panel => [panel.dataset.metric, panel.dataset.companion].filter(Boolean)))]);
    assert.deepEqual(covered.sort(), [...keys].sort());
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
    const loadedCount = notebookRequests.length;
    await page.locator('#details > summary').click();
    await page.locator('#details > summary').click();
    assert.equal(notebookRequests.length, loadedCount, 'Reopening the notebook reuses the loaded data');
    const c8 = page.locator('#tab-first_ask_c8_p50_ms');
    await c8.click();
    assert.equal(await c8.getAttribute('aria-selected'), 'true');
    assert(await page.locator('#view-first_ask_c8_p50_ms').isVisible());
    const repeatC8 = page.locator('#tab-repeat-warm_c8_p50_ms');
    await repeatC8.click();
    assert(await page.locator('#view-repeat-warm_c8_p50_ms').isVisible());
    assert.equal(await c8.getAttribute('aria-selected'), 'true', 'First-ask and repeated-query concurrency controls are independent');
    const recall = page.locator('#detail-retrieval').getByRole('tab', { name: 'nDCG@10', exact: true });
    await recall.click();
    await recall.press('Home');
    assert.equal(await page.locator('#tab-recall_at_10').getAttribute('aria-selected'), 'true', 'Tab keyboard navigation');
    await page.locator('[data-select-system][value="fyrr"]').uncheck();
    await assertDiagnosticsUnranked(page);
    assert.equal(await page.locator('.chart-row[data-system="fyrr"]:visible').count(), 0);
    assert(new URL(page.url()).searchParams.get('systems').includes('onyx'));
    await page.locator('#speed-notes-tab-c8').click();
    await page.getByRole('button', { name: 'Copy selection link' }).click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), await page.locator('.permalink input').inputValue());
    await page.reload();
    await page.locator('#details[data-load-state="ready"]').waitFor();
    assert.equal(await page.locator('[data-select-system][value="fyrr"]').isChecked(), false, 'Permalink restores systems');
    assert.equal(await page.locator('#tab-first_ask_c8_p50_ms').getAttribute('aria-selected'), 'true', 'Permalink restores concurrency');
    assert.equal(await page.locator('#speed-notes-tab-c8').getAttribute('aria-selected'), 'true', 'Permalink restores paired story concurrency');
    assert(await page.locator('#comparison-options').evaluate(element => element.open));
    assert(await page.locator('#search-notes').evaluate(element => element.open), 'Permalink restores the open disclosure');
    await page.locator('[data-select-system][value="fyrr"]').check();
    assert.equal(await page.locator('[data-select-system][value="fyrr@jev-rerank"]').count(), 0, 'Private reranker never enters public filters');
    if (results.systems.some(system => system.variant === 'concurrency-8')) {
      const sized = page.locator('[data-select-system][value="fyrr@concurrency-8"]');
      assert.equal(await sized.isChecked(), false);
      await sized.check();
      assert(await page.locator('#view-first_ask_c8_p50_ms .chart-row[data-system="fyrr@concurrency-8"]').isVisible(), 'Sized reader has a separate C8 row');
      assert.match(await page.locator('#view-first_ask_c8_p50_ms .chart-row[data-system="fyrr@concurrency-8"] .run-note').textContent(), /reader.concurrency=8/);
      for (const id of ['view-first_ask_c1_p50_ms', 'detail-ingest', 'detail-retrieval']) assert.equal(await page.locator(`#${id} .chart-row[data-system="fyrr@concurrency-8"]`).count(), 0, 'Sized C8 row cannot imply an unmeasured track');
      assert.equal(await page.locator('.hero-evidence [data-system="fyrr@concurrency-8"]').count(), 0);
      await sized.uncheck();
    }
    for (const variant of results.systems.filter(system => results.variants?.[system.variant]?.experimental)) {
      const key = variant.id + '@' + variant.variant;
      const input = page.locator(`[data-select-system][value="${key}"]`);
      assert.equal(await input.isChecked(), false, 'Experimental runs are optional');
      await input.check();
      assert(await page.locator(`#view-first_ask_c8_p50_ms .chart-row[data-system="${key}"]`).isVisible());
      assert.match(await page.locator(`#view-first_ask_c8_p50_ms .chart-row[data-system="${key}"] .product-name`).textContent(), /experimental/);
      assert.equal(await page.locator(`.hero-evidence [data-system="${key}"]`).count(), 0, 'Experiments never replace headline rows');
      await input.uncheck();
    }
    const roundoffTie = results.details?.find(row => row.kind === 'question' && row.metric === 'ndcg_at_10' && (!row.variant || row.variant === 'default') && row.results.fyrr?.value != null && row.results.onyx?.value != null && row.results.fyrr.value !== row.results.onyx.value && Math.abs(row.results.fyrr.value - row.results.onyx.value) <= 8 * Number.EPSILON);
    if (roundoffTie) {
      for (const input of await page.locator('[data-select-system]').all()) if (!['fyrr', 'onyx'].includes(await input.inputValue())) await input.uncheck();
      await page.locator('#detail-metric').selectOption('ndcg_at_10');
      await page.locator('#tab-question').click();
      await page.locator('#detail-search').fill(roundoffTie.label);
      const row = page.locator('#detail-question tr[data-detail-metric="ndcg_at_10"]:visible');
      assert.equal(await row.count(), 1);
      assert.equal(await row.locator('.is-winner').count(), 2, 'Machine round-off is a tie, not a manufactured win');
      assert.equal((await row.locator('.relative:visible').allTextContents()).filter(text => text === 'best · tied').length, 2);
      await page.locator('#detail-search').fill('');
      await page.locator('#detail-metric').selectOption(results.website.headline_metric);
      await page.locator('#tab-category').click();
      for (const input of await page.locator('[data-select-system]').all()) if (input && await input.evaluate(element => element.defaultChecked)) await input.check();
    }
    await page.locator('.metrics-select summary').click();
    for (const p of [50, 95]) await page.locator(`[data-select-metric][value="first_ask_c8_p${p}_ms"]`).uncheck();
    assert(await page.locator('#speed-notes-tab-c8').isVisible(), 'Repeated measure keeps the paired concurrency view available');
    assert(await page.locator('#speed-notes-c8 [data-metric="warm_c8_p50_ms"]').isVisible());
    for (const p of [50, 95]) await page.locator(`[data-select-metric][value="first_ask_c8_p${p}_ms"]`).check();
    await page.locator('[data-select-metric][value="ingest_docs_per_min"]').uncheck();
    assert.equal(await page.locator('#detail-ingest [data-metric="ingest_docs_per_min"]').isVisible(), false);
    const details = page.locator('#detail-ingest .metric-panel:not([hidden]) details').first();
    await details.locator('summary').click();
    assert(await details.getAttribute('open') !== null);
    await page.screenshot({ path: `${report}/uncertainty-${suffix}.png` });
    if (!(await page.locator('#methodology').evaluate(element => element.open))) await page.locator('#methodology > summary').click();
    const [download] = await Promise.all([
      page.waitForEvent('download'),
      page.getByRole('link', { name: 'Download results JSON' }).click(),
    ]);
    assert.equal(download.suggestedFilename(), 'fyrr-comparison-results.json');
    assert.equal(await download.failure(), null);
    assert.deepEqual(JSON.parse(await readFile(await download.path(), 'utf8')), results, 'The complete downloaded file preserves every reviewed result');
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
    assert.equal(await page.locator('#serverless a[href="/docs/lambda"]').count(), 1);
    assert(await page.getByRole('link', { name: 'Use cases', exact: true }).isVisible());
    await assertNavigation(page);
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Merged homepage navigation fits');
    await page.getByRole('link', { name: 'Use cases', exact: true }).click();
    await page.locator('h1').waitFor();
    assert(await page.getByRole('link', { name: 'Compare', exact: true }).isVisible());
    assert(await page.getByRole('link', { name: 'Use cases', exact: true }).isVisible());
    await assertNavigation(page);
    await page.waitForFunction(() => {
      const canvas = document.querySelector('.step__sea');
      return canvas && canvas.width > 0 && canvas.getContext('2d').getImageData(0, 0, canvas.width, canvas.height).data.some((value, index) => index % 4 === 3 && value > 0);
    });
    assert(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Use cases remains functional after replay');

    assert.deepEqual(errors, [], `Browser errors at ${width}/${motion}`);
    await context.close();
    cases++;
    console.log(`PASS ${width}px / ${motion}: routing, data, no overflow, settled bar widths within 1%, tabs and keyboard, filters, clipboard permalink roundtrip, details, screenshots${motion === 'reduce' ? ', static map and lighthouse' : ''}`);
  }
} finally { await browser.close(); }
console.log(`Comparison browser cases: ${cases} passed, 0 failed, 0 ignored. Screenshots: ${report}`);
