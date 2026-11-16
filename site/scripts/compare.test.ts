import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readResults, parseResults, assertPublishable, headlineVerdict, renderMetric, renderSections, renderProducts, relativeValue } from '../lib/compare';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const fixture = await readResults();
fixture.draft = true;
fixture.synthetic_design_preview = false;
fixture.benchmark.publishable = true;
fixture.details = [];
for (const system of fixture.systems) for (const metric of Object.values(system.metrics)) {
  metric.status = 'n/e'; metric.value = null; metric.n = 0; metric.expected = 0;
  metric.attempted = 0; metric.missing = 0; metric.ci95 = null; metric.sample_ids = [];
}
test('draft data cannot be published, even by removing the draft flag', () => {
  assert.throws(() => assertPublishable(fixture), /placeholder/);
  assert.throws(() => parseResults({ ...fixture, draft: false }), /hashes|measurements/);
  const { draft: _, ...missing } = fixture;
  assert.throws(() => parseResults(missing), /draft flag/);
});
test('unavailable metrics have no bars and are never encoded as zero', () => {
  const html = renderMetric(fixture, 'answer_correctness');
  assert.equal((html.match(/class="bar(?: bar--fyrr)? is-empty"/g) ?? []).length, fixture.systems.length);
  assert(!html.includes('<i style='));
  assert(html.includes('n/e'));
  const bad = structuredClone(fixture);
  bad.systems[0].metrics.answer_correctness.value = 0;
  assert.throws(() => parseResults(bad), /value must match status/);
});
test('new systems and metrics render without a layout change', () => {
  const extra = structuredClone(fixture);
  extra.systems.push({ ...structuredClone(extra.systems[1]), id: 'new-system', name: 'New <system>' });
  extra.systems[0].metrics.new_metric = { status: 'measured', value: 0, unit: 'ms', n: 1 };
  const parsed = parseResults(extra);
  assert(renderSections(parsed).includes('id="additional"'));
  assert(renderSections(parsed).includes('data-metric="new_metric"'));
  assert(renderProducts(parsed).includes('New &lt;system&gt;'));
  assert(renderMetric(parsed, 'new_metric').includes('--w:0%'));
});
test('higher competitor scores draw longer bars with exact intervals', () => {
  const measured = structuredClone(fixture);
  measured.systems.forEach((system, index) => {
    system.metrics.answer_correctness = { status: index === 2 ? 'partial' : 'measured', value: index === 0 ? .4 : .8, unit: 'fraction', n: 10, attempted: 12, missing: 2, ci95: index === 0 ? [.3, .5] : [.7, .9] };
  });
  const html = renderMetric(parseResults(measured), 'answer_correctness');
  assert(html.includes('--w:40%'));
  assert(html.includes('--w:80%'));
  assert(html.includes('--low:70%;--high:90%'));
  assert(html.includes('80.0% *'));
  assert(html.includes('valid 10 / attempted 12 · missing 2'));
  assert(html.includes('<span>0%</span>'));
  assert(html.includes('<span>100%</span>'));
});
test('inconsistent units, samples and uncertainty fail at the boundary', () => {
  for (const change of [
    (data: typeof fixture) => { data.systems[1].metrics.answer_correctness.unit = 'ms'; },
    (data: typeof fixture) => { data.systems[0].metrics.answer_correctness.missing = 1; },
    (data: typeof fixture) => { data.systems[0].metrics.answer_correctness.ci95 = [.2, .3]; },
  ]) {
    const bad = structuredClone(fixture);
    change(bad);
    assert.throws(() => parseResults(bad), /Invalid comparison/);
  }
});
test('headline follows complete retrieval evidence and stays neutral on mismatched samples', () => {
  const data = structuredClone(fixture);
  data.draft = false;
  data.systems.forEach((system, index) => {
    system.metrics.recall_at_10 = { status: 'measured', value: index === 1 ? .9 : .8, unit: 'fraction', n: 2, expected: 2, attempted: 2, missing: 0, ci95: index === 1 ? [.85, .95] : [.75, .85], sample_ids: ['a', 'b'] };
  });
  assert(headlineVerdict(data).startsWith(`${data.systems[1].name} has the highest observed Recall@10`.replace('Recall@10', 'recall@10')));
  data.systems[1].metrics.recall_at_10.sample_ids = ['a', 'different'];
  assert(headlineVerdict(data).includes('still being measured'));
  data.systems[1].metrics.recall_at_10.status = 'partial';
  assert(headlineVerdict(data).includes('still being measured'));
});
test('zero is a measured result, ratios handle zero baselines and axes stay round', () => {
  const data = structuredClone(fixture);
  for (const system of data.systems) for (const key of ['warm_c1_p50_ms', 'warm_c1_p95_ms', 'warm_c8_p50_ms', 'warm_c8_p95_ms']) {
    system.metrics[key] = { status: 'measured', value: key.includes('p95') ? 142 : 0, unit: 'ms', n: 1, expected: 1, attempted: 1, missing: 0, ci95: key.includes('p95') ? [130, 153.36] : [0, 0] };
  }
  const html = renderSections(data);
  assert(html.includes('data-companion="warm_c1_p95_ms"'));
  assert(html.includes('<span>50</span><span>100</span><span>150</span><span>200</span>'));
  assert(!html.includes('168.7'));
  assert.equal(relativeValue(0, 0, 'lower_is_better'), 1);
  assert.equal(relativeValue(1, 0, 'lower_is_better'), Infinity);
});
test('first-ask and repeated-query latency have independent controls and diagnostics stay scoped', () => {
  const data = structuredClone(fixture);
  data.website.metrics.engine_diagnostic = { key: 'engine_diagnostic', label: 'Engine time', unit: 'ms', group: 'speed', direction: 'lower_is_better', ci: null, tier: 'secondary', scope: 'fyrr-only', description: 'Total minus embedding stage; not a cross-product measurement.' };
  for (const system of data.systems) system.metrics.engine_diagnostic = { status: 'n/e', value: null, unit: 'ms', n: 0 };
  const html = renderSections(parseResults(data));
  assert(html.includes('id="tab-first_ask_c8_p50_ms"'));
  assert(html.includes('id="tab-repeat-warm_c8_p50_ms"'));
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map(match => match[1]);
  assert.equal(new Set(ids).size, ids.length);
  const diagnostic = renderMetric(data, 'engine_diagnostic');
  assert(diagnostic.includes('No cross-product comparison'));
  assert(!diagnostic.includes('data-system="onyx"'));
  assert(!diagnostic.includes('data-system="pipeshub"'));
});
test('deploy guard rejects draft even when the local preview opt-in is set', () => {
  const dir = mkdtempSync(join(tmpdir(), 'fyrr-site-draft-'));
  try {
    const file = join(dir, 'results.json');
    writeFileSync(file, JSON.stringify(fixture));
    const child = spawnSync(process.execPath, ['scripts/check-compare.ts', '--publish'], { env: { ...process.env, SITE_ALLOW_DRAFT_COMPARE: '1', COMPARE_PREVIEW_RESULTS: file }, encoding: 'utf8' });
    assert.notEqual(child.status, 0);
    assert.match(child.stderr, /placeholder data/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
  assert.throws(() => assertPublishable({ ...fixture, draft: false, synthetic_design_preview: true }), /never be published/);
  assert.throws(() => parseResults({ ...fixture, synthetic_design_preview: true, benchmark: { ...fixture.benchmark, publishable: true } }), /publishable to false/);
});
test('an optional variant has its own row and cannot win the default headline', () => {
  const data = structuredClone(fixture);
  data.draft = false;
  data.systems.forEach((system, index) => {
    const value = system.variant === 'jev-rerank' ? 1 : index === 1 ? .9 : .8;
    system.metrics.recall_at_10 = { status: 'measured', value, unit: 'fraction', n: 2, expected: 2, attempted: 2, missing: 0, ci95: [value - .05, Math.min(1, value + .05)], sample_ids: ['a', 'b'] };
  });
  assert(headlineVerdict(data).startsWith(data.systems[1].name));
  assert(renderMetric(data, 'recall_at_10').includes('data-system="fyrr@jev-rerank"'));
  assert.equal((renderProducts(data).match(/class="product-card/g) ?? []).length, new Set(data.systems.map(system => system.id)).size);
  data.systems.push(structuredClone(data.systems[0]));
  assert.throws(() => parseResults({ ...data, draft: true }), /unique product\/variant/);
});
test('synthetic or smoke fixtures cannot overwrite the committed results', () => {
  const dir = mkdtempSync(join(tmpdir(), 'fyrr-site-import-'));
  const before = readFileSync('data/compare-results.json', 'utf8');
  try {
    mkdirSync(join(dir, 'data'));
    const canonical = join(dir, 'data/compare-results.json');
    writeFileSync(canonical, before);
    const importer = join(process.cwd(), 'scripts/import-compare.ts');
    for (const synthetic of [true, false]) {
      const file = join(dir, 'rejected.json');
      writeFileSync(file, JSON.stringify({ ...fixture, synthetic_design_preview: synthetic, benchmark: { ...fixture.benchmark, publishable: false } }));
      const child = spawnSync(process.execPath, [importer, file], { cwd: dir, encoding: 'utf8' });
      assert.notEqual(child.status, 0);
      assert.match(child.stderr, /cannot be imported/);
      assert.equal(readFileSync('data/compare-results.json', 'utf8'), before);
      assert.equal(readFileSync(canonical, 'utf8'), before);
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
