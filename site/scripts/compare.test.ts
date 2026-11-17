import { createHash } from 'node:crypto';
import { gzipSync } from 'node:zlib';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { type Results, readResults, parseResults, assertPublishable, headlineVerdict, comparisonScope, headlineSystems, renderMetric, renderSections, renderProducts, renderScoreboard, renderStories, renderDetailView, renderControls, renderLambdaProjection, lambdaProjectionCost, renderMethodology, renderProvenance, clientResults, verifyEvidenceArchive, relativeValue } from '../lib/compare';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const fixture = await readResults();
fixture.draft = true;
fixture.synthetic_design_preview = false;
fixture.benchmark.publishable = true;
fixture.details = [];
// Test cases own their observations; release imports must not change test order,
// promote actual costs into dummy runs or replace the unavailable default fixture.
fixture.systems = ['fyrr', 'onyx', 'pipeshub'].map(id => {
  const source = fixture.systems.find(system => system.id === id)!;
  return { id, name: source.name, variant: 'default', version: {}, config: {}, date: '', metrics: structuredClone(source.metrics), footprint: {}, notes: [] };
});
fixture.systems.push({ ...structuredClone(fixture.systems[0]), variant: 'concurrency-8' });

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
test('evidence downloads require the reviewed local filename, size and digest', async () => {
  const data = structuredClone(fixture);
  const payload = Buffer.from('reviewed test evidence');
  const sha256 = createHash('sha256').update(payload).digest('hex');
  const filename = `enterpriserag-small-v1-${sha256.slice(0, 12)}.tar.gz`;
  data.benchmark.evidence_archive = { url: `/compare/evidence/${filename}`, sha256, bytes: payload.length, label: 'Evidence bundle' };
  const directory = mkdtempSync(join(tmpdir(), 'fyrr-evidence-'));
  try {
    writeFileSync(join(directory, filename), payload);
    await verifyEvidenceArchive(parseResults(data), directory);
    assert(renderMethodology(data).includes('Evidence bundle'));
    assert(renderMethodology(data).includes(sha256));
    writeFileSync(join(directory, filename), 'changed');
    await assert.rejects(verifyEvidenceArchive(data, directory), /reviewed digest and size/);
    for (const url of ['https://example.test/bundle.tar.gz', '/compare/evidence/../../private.tar.gz', '/compare/evidence/mismatch.tar.gz']) {
      (data.benchmark.evidence_archive as any).url = url;
      assert.throws(() => parseResults(data), /archive path, digest and size/);
    }
    (data.benchmark.evidence_archive as any).url = `/compare/evidence/${filename}`;
    (data.benchmark.evidence_archive as any).bytes = 20_000_001;
    assert.throws(() => parseResults(data), /at most 20 MB/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
test('first-frame data contains only chart inputs and retains comparable sample identities', async () => {
  const data = await readResults();
  const before = JSON.stringify(data);
  const initial = clientResults(data, true);
  const encoded = JSON.stringify(initial);
  assert(!encoded.includes('sample_ids'));
  assert(initial.systems.every(system => !('footprint' in system)), 'Heavy resource records are excluded; metric group names remain valid chart inputs');
  assert(!encoded.includes('binary_sha256'));
  assert(!('details' in initial) && !('protocol' in initial));
  assert(encoded.length < 32_000, 'Inline chart payload has a fixed size budget independent of question count');
  assert(gzipSync(encoded).length < 5_000);
  const deferred = initial.systems.filter(system => system.deferred);
  assert(deferred.length > 0);
  assert(deferred.every(system => Object.keys(system.metrics).length === 0));
  const full = clientResults(data);
  for (const system of full.systems) {
    const raw = data.systems.find(run => run.id === system.id && run.variant === system.variant)!;
    for (const [key, metric] of Object.entries(system.metrics)) {
      assert.equal(metric.value, raw.metrics[key].value);
      assert.equal(metric.n, raw.metrics[key].n);
    }
  }
  assert.equal(JSON.stringify(data), before, 'Projection never alters canonical results');
});
test('large provenance is deferred without losing its explicit download or license notice', () => {
  const data = structuredClone(fixture);
  data.systems[0].version = { binary_sha256: 'unique-provenance-test-value' };
  const initial = renderMethodology(data, true);
  assert(!initial.includes('unique-provenance-test-value'));
  assert(initial.includes('id="provenance-body"'));
  assert(initial.includes('/compare/results.json'));
  assert(renderProvenance(data).includes('unique-provenance-test-value'));
});
test('unavailable metrics have no bars and are never encoded as zero', () => {
  const html = renderMetric(fixture, 'recall_at_10');
  assert.equal((html.match(/class="bar(?: bar--fyrr)? is-empty"/g) ?? []).length, fixture.systems.filter(system => system.variant !== 'concurrency-8').length);
  assert(!html.includes('<i style='));
  assert(html.includes('n/e'));
  const bad = structuredClone(fixture);
  bad.systems[0].metrics.recall_at_10.value = 0;
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
    system.metrics.recall_at_10 = { status: index === 2 ? 'partial' : 'measured', value: index === 0 ? .4 : .8, unit: 'fraction', n: 10, attempted: 12, missing: 2, ci95: index === 0 ? [.3, .5] : [.7, .9] };
  });
  const html = renderMetric(parseResults(measured), 'recall_at_10');
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
    (data: typeof fixture) => { data.systems[1].metrics.recall_at_10.unit = 'ms'; },
    (data: typeof fixture) => { data.systems[0].metrics.recall_at_10.missing = 1; },
    (data: typeof fixture) => { data.systems[0].metrics.recall_at_10.ci95 = [.2, .3]; },
  ]) {
    const bad = structuredClone(fixture);
    change(bad);
    assert.throws(() => parseResults(bad), /Invalid comparison/);
  }
});
test('fractional scores tolerate only machine round-off and preserve raw observations', () => {
  const data = structuredClone(fixture);
  data.systems[0].metrics.ndcg_at_10 = { status: 'measured', value: 1.0000000000000002, unit: 'fraction', n: 1, ci95: [1, 1.0000000000000002] };
  const parsed = parseResults(data);
  assert.equal(parsed.systems[0].metrics.ndcg_at_10.value, 1.0000000000000002);
  assert.equal(relativeValue(1, 1.0000000000000002, 'higher_is_better'), 1);
  assert.equal(relativeValue(.999999, 1, 'higher_is_better') === 1, false);
  data.systems[0].metrics.ndcg_at_10.value = 1.000001;
  assert.throws(() => parseResults(data), /fraction range/);
});
test('headline follows complete retrieval evidence and stays neutral on mismatched samples', () => {
  const data = structuredClone(fixture);
  data.draft = false;
  data.systems.forEach((system, index) => {
    system.metrics.recall_at_10 = { status: 'measured', value: index === 1 ? .9 : .8, unit: 'fraction', n: 2, expected: 2, attempted: 2, missing: 0, ci95: index === 1 ? [.85, .95] : [.75, .85], sample_ids: ['a', 'b'] };
  });
  assert(headlineVerdict(data).includes(`10.0 pp behind ${data.systems[1].name}`));
  data.systems[1].metrics.recall_at_10.sample_ids = ['a', 'different'];
  assert(headlineVerdict(data).startsWith('Search quality is still being measured'));
  data.systems[1].metrics.recall_at_10.status = 'partial';
  assert(!headlineVerdict(data).includes('10.0 pp behind Onyx'), 'Partial results cannot establish a winner');
});
test('cost-led narrative makes only supported claims and lets competitors win', () => {
  const data = structuredClone(fixture);
  data.draft = false;
  data.systems.filter(system => !system.variant || system.variant === 'default').forEach((system, index) => {
    const quality = index === 1 ? .91 : .9;
    system.metrics.recall_at_10 = { status: 'measured', value: quality, unit: 'fraction', n: 2, expected: 2, attempted: 2, missing: 0, ci95: [quality - .01, quality + .01], sample_ids: ['a', 'b'] };
    system.metrics.first_ask_c1_p50_ms = { status: 'measured', value: 100 * (index + 1), unit: 'ms', n: 2, expected: 2, attempted: 2, missing: 0, ci95: [95 * (index + 1), 105 * (index + 1)] };
    system.metrics.ingest_wall_s = { status: 'measured', value: 100 * (index + 1), unit: 'seconds', n: 1, expected: 1, attempted: 1, missing: 0 };
    const cost = index === 0 ? 10 : index * 50;
    system.metrics.monthly_cost_usd = { status: 'measured', value: cost, unit: 'USD/month', n: 1, expected: 1, attempted: 1, missing: 0 };
    system.metrics.load_rss_peak_bytes = { status: 'measured', value: 1024 ** 3, unit: 'bytes', n: 1 };
    system.footprint.monthly_cost_estimate = { basis: 'documented-production', model_version: 2, status: 'estimate', total_usd: cost, assumptions: { hours_per_month: 730, documents: 20000, queries_per_month: 100000, daily_changed_fraction: .01 }, components: { compute_usd_month: cost, storage_usd_month: 0, network_usd_month: 0, control_plane_usd_month: 0, security_usd_month: 0, observability_usd_month: 0, api_usd_month: 0, embedding_usd_per_document: 0, llm_enrichment_usd_per_document: 0 }, line_items: [{ key: 'compute', label: 'Compute', required: true, usd_month: cost, deployment_source: 'https://example.test/deployment', price_source: { url: 'https://example.test/pricing' } }, { key: 'embeddings', kind: 'api', label: 'Embeddings', required: true, usd_month: 0, deployment_source: null, price_source: { url: 'https://example.test/pricing' } }], missing: [], sources: [{ label: 'Test tariff, dated', url: 'https://example.test/pricing' }] };
  });
  const verdict = headlineVerdict(data);
  assert.equal(verdict, 'Similar search quality. A fraction of the estimated running cost. Faster queries and indexing.');
  const scoped = structuredClone(data);
  scoped.draft = true; scoped.systems[0].version = { version: 'test' }; scoped.systems[0].date = '2026-11-17';
  for (const metric of Object.values(scoped.systems.find(system => system.id === 'pipeshub')!.metrics)) Object.assign(metric, { status: 'n/e', value: null, n: 0, expected: 0, attempted: 0, missing: 0, ci95: null, sample_ids: [] });
  assert.equal(headlineVerdict(scoped), verdict, 'n/e products are disclosed separately and do not erase measured comparisons');

  const stories = renderStories(data);
  assert(stories.indexOf('id="story-footprint"') < stories.indexOf('id="story-speed"'));
  assert(stories.indexOf('id="story-speed"') < stories.indexOf('id="story-ingest"'));
  assert(stories.indexOf('id="story-ingest"') < stories.indexOf('id="story-quality"'));
  assert(stories.includes('Costs a fraction to run.'));
  assert(stories.includes('Finds the right documents (comparable).'));
  assert(stories.includes('LLM enrichment estimate / document'));
  for (const weaken of [
    (weaker: typeof data) => { weaker.systems[0].metrics.recall_at_10.value = .889; weaker.systems[0].metrics.recall_at_10.ci95 = [.888, .89]; },
    (weaker: typeof data) => { const peer = weaker.systems.find(system => system.id === 'pipeshub')!; peer.metrics.recall_at_10.value = .5; peer.metrics.recall_at_10.ci95 = [.49, .51]; },
    (weaker: typeof data) => { weaker.systems[0].metrics.first_ask_c1_p50_ms.ci95 = [90, 220]; },
    (weaker: typeof data) => { weaker.systems[0].metrics.ingest_wall_s.value = 300; },
    (weaker: typeof data) => { weaker.systems[0].footprint.monthly_cost_estimate = { status: 'n/e' }; },
    (weaker: typeof data) => { (weaker.systems[0].footprint.monthly_cost_estimate as any).line_items[0].deployment_source = null; },
    (weaker: typeof data) => { (weaker.systems[0].footprint.monthly_cost_estimate as any).line_items[1].price_source = null; },
  ]) {
    const weaker = structuredClone(data);
    weaken(weaker);
    assert(!headlineVerdict(weaker).includes('Faster queries and indexing.'), 'Short marketing headline requires every guarded clause');
  }
  const own = data.systems[0];
  own.metrics.recall_at_10.value = .5;
  own.metrics.recall_at_10.ci95 = [.49, .51];
  own.metrics.first_ask_c1_p50_ms.value = 1000;
  own.metrics.first_ask_c1_p50_ms.ci95 = [950, 1050];
  own.metrics.ingest_wall_s.value = 1000;
  own.footprint.monthly_cost_estimate = { status: 'n/e', total_usd: null, assumptions: {}, components: {}, missing: ['LLM enrichment cost'] };
  const revised = headlineVerdict(data);
  assert(revised.includes('41.0 pp behind Onyx'));
  assert(revised.includes('Onyx has lower observed median query latency'));
  assert(revised.includes('Onyx has lower measured indexing time'));
  assert(revised.includes('Monthly operating cost is not yet estimated'));
  assert(!revised.includes('Similar'));
  assert(!renderStories(data).includes('Costs a fraction to run.'));
});
test('measured review data gets a factual headline without claiming a win over missing products', () => {
  const data = structuredClone(fixture);
  const own = data.systems[0]; own.version = { version: 'test' }; own.date = '2026-11-17';
  for (const [key, value] of Object.entries({ recall_at_10: .9, first_ask_c1_p50_ms: 100, ingest_wall_s: 600, monthly_cost_usd: 40 })) Object.assign(own.metrics[key], { status: 'measured', value, n: 1, expected: 1, attempted: 1, missing: 0 });
  const verdict = headlineVerdict(data);
  assert(verdict.includes('90.0% recall@10. 100 ms new-question p50. 10 min to index. $40/month estimated production cost.'));
  assert(verdict.includes('PipesHub: 20k run incomplete'));
  assert(!/faster|cheaper|similar/i.test(verdict));
});
test('light and team cost assumptions stay separate and expose capacity pass criteria', () => {
  const data = structuredClone(fixture);
  data.systems[0].footprint.minimum_deployment_cost_estimate = {
    status: 'estimate', total_usd: 10, assumptions: {}, components: {}, scenarios: {
      light: { status: 'estimate', total_usd: 10, assumptions: { load_concurrency: 1, hours_per_month: 730, corpus_documents: 20000, queries_per_month: 100000, daily_changed_fraction: .01 }, components: {}, sizing: {} },
      team: { status: 'estimate', total_usd: 20, configuration_note: 'reader.concurrency=8; measured sized reader', assumptions: { load_concurrency: 8 }, components: {}, sizing: { selected_cpus: 2, measured_peak_rss_bytes: 1024 ** 3, ram_headroom_fraction: .25, capacity_tests: [
        { cpus: 2, concurrency: 8, expected: 250, valid: 250, errors: 0, p95_ms: 120, threshold_p95_ms: 150, pass: true },
        { cpus: 4, concurrency: 8, expected: 250, valid: 249, errors: 1, p95_ms: 130, threshold_p95_ms: 150, pass: true },
      ] } },
    },
  };
  const before = renderScoreboard(data);
  const html = renderStories(data);
  assert(html.includes('id="cost-light"'));
  assert(html.includes('id="cost-team"'));
  assert(html.includes('reader.concurrency=8; measured sized reader'));
  assert(html.includes('120 / 150 ms'));
  assert(html.includes('pass<small>250 / 250 valid · 0 errors'));
  assert(html.includes('fail<small>249 / 250 valid · 1 errors'));
  assert(html.includes('peak memory plus 25% headroom'));
  assert(html.includes('730 h/month · 20,000 documents · 100,000 searches/month · 1.0% documents updated daily.'));
  (data.systems[0].footprint.minimum_deployment_cost_estimate as any).scenarios.team.total_usd = 999;
  assert.equal(renderScoreboard(data), before, 'Team estimates cannot replace headline cost');
});
test('documented production costs retain unknown required items and keep the minimum separate', () => {
  const data = structuredClone(fixture);
  data.systems[0].footprint.monthly_cost_estimate = {
    basis: 'documented-production', model_version: 2, status: 'partial', total_usd: null, known_subtotal_usd: 42,
    profile: 'Documented production test', assumptions: { hours_per_month: 730, documents: 20000, queries_per_month: 100000, daily_changed_fraction: .01 },
    components: { compute_usd_month: 42, storage_usd_month: null },
    line_items: [{ label: 'Snapshot storage', count: 1, quantity: null, unit: 'GB-month', unit_price_usd: .05, usd_month: null, required: true, quantity_basis: 'Used blocks unknown', deployment_source: 'https://example.test/deployment', price_source: { url: 'https://example.test/pricing' } }],
    missing: ['Snapshot used blocks'], notes: ['Availability and topology differ.'],
  };
  data.systems[0].footprint.minimum_deployment_cost_estimate = { status: 'estimate', total_usd: 10 };
  const html = renderStories(data);
  assert(html.includes('Known subtotal: $42 / month · incomplete'));
  assert(html.includes('Snapshot used blocks'));
  assert(html.includes('Used blocks unknown'));
  assert(html.includes('Snapshot storage'));
  assert(html.includes('href="https://example.test/deployment"'));
  assert(html.includes('href="https://example.test/pricing"'));
  assert(html.includes('not a cloud capacity or availability guarantee'));
  assert(html.includes('id="minimum-cost-details"><summary>'));
  const firstCard = html.slice(html.indexOf('id="cost-production"'), html.indexOf('id="minimum-cost-details"'));
  assert(firstCard.includes('class="cost-total">n/e / month'));
  assert(!firstCard.includes('class="cost-total">$42'));
  assert(renderScoreboard(data).includes('Minimum to run: $10'));
  assert.equal(data.systems[0].metrics.monthly_cost_usd.value, null, 'The minimum never fills the main cost metric');
});
test('overview separates documented deployment counts, cost bounds and diagnostic ingest from measurements', () => {
  const data = structuredClone(fixture);
  data.systems[0].footprint.monthly_cost_estimate = { component_counts: { always_on_reader_instances: 1, scheduled_writer_role: 1, tls_proxy_processes: 1 } };
  const pipes = data.systems.find(system => system.id === 'pipeshub')!;
  pipes.footprint.monthly_cost_estimate = { status: 'partial', total_usd: null, known_subtotal_usd: 2551.517, component_counts: { app_pods_min: 2, dind_sidecars: 2, proxy_sidecars: 2, mongodb_members: 3, redis_members: 2, qdrant_nodes: 3, neo4j_community_nodes: 1 } };
  pipes.default_ingest_baseline = { docs_per_min: 7.8042, projected_20000_seconds: 153763, notes: ['Interrupted 1k diagnostic'] };
  pipes.ingest_diagnostics = [{ configuration_id: 'full-final-segment1', comparable: false, docs_per_min: 70.5662 }, { configuration_id: 'full-final-segment2', comparable: false, docs_per_min: 87.3404 }, { configuration_id: 'default', comparable: false, ...pipes.default_ingest_baseline }];
  const html = renderScoreboard(data);
  assert(html.includes('data-overview-deployment="true" data-status="measured" data-value="3"'));
  assert(html.includes('data-overview-deployment="true" data-status="measured" data-value="15"'));
  assert(html.includes('reader + writer job + TLS proxy; plus S3'));
  assert(html.includes('≥ $2,552/month'));
  assert(html.includes('Known subtotal; enrichment n/e'));
  assert(html.includes('about 71–87 docs/min'));
  assert(html.includes('Measured segments; full 20k time n/e (harness interruption)'));
  assert(!html.includes('7.8 docs/min'));
  assert(!html.includes('projected ~43 h'));
  assert(renderStories(data).includes('7.8 docs/min'));
  for (const key of ['service_count', 'monthly_cost_usd', 'ingest_wall_s']) assert.equal(pipes.metrics[key].value, null, 'Raw measurements are never overwritten by deployment or diagnostic display values');
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
test('new and repeated questions stay visible together without changing headline speed', () => {
  const data = structuredClone(fixture);
  data.draft = false;
  data.protocol.latency = { query_embedding_cache: { fyrr: 'Reuses in-process embeddings', onyx: 'Reuses cached embeddings for 15 minutes', pipeshub: false } };
  data.systems.filter(system => !system.variant || system.variant === 'default').forEach((system, index) => {
    const first = index === 1 ? 100 : index === 0 ? 200 : 300;
    const repeat = index === 0 ? 1 : 50;
    for (const p of [50, 95]) {
      const multiplier = p === 50 ? 1 : 2;
      system.metrics[`first_ask_c1_p${p}_ms`] = { status: 'measured', value: first * multiplier, unit: 'ms', n: 2, expected: 2, attempted: 2, missing: 0, ci95: [first * multiplier, first * multiplier] };
      system.metrics[`warm_c1_p${p}_ms`] = { status: 'measured', value: repeat * multiplier, unit: 'ms', n: 6, expected: 6, attempted: 6, missing: 0, ci95: [repeat * multiplier, repeat * multiplier] };
    }
  });
  const html = renderStories(data);
  const c1 = html.slice(html.indexOf('id="story-speed-c1"'), html.indexOf('id="story-speed-c8"'));
  const c8 = html.slice(html.indexOf('id="story-speed-c8"'));
  for (const [block, c] of [[c1, 1], [c8, 8]] as const) {
    assert(block.includes(`data-metric="first_ask_c${c}_p50_ms"`));
    assert(block.includes(`data-metric="warm_c${c}_p50_ms"`));
    assert(block.includes('New question</h3>'));
    assert(block.includes('Repeated question</h3>'));
  }
  assert(html.includes('Reuses in-process embeddings'));
  assert(html.includes('Reuses cached embeddings for 15 minutes'));
  assert(html.includes('does not reuse cached query embeddings'));
  const ids = [...(html + renderSections(data)).matchAll(/\bid="([^"]+)"/g)].map(match => match[1]);
  assert.equal(new Set(ids).size, ids.length);
  assert(renderScoreboard(data).includes('Repeated question<br>p50 1 ms · p95 2 ms'));
  assert(renderControls(data).includes('value="warm_c1_p50_ms" checked'));
  assert(headlineVerdict(data).includes('Onyx has lower observed median query latency'));
  assert(!headlineVerdict(data).includes('Faster queries'));
});
test('noncomparable diagnostics cannot produce ratios, heatmap winners or a headline win', () => {
  const data = structuredClone(fixture);
  const key = 'voyage_probe_c1_p50_ms';
  data.website.metrics[key] = { key, label: 'Voyage provider probe', unit: 'ms', group: 'speed', direction: 'lower_is_better', ci: null, tier: 'secondary', comparable: false, description: 'Provider and network diagnostic; not an engine measurement.' };
  data.systems.forEach((system, i) => { system.metrics[key] = { status: 'measured', value: i + 1, unit: 'ms', n: 1, expected: 1, attempted: 1, missing: 0, ci95: [i + 1, i + 1], sample_ids: ['a'] }; });
  data.details = [{ id: 'probe', label: 'Probe', kind: 'category', metric: key, results: Object.fromEntries(data.systems.filter(system => !system.variant || system.variant === 'default').map(system => [system.id, system.metrics[key]])) }];
  data.website.headline_metric = key;
  data.website.overview_metrics = [key];
  const parsed = parseResults(data);
  const html = renderMetric(parsed, key);
  assert(html.includes('Diagnostic only · no product ranking'));
  assert(html.includes('Provider and network diagnostic'));
  assert(html.includes('diagnostic · unranked'));
  assert(!html.includes('×'));
  assert(!html.includes('best'));
  assert(renderScoreboard(parsed).includes('diagnostic · unranked'));
  assert(renderDetailView(parsed).includes('diagnostic · unranked'));
  parsed.draft = false;
  assert(headlineVerdict(parsed).includes('does not establish a product ranking'));
  // Execute the actual browser ranking function with a minimal DOM surface.
  const source = readFileSync('src/compare.js', 'utf8');
  const start = source.indexOf('  function equalValue(');
  const end = source.indexOf('\n  function update()', start);
  assert(start > 0 && end > start);
  const clientRatios = new Function('results', source.slice(start, end) + '\nreturn ratios;')(data);
  for (const heatmap of [false, true]) {
    const classes = new Set(['is-best', 'is-winner']);
    const properties = new Map([['--heat', '.14']]);
    const classList = { remove: (name: string) => classes.delete(name), contains: (name: string) => classes.has(name), add: (name: string) => classes.add(name), toggle: (name: string, on: boolean) => on ? classes.add(name) : classes.delete(name) };
    const relative = { textContent: '×1.00 best', classList };
    const cell = { dataset: { system: data.systems[0].id, status: 'measured', value: '1', n: '1', attempted: '1', expected: '1', missing: '0', samples: 'a' }, classList, querySelector: () => relative, style: { removeProperty: (name: string) => properties.delete(name), setProperty: (name: string, value: string) => properties.set(name, value) } };
    clientRatios([cell], 'lower_is_better', heatmap, 'ms', key);
    assert.equal(relative.textContent, 'diagnostic · unranked');
    assert(!classes.has('is-best') && !classes.has('is-winner'));
    assert(!properties.has('--heat'));
  }
  const bad = structuredClone(data) as unknown as { website: { metrics: Record<string, { comparable: unknown }> } };
  bad.website.metrics[key].comparable = 'false';
  assert.throws(() => parseResults(bad), /metric presentation metadata/);
});
test('ingest overview uses the reported throughput and never infers a missing rate', () => {
  const data = structuredClone(fixture);
  data.benchmark.documents = 20_000;
  data.systems[0].metrics.ingest_wall_s = { status: 'measured', value: 1200, unit: 'seconds', n: 1 };
  data.systems[0].metrics.ingest_docs_per_min = { status: 'measured', value: 777, unit: 'docs/min', n: 1 };
  data.systems[1].metrics.ingest_wall_s = { status: 'partial', value: 600, unit: 'seconds', n: 1 };
  data.systems[2].metrics.ingest_wall_s = { status: 'measured', value: 0, unit: 'seconds', n: 1 };
  const html = renderScoreboard(parseResults(data));
  assert(html.includes('data-score-metric="ingest_wall_s"'));
  assert(html.includes('20.0 min'));
  assert(html.includes('777 docs/min'));
  assert(!html.includes('1,000 docs/min'));
  assert(!html.includes('2,000 docs/min'));
  assert(!html.includes('Infinity'));
  assert.equal((html.match(/n\/e docs\/min/g) ?? []).length, 2);
  assert(!html.includes('fyrr@concurrency-8'));
  data.systems[0].metrics.ingest_docs_per_min.value = 0;
  assert(renderScoreboard(data).includes('0 docs/min'));
  data.systems[0].metrics.ingest_docs_per_min.status = 'partial';
  assert(renderScoreboard(data).includes('0 docs/min *'));
});
test('ingest story stacks only exposed elapsed stages within total wall time', () => {
  const data = structuredClone(fixture);
  const system = data.systems[0];
  system.metrics.ingest_wall_s = { status: 'measured', value: 1200, unit: 'seconds', n: 1 };
  system.metrics.embedding_cost_usd = { status: 'measured', value: 2, unit: 'USD', n: 1 };
  data.website.metrics.ingest_preprocessing_seconds = { key: 'ingest_preprocessing_seconds', label: 'Preprocessing', unit: 'seconds', group: 'ingest', direction: 'lower_is_better', ci: null, stage_of: 'ingest_wall_s', stage_order: 0 };
  system.metrics.ingest_preprocessing_seconds = { status: 'measured', value: 300, unit: 'seconds', n: 1 };
  const html = renderStories(parseResults(data));
  assert(html.includes('Indexing time.'));
  assert(html.includes('class="ingest-segment" style="width:25%'));
  assert(html.includes('class="ingest-segment" style="width:75%'));
  assert(html.includes('Other / unreported: 900 s'));
  assert(html.includes('Embedding cost: $2'));
  assert(html.includes('Stage breakdown n/e; total wall time only.'));
  system.metrics.ingest_preprocessing_seconds.value = 1500;
  const overlapping = renderStories(parseResults(data));
  assert(!overlapping.includes('class="ingest-segment"'));
  assert(overlapping.includes('Stages cannot be stacked against this wall time'));
  data.website.metrics.ingest_preprocessing_seconds.unit = 'ms';
  assert.throws(() => parseResults(data), /ingest stages must be elapsed seconds/);
});
test('ingest diagnostics retain their distinct scopes without filling canonical bars', () => {
  const data = structuredClone(fixture);
  const product = data.systems.find(system => system.id === 'pipeshub')!;
  product.ingest_diagnostics = ['default', 'parallel-partitions', 'scaled-indexing'].map((configuration_id, index) => ({ configuration_id, comparable: false, docs_per_min: 5 + index, interval_seconds: 120, newly_completed_documents: 10 + index, projected_20000_seconds: 20000, smoke_corpus_documents: index === 1 ? undefined : 1000, submitted_documents: index === 1 ? 20000 : undefined, completed_1000_ingest: index === 2, notes: [`Limit for ${configuration_id}`] }));
  const parsed = parseResults(data);
  const stories = renderStories(parsed);
  assert(stories.includes('separate ingest observations'));
  assert(stories.includes('Scaled indexing'));
  assert(stories.includes('1,000-document ingest completed'));
  assert(stories.includes('20,000 documents · interrupted / partial window'));
  assert.equal((stories.match(/PROJECTED 5.56 h/g) ?? []).length, 3);
  assert(stories.includes('Limit for parallel-partitions'));
  assert(!renderMetric(parsed, 'ingest_wall_s', { ingest: true }).includes('<i style='));
  assert.equal(product.metrics.ingest_docs_per_min.value, null);
  assert(!renderScoreboard(parsed).includes('5.56'));
  product.ingest_diagnostics[0].comparable = true as false;
  assert.throws(() => parseResults(data), /noncomparable ingest diagnostics/);
  product.ingest_diagnostics[0].comparable = false;
  product.ingest_diagnostics[1].configuration_id = 'default';
  assert.throws(() => parseResults(data), /distinct noncomparable/);
});
test('ingest discloses accepted coverage and rejects inconsistent rejection counts', () => {
  const data = structuredClone(fixture);
  const system = data.systems[0];
  system.ingest_coverage = { submitted: 20_000, accepted: 19_999, rejected: 1, rejected_documents: [{ id: 'rejected-test-document', reason: 'unsupported input' }] };
  system.metrics.ingest_wall_s = { status: 'measured', value: 1200, unit: 'seconds', n: 1 };
  system.metrics.ingest_docs_per_min = { status: 'measured', value: 999.95, unit: 'docs/min', n: 1 };
  data.website.metrics.ingest_coverage_fraction = { key: 'ingest_coverage_fraction', label: 'Accepted ingest coverage', unit: 'fraction', group: 'ingest', direction: 'higher_is_better', tier: 'primary', ci: null };
  system.metrics.ingest_coverage_fraction = { status: 'measured', value: .99995, unit: 'fraction', n: 1 };
  data.systems[1].metrics.ingest_coverage_fraction = { status: 'measured', value: 1, unit: 'fraction', n: 1 };
  const parsed = parseResults(data);
  const expected = '19,999 / 20,000 accepted · 1 rejected';
  assert(renderScoreboard(parsed).includes(expected));
  assert(renderScoreboard(parsed).includes('1,000 docs/min'));
  const story = renderStories(parsed);
  assert(story.includes(expected));
  assert(story.includes('until every accepted document is searchable'));
  assert(story.includes('Coverage n/e'));
  const coverage = renderMetric(parsed, 'ingest_coverage_fraction');
  assert(coverage.includes('99.995%'));
  assert(coverage.includes('-0.005 pp'));
  system.ingest_coverage.accepted = 20_000;
  assert.throws(() => parseResults(data), /ingest coverage must reconcile/);
  system.ingest_coverage.accepted = 19_999;
  system.ingest_coverage.rejected_documents = [];
  assert.throws(() => parseResults(data), /ingest coverage must reconcile/);
  system.ingest_coverage = { submitted: null, accepted: null, rejected: null, rejected_documents: [] };
  assert(renderStories(parseResults(data)).includes('Coverage n/e'));
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
    const value = system.variant && system.variant !== 'default' ? 1 : index === 1 ? .9 : .8;
    system.metrics.recall_at_10 = { status: 'measured', value, unit: 'fraction', n: 2, expected: 2, attempted: 2, missing: 0, ci95: [value - .05, Math.min(1, value + .05)], sample_ids: ['a', 'b'] };
  });
  assert(headlineVerdict(data).includes(`10.0 pp behind ${data.systems[1].name}`));
  assert(renderMetric(data, 'first_ask_c8_p50_ms').includes('data-system="fyrr@concurrency-8"'));
  assert.equal((renderProducts(data).match(/class="product-card/g) ?? []).length, new Set(data.systems.map(system => system.id)).size);
  data.systems.push(structuredClone(data.systems[0]));
  assert.throws(() => parseResults({ ...data, draft: true }), /unique product\/variant/);
});
test('sized C8 variant stays separate from default wins and out of unmeasured tracks', () => {
  const data = structuredClone(fixture);
  const sized = data.systems.find(system => system.variant === 'concurrency-8')!;
  assert(sized);
  data.systems[0].metrics.first_ask_c8_p50_ms = { status: 'partial', value: 100, unit: 'ms', n: 200, expected: 250, attempted: 250, missing: 50, ci95: [90, 110] };
  const before = structuredClone(data.systems[0]);
  sized.metrics.first_ask_c8_p50_ms = { status: 'measured', value: 1, unit: 'ms', n: 250, expected: 250, attempted: 250, missing: 0, ci95: [1, 1] };
  const c8 = renderMetric(parseResults(data), 'first_ask_c8_p50_ms');
  assert(c8.includes('data-system="fyrr@concurrency-8"'));
  assert(c8.includes('reader.concurrency=8'));
  assert(c8.includes('valid 200 / attempted 250 · missing 50'));
  assert(c8.includes('partial · unranked'));
  for (const key of ['first_ask_c1_p50_ms', 'warm_c1_p50_ms', 'ingest_wall_s', 'embedding_cost_usd', 'recall_at_10']) assert(!renderMetric(data, key).includes('data-system="fyrr@concurrency-8"'));
  assert(!renderScoreboard(data).includes('fyrr@concurrency-8'));
  assert(!renderStories(data).includes('fyrr@concurrency-8'));
  data.draft = false;
  data.systems.forEach((system, index) => { system.metrics.recall_at_10 = { status: 'measured', value: system.variant === 'concurrency-8' ? 1 : index === 1 ? .9 : .8, unit: 'fraction', n: 1, expected: 1, attempted: 1, missing: 0, ci95: [0, 1], sample_ids: ['a'] }; });
  assert(headlineVerdict(data).includes(`10.0 pp behind ${data.systems[1].name}`));
  // Rendering never mutates the default run's original C8 refusal evidence.
  assert.deepEqual(data.systems[0].metrics.first_ask_c8_p50_ms, before.metrics.first_ask_c8_p50_ms);
});
test('an authorized completed alternative represents its product once with visible baseline caveats', () => {
  const data = structuredClone(fixture);
  const original = data.systems.find(system => system.id === 'pipeshub')!;
  const parallel = { ...structuredClone(original), variant: 'parallel-partitions', headline_note: 'FAIR_SCHEDULING_PARALLEL_PARTITIONS=true, non-default, source-documented; default 20k ingest not completed', default_ingest_baseline: { docs_per_min: 10, interval_seconds: 60, newly_completed_documents: 10, projected_20000_seconds: 120000, notes: ['Observed from an intentionally interrupted 1k smoke index; rate sample only.'] } };
  data.variants!['parallel-partitions'] = { label: 'PipesHub parallel partitions', headline: true, systems: ['pipeshub'], replaces_headline_variant: 'default', reranker: null, description: parallel.headline_note, source_url: 'https://github.com/pipeshub-ai/pipeshub-ai/blob/v1.0.0/backend/python/app/services/messaging/config.py#L533' };
  data.systems.push(parallel);
  assert.equal(headlineSystems(parseResults(data)).find(system => system.id === 'pipeshub')?.variant, 'default', 'A planned or incomplete alternative cannot replace the baseline');
  data.systems.forEach(system => { system.metrics.recall_at_10 = { status: 'measured', value: system === parallel ? .95 : .8, unit: 'fraction', n: 1, expected: 1, attempted: 1, missing: 0, sample_ids: ['a'], ci95: [0, 1] }; });
  parallel.metrics.ingest_wall_s = { status: 'measured', value: 1000, unit: 'seconds', n: 1 };
  const parsed = parseResults(data);
  const selected = headlineSystems(parsed);
  assert.equal(selected.length, 3);
  assert.equal(selected.find(system => system.id === 'pipeshub')?.variant, 'parallel-partitions');
  const board = renderScoreboard(parsed);
  assert.equal((board.match(/class="score-row/g) ?? []).length, 3);
  assert(board.includes('/compare/runs/pipeshub--parallel-partitions.json'));
  assert(board.includes('FAIR_SCHEDULING_PARALLEL_PARTITIONS=true, non-default'));
  assert(!board.includes('10 docs/min observed'));
  assert(!board.includes('PROJECTED 33.33 h'));
  assert(!board.includes('default-ingest-baseline'));
  assert.equal(original.metrics.ingest_wall_s.status, 'n/e');
  assert(!board.includes('data-value="120000"'));
  assert(!renderStories(parsed).includes('data-system="pipeshub"'));
  const controls = renderControls(parsed);
  assert(controls.includes('value="pipeshub@parallel-partitions" checked'));
  assert(!controls.includes('value="pipeshub" checked'));
  parsed.draft = false;
  assert(headlineVerdict(parsed).includes('15.0 pp behind PipesHub (non-default configuration)'));
  data.variants!['parallel-partitions'].systems = ['onyx'];
  assert.equal(headlineSystems(data).find(system => system.id === 'pipeshub')?.variant, 'default', 'Representative authorization is product scoped');
  data.variants!['parallel-partitions'].systems = ['pipeshub'];
  data.systems.push({ ...structuredClone(parallel), variant: 'another-alternative' });
  data.variants!['another-alternative'] = { ...data.variants!['parallel-partitions'] };
  data.draft = true;
  assert.throws(() => parseResults(data), /one authorized headline representative per product/);
});
test('a missing ingest clock cannot hide independent search evidence or verified coverage', () => {
  for (const key of ['recall_at_10', 'first_ask_c1_p50_ms']) {
    const data = structuredClone(fixture);
    const source = data.systems.find(system => system.id === 'pipeshub')!;
    const run = { ...structuredClone(source), variant: 'parallel-partitions', headline_note: 'Non-default parallel scheduling; ingest observer interrupted, search measured independently.', notes: ['Ingest clock invalid; verified inventory and shared probe completed.'] };
    Object.assign(run.metrics[key], { status: 'measured', value: key === 'recall_at_10' ? .9 : 100, n: 1, expected: 1, attempted: 1, missing: 0, sample_ids: ['test-question'] });
    for (const metric of ['ingest_wall_s', 'ingest_docs_per_min']) Object.assign(run.metrics[metric], { status: 'n/e', value: null, n: 0, expected: 1, attempted: 1, missing: 1, notes: ['Observer interruption invalidated wall clock.'] });
    run.ingest_coverage = { submitted: 20000, accepted: 20000, rejected: 0, rejected_documents: [] };
    run.ingest_completion = { status: 'verified_complete_after_resume', verified_searchable: true, canonical_timing_valid: false, artifact: 'runtime/test/ingest-resume.json', resume_evidence: { submitted: 20000, accepted: 20000, rejected: 0, documents: 20000, native_inventory_verified: true, native_inventory_count: 20000, late_probe_verified: true } };

    data.variants!['parallel-partitions'] = { label: 'PipesHub parallel scheduling', headline: true, systems: ['pipeshub'], reranker: null, description: run.headline_note, source_url: 'https://example.test/config' };
    data.systems.push(run);
    const parsed = parseResults(data);
    const representative = headlineSystems(parsed).find(system => system.id === 'pipeshub')!;
    assert.equal(representative.variant, 'parallel-partitions');
    assert.equal(representative.metrics.ingest_wall_s.value, null);
    assert(renderMetric(parsed, key).includes('data-system="pipeshub@parallel-partitions"'));
    assert(renderScoreboard(parsed).includes('20,000 / 20,000 accepted'));
    assert(comparisonScope(parsed).includes('ingest time n/e'));
    assert(!comparisonScope(parsed).includes('PipesHub: 20k run not completed'));
    const story = renderStories(parsed);
    if (key === 'first_ask_c1_p50_ms') { assert(story.includes('retrieval: n/e')); assert(!story.includes('retrieval and new-question C1 latency: n/e')); }
    assert.equal(headlineSystems(parsed).filter(system => system.id === 'pipeshub').length, 1);
    run.ingest_completion.resume_evidence.late_probe_verified = false as true;
    assert.throws(() => parseResults(data), /verified inventory and late probe/);
    run.ingest_completion.resume_evidence.late_probe_verified = true;
    run.ingest_completion.resume_evidence.native_inventory_verified = false as true;
    assert.throws(() => parseResults(data), /verified inventory and late probe/);
  }
});
test('ordinary complete and unverified ingest states reconcile without gating other metrics', () => {
  const data = structuredClone(fixture), run = data.systems[0];
  run.ingest_coverage = { submitted: 20000, accepted: 19999, rejected: 1, rejected_documents: ['rejected'], searchable: 19999, pending: 0, pending_documents: [] };
  for (const key of ['ingest_wall_s', 'ingest_docs_per_min']) Object.assign(run.metrics[key], { status: 'measured', value: 100, n: 1, expected: 1, attempted: 1, missing: 0, sample_ids: ['ingest'] });
  run.ingest_completion = { status: 'verified_complete', canonical_timing_valid: true, artifact: 'runtime/test/ingest.json', verified_inventory: true, verified_searchable: true, all_accepted_searchable: true, searchable: 19999, pending: 0, resume_evidence: null };
  assert.equal(parseResults(data).systems[0].ingest_completion?.status, 'verified_complete');
  for (const mutate of [
    (state: Results['systems'][number]) => { assert(state.ingest_completion?.status === 'verified_complete'); state.ingest_completion.searchable = 20000; },
    (state: Results['systems'][number]) => { assert(state.ingest_completion); Object.assign(state.ingest_completion, { canonical_timing_valid: false }); },
    (state: Results['systems'][number]) => { state.metrics.ingest_wall_s.value = null; state.metrics.ingest_wall_s.status = 'n/e'; },
  ]) {
    const bad = structuredClone(data); mutate(bad.systems[0]);
    assert.throws(() => parseResults(bad), /completion|searchability|timing/);
  }
  run.ingest_completion = { status: 'not_verified', canonical_timing_valid: false, artifact: 'runtime/test/ingest.json', verified_inventory: false, verified_searchable: false, all_accepted_searchable: false, searchable: null, pending: null, resume_evidence: null };
  for (const key of ['ingest_wall_s', 'ingest_docs_per_min']) Object.assign(run.metrics[key], { status: 'n/e', value: null, n: 0, expected: 0, attempted: 0, missing: 0, sample_ids: [] });
  run.metrics.first_ask_c8_p50_ms = { status: 'measured', value: 10, unit: 'ms', n: 1, expected: 1, attempted: 1, missing: 0 };
  assert.equal(parseResults(data).systems[0].metrics.first_ask_c8_p50_ms.status, 'measured');
  run.ingest_completion.verified_searchable = true as false;
  assert.throws(() => parseResults(data), /unverified completion/);
});
test('partial input coverage retains measured search scores and discloses pending documents', () => {
  const data = structuredClone(fixture);
  const run = { ...structuredClone(data.systems.find(system => system.id === 'pipeshub')!), variant: 'parallel-partitions', headline_note: 'Non-default parallel scheduling; partial searchability disclosed.', notes: ['Same question set and gold; pending duplicate copies are not excluded from scoring.'] };
  const pending = ['pending-duplicate-a', 'pending-duplicate-b'];
  run.ingest_coverage = { submitted: 20000, accepted: 20000, rejected: 0, rejected_documents: [], searchable: 19998, pending: 2, pending_documents: pending };
  run.ingest_completion = { status: 'verified_partial_coverage', verified_inventory: true, verified_searchable: false, all_accepted_searchable: false, canonical_timing_valid: false, searchable: 19998, pending: 2, artifact: 'runtime/test/ingest-resume.json', resume_evidence: { submitted: 20000, accepted: 20000, rejected: 0, documents: 19998, searchable: 19998, pending: 2, pending_documents: pending, native_inventory_verified: true, native_inventory_count: 19998, late_probe_verified: true, verified_coverage_monotonic: 1000 } };
  for (const key of ['ingest_wall_s', 'ingest_docs_per_min']) Object.assign(run.metrics[key], { status: 'n/e', value: null, n: 0, expected: 1, attempted: 1, missing: 1 });
  run.metrics.recall_at_10 = { status: 'measured', value: .9, unit: 'fraction', n: 1, expected: 1, attempted: 1, missing: 0, sample_ids: ['same-question'], ci95: [.8, 1] };
  run.metrics.first_ask_c1_p50_ms = { status: 'measured', value: 100, unit: 'ms', n: 1, expected: 1, attempted: 1, missing: 0 };
  for (const [key, value] of [['ingest_coverage_fraction', 1], ['searchable_coverage_fraction', .9999]] as const) {
    data.website.metrics[key] = { key, label: key, unit: 'fraction', group: 'ingest', direction: 'higher_is_better', tier: 'primary', ci: null };
    run.metrics[key] = { status: 'measured', value, unit: 'fraction', n: 1, expected: 1, attempted: 1, missing: 0 };
  }
  data.variants!['parallel-partitions'] = { label: 'PipesHub parallel scheduling', headline: true, systems: ['pipeshub'], reranker: null, description: run.headline_note, source_url: 'https://example.test/config' };
  data.systems.push(run);
  run.background_activity = { enrichment_active: true, boundaries: [{ phase: 'measurement', boundary: 'start', completed: 10, pending: 19990, failed: 0 }, { phase: 'measurement', boundary: 'end', completed: 20, pending: 19980, failed: 0 }] };
  const parsed = parseResults(data);
  const selected = headlineSystems(parsed).find(system => system.id === 'pipeshub')!;
  assert.equal(selected.variant, 'parallel-partitions');
  assert.equal(selected.metrics.recall_at_10.status, 'measured');
  assert.deepEqual(selected.metrics.recall_at_10.sample_ids, ['same-question']);
  assert.equal(selected.metrics.ingest_wall_s.value, null);
  assert.equal(selected.metrics.ingest_coverage_fraction.value, 1);
  assert.equal(selected.metrics.searchable_coverage_fraction.value, .9999);
  for (const html of [renderScoreboard(parsed), renderMetric(parsed, 'recall_at_10'), renderMetric(parsed, 'first_ask_c1_p50_ms')]) {
    assert(html.includes('Partial searchability'));
    assert(html.includes('Background document enrichment active'));
    assert(html.includes('Enrichment completed: 10 → 20'));
    assert(html.includes('99.99% searchable versus 100.0% accepted'));
    assert(html.includes('19,998 / 20,000 submitted; 2 pending'));
  }
  assert(comparisonScope(parsed).includes('19,998 / 20,000 searchable (2 pending)'));
  assert(comparisonScope(parsed).includes('ingest time n/e'));
  for (const mutate of [
    (system: (typeof fixture.systems)[number]) => { system.ingest_coverage!.pending = 3; },
    (system: (typeof fixture.systems)[number]) => { system.ingest_coverage!.pending_documents = []; },
    (system: (typeof fixture.systems)[number]) => { system.ingest_completion!.resume_evidence!.native_inventory_count = 20000; },
    (system: (typeof fixture.systems)[number]) => { system.ingest_completion!.resume_evidence!.documents = 20000; },
    (system: (typeof fixture.systems)[number]) => { system.ingest_completion!.verified_searchable = true; },
    (system: (typeof fixture.systems)[number]) => { system.ingest_completion!.resume_evidence!.late_probe_verified = false as true; },
    (system: (typeof fixture.systems)[number]) => { system.metrics.ingest_wall_s = { status: 'measured', value: 1, unit: 'seconds', n: 1, expected: 1, attempted: 1, missing: 0 }; },
  ]) {
    const bad = structuredClone(data);
    mutate(bad.systems.find(system => system.variant === 'parallel-partitions')!);
    assert.throws(() => parseResults(bad), /searchability|pending|searchable|late probe|canonical ingest timing/);
  }
});
test('coverage fractions use independent counts while retrieval scores require paired sample identities', () => {
  const data = structuredClone(fixture);
  const key = 'searchable_coverage_fraction';
  data.website.metrics[key] = { key, label: 'Searchable coverage', unit: 'fraction', group: 'ingest', direction: 'higher_is_better', ci: null };
  const source = readFileSync('src/compare.js', 'utf8');
  const start = source.indexOf('  function equalValue('), end = source.indexOf('  function update()', start);
  const rank = new Function('results', source.slice(start, end) + '\nreturn ratios;')(data);
  function cell(value: number) {
    const classes = new Set();
    const classList = { remove: (name: string) => classes.delete(name), add: (name: string) => classes.add(name), contains: (name: string) => classes.has(name), toggle: (name: string, on: boolean) => on ? classes.add(name) : classes.delete(name) };
    const label = { textContent: '', classList };
    return { dataset: { value: String(value), status: 'measured', n: '1', expected: '1', attempted: '1', missing: '0', samples: '' }, classList, querySelector: () => label, style: { removeProperty() {}, setProperty() {} }, label, classes };
  }
  const cells = [cell(1), cell(.9999)];
  rank(cells, 'higher_is_better', true, 'fraction', key);
  assert.equal(cells[0].label.textContent, 'best');
  assert(cells[0].classes.has('is-best'));
  data.website.metrics[key].group = 'retrieval';
  rank(cells, 'higher_is_better', true, 'fraction', key);
  assert.equal(cells[0].classes.has('is-best'), false, 'Unpaired retrieval observations must not establish a winner');
});
test('experimental Lambda measurements remain optional notebook variants', () => {
  const data = structuredClone(fixture);
  const extra = { ...structuredClone(data.systems[0]), variant: 'lambda-reader', notes: ['Real 20k experiment; cold starts retained.'] };
  extra.metrics.recall_at_10 = { status: 'measured', value: .95, unit: 'fraction', n: 1, expected: 1, attempted: 1, missing: 0, sample_ids: ['a'], ci95: [.9, 1] };
  data.systems.push(extra);
  data.variants!['lambda-reader'] = { label: 'fyrr on AWS Lambda, 4096 MiB', headline: false, experimental: true, reranker: null, systems: ['fyrr'], description: 'Real 20k experiment; cold starts retained.' };
  const parsed = parseResults(data);
  const controls = renderControls(parsed);
  assert(controls.includes('fyrr on AWS Lambda, 4096 MiB · experimental'));
  assert(!controls.includes('value="fyrr@lambda-reader" checked'));
  assert(!renderScoreboard(parsed).includes('fyrr@lambda-reader'));
  assert(!renderStories(parsed).includes('fyrr@lambda-reader'));
  assert(renderSections(parsed).includes('fyrr@lambda-reader'));
  assert(renderSections(parsed).includes('Real 20k experiment; cold starts retained.'));
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
test('private rows and provenance cannot enter public views or imported JSON', () => {
  for (const mutate of [
    (data: typeof fixture) => { data.systems.push({ ...structuredClone(data.systems[0]), variant: 'jev-rerank' }); },
    (data: typeof fixture) => { data.variants!['jev-rerank'] = { label: 'Private reranker', headline: false, reranker: null }; },
    (data: typeof fixture) => { data.protocol.provider_pricing = { jev: { model: 'jev-1.14.0' } }; },
    (data: typeof fixture) => { data.protocol.prompt_hashes = { jev_proxy: 'private-test-hash' }; },
    (data: typeof fixture) => { data.systems[0].version.commit = 'c2a34a3'; },
  ]) {
    const data = structuredClone(fixture);
    mutate(data);
    assert.throws(() => parseResults(data), /private benchmark data/);
  }
  const dir = mkdtempSync(join(tmpdir(), 'fyrr-site-private-'));
  try {
    mkdirSync(join(dir, 'data'));
    const canonical = join(dir, 'data/compare-results.json');
    const before = JSON.stringify(fixture);
    writeFileSync(canonical, before);
    const incoming = structuredClone(fixture);
    incoming.protocol.proxy_metadata = { variant: 'jev-rerank' };
    const file = join(dir, 'private.json');
    writeFileSync(file, JSON.stringify(incoming));
    const child = spawnSync(process.execPath, [join(process.cwd(), 'scripts/import-compare.ts'), file], { cwd: dir, encoding: 'utf8' });
    assert.notEqual(child.status, 0);
    assert.match(child.stderr, /private benchmark data/);
    assert.equal(readFileSync(canonical, 'utf8'), before);
    assert(!/\bjev\b|c2a34a3/i.test(JSON.stringify({ ...fixture, website: { ...fixture.website, lambda_projection: undefined } })));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
test('Lambda projection retains every cold sample and prices only its separate synthetic workload', () => {
  const trial = fixture.website.lambda_projection!;
  assert(trial);
  parseResults(fixture);
  const warm = lambdaProjectionCost(trial, 100000, 0);
  const cold = lambdaProjectionCost(trial, 100000, .01);
  assert(Math.abs(warm.total_usd - .3310250065911755) < 1e-12);
  assert(Math.abs(cold.total_usd - .44184185189117553) < 1e-12);
  assert(cold.total_usd > warm.total_usd);
  const html = renderLambdaProjection(fixture);
  for (const text of ['134 ms', '289 ms', '2.198 s', '2.305 s', '35.756 s', 'Experimental / separate cost projection', '/docs/lambda']) assert(html.includes(text));
  assert(!html.includes('data-metric='));
  assert(html.includes('Synthetic embedding and identity services'));
  const changed = structuredClone(fixture);
  changed.website.lambda_projection!.prices.lambda_gb_second_usd = 0;
  assert.equal(headlineVerdict(changed), headlineVerdict(fixture));
  assert.equal(renderScoreboard(changed), renderScoreboard(fixture));
});
test('Lambda trial provenance exception cannot admit old comparison rows or hide cold observations', () => {
  const oldRow = structuredClone(fixture);
  oldRow.systems[0].version.commit = oldRow.website.lambda_projection!.reader_commit;
  assert.throws(() => parseResults(oldRow), /private benchmark data/);
  const relabelled = structuredClone(fixture);
  relabelled.website.lambda_projection!.documents = 20000;
  assert.throws(() => parseResults(relabelled), /private benchmark data|Lambda trial scope/);
  const missingCold = structuredClone(fixture);
  missingCold.website.lambda_projection!.cold_client_ms = [2198, 2305];
  assert.throws(() => parseResults(missingCold), /Lambda projection workload/);
});
test('answer scoring and its protocol or artifact metadata cannot enter public data', () => {
  for (const mutate of [
    (data: typeof fixture) => { data.systems[0].metrics.answer_correctness = { status: 'n/e', value: null, unit: 'fraction', n: 0 }; },
    (data: typeof fixture) => { data.protocol.judge = { model: 'private-test-model' }; },
    (data: typeof fixture) => { data.protocol.secondary_execution = {}; },
    (data: typeof fixture) => { Object.assign(data.systems[0], { source_slices: { example: { metrics: { native_answer_completeness: {} } } } }); },
    (data: typeof fixture) => { data.details = [{ id: 'a', label: 'a', kind: 'question', metric: 'answer_completeness', results: {} }]; },
    (data: typeof fixture) => { data.systems[0].artifacts = { outputs: 'raw/answers.jsonl' }; },
  ]) {
    const data = structuredClone(fixture);
    mutate(data);
    assert.throws(() => parseResults(data), /private answer/);
  }
  assert(!renderSections(fixture).includes('id="quality"'));
  assert(!Object.keys(fixture.website.metrics).some(key => /^(?:answer_|native_answer)/.test(key)));
});
test('import retains partial C8 measurements and refusals despite conservative readiness flags', () => {
  const dir = mkdtempSync(join(tmpdir(), 'fyrr-site-partial-'));
  try {
    mkdirSync(join(dir, 'data'));
    const canonical = join(dir, 'data/compare-results.json');
    writeFileSync(canonical, JSON.stringify(fixture));
    const incoming = { ...structuredClone(fixture), publication_ready_primary: false };
    const run = { ...incoming.systems[0], publication_ready_primary: false, status: 'partial', errors: [{ code: 'busy', count: 50 }], notes: ['C8: 200 valid responses; 50 explicit busy refusals.'] };
    run.metrics.first_ask_c8_p50_ms = { status: 'partial', value: 100, unit: 'ms', n: 200, attempted: 250, expected: 250, missing: 50, ci95: [90, 110] };
    run.metrics.first_ask_c8_failure_rate = { status: 'measured', value: .2, unit: 'fraction', n: 250, attempted: 250, expected: 250, missing: 0, ci95: [.15, .25] };
    incoming.website.metrics.first_ask_c8_failure_rate = { key: 'first_ask_c8_failure_rate', label: 'First-ask failures at concurrency 8', unit: 'fraction', group: 'speed', direction: 'lower_is_better', ci: null, tier: 'primary' };
    incoming.systems = [run];
    const file = join(dir, 'partial.json');
    writeFileSync(file, JSON.stringify(incoming));
    const child = spawnSync(process.execPath, [join(process.cwd(), 'scripts/import-compare.ts'), file], { cwd: dir, encoding: 'utf8' });
    assert.equal(child.status, 0, child.stderr);
    const imported = JSON.parse(readFileSync(canonical, 'utf8'));
    assert.equal(imported.draft, true);
    assert.equal(imported.publication_ready_primary, false);
    assert.equal(imported.systems.length, fixture.systems.length);
    const actual = imported.systems.find((system: { id: string; variant?: string }) => system.id === run.id && (system.variant ?? 'default') === (run.variant ?? 'default'));
    assert.deepEqual(actual, run);
    assert(renderMetric(parseResults(imported), 'first_ask_c8_p50_ms').includes('partial · unranked'));
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
test('a sole incomplete product attempt stays visible without a fictional default run', () => {
  const dir = mkdtempSync(join(tmpdir(), 'fyrr-site-unavailable-'));
  try {
    mkdirSync(join(dir, 'data'));
    const canonical = join(dir, 'data/compare-results.json');
    const original = structuredClone(fixture.systems.find(system => system.id === 'pipeshub')!);
    const run = { ...structuredClone(original), variant: 'parallel-partitions', date: '2026-11-17T12:00:00Z', version: { version: 'test-version' }, config: { FAIR_SCHEDULING_PARALLEL_PARTITIONS: true }, status: 'partial', headline_note: 'Non-default, source-documented; full 20k interrupted', notes: ['No accepted inventory or shared probe; retrieval and latency are n/e.'] };
    const incoming = structuredClone(fixture);
    incoming.systems = [run];
    incoming.variants!['parallel-partitions'] = { label: 'PipesHub parallel partitions', headline: true, systems: ['pipeshub'], reranker: null, description: run.headline_note, source_url: 'https://example.test/config' };
    const file = join(dir, 'attempt.json');
    writeFileSync(file, JSON.stringify(incoming));
    for (const realDefault of [false, true]) {
      const current = structuredClone(fixture);
      if (realDefault) {
        const actual = current.systems.find(system => system.id === 'pipeshub')!;
        actual.date = '2026-11-17T10:00:00Z'; actual.version = { version: 'actual-prior-run' }; actual.notes = ['Actual prior full-run attempt, interrupted.'];
      }
      current.details = [{ id: 'placeholder', kind: 'category', label: 'placeholder', metric: 'recall_at_10', results: { pipeshub: structuredClone(original.metrics.recall_at_10) } }];
      writeFileSync(canonical, JSON.stringify(current));
      const child = spawnSync(process.execPath, [join(process.cwd(), 'scripts/import-compare.ts'), file], { cwd: dir, encoding: 'utf8' });
      assert.equal(child.status, 0, child.stderr);
      const imported = parseResults(JSON.parse(readFileSync(canonical, 'utf8')));
      const defaults = imported.systems.filter(system => system.id === 'pipeshub' && (!system.variant || system.variant === 'default'));
      assert.equal(defaults.length, realDefault ? 1 : 0);
      assert.equal(headlineSystems(imported).filter(system => system.id === 'pipeshub').length, 1);
      if (!realDefault) {
        const board = renderScoreboard(imported);
        assert(board.includes('data-system="pipeshub@parallel-partitions"'));
        assert(board.includes('No accepted inventory or shared probe'));
        assert.equal(imported.details?.some(row => (!row.variant || row.variant === 'default') && row.results.pipeshub), false);
      }
    }
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
