import { readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';

export type Metric = {
  status: 'measured' | 'partial' | 'n/e' | 'invalid';
  value: number | null;
  unit: string;
  n: number;
  attempted?: number;
  missing?: number;
  expected?: number;
  sample_ids?: string[];
  ci95?: [number, number] | null;
  method?: string;
  artifact?: string;
  notes?: string[];
};
type Group = { id: string; number: string; label: string; title: string; description: string };
type Product = { type: string; pros: string[]; cons: string[]; sources: { label: string; url: string }[] };
type Feature = { label: string; cells: Record<string, { text: string; sources: { label: string; url: string }[] }> };
export type Results = {
  schema_version: '1.1';
  draft: boolean;
  synthetic_design_preview?: boolean;
  variants?: Record<string, { label: string; headline: boolean; reranker: Record<string, unknown> | null }>;
  details?: { id: string; label: string; kind: 'category' | 'question'; metric: string; variant?: string; results: Record<string, Metric> }[];
  benchmark: Record<string, unknown>;
  protocol: Record<string, unknown>;
  systems: {
    id: string; name: string; variant?: string; version: Record<string, unknown>; config: Record<string, unknown>;
    date: string; status?: string; metrics: Record<string, Metric>; footprint: Record<string, unknown>;
    notes: string[]; artifacts?: Record<string, unknown>;
  }[];
  website: {
    headline_metric: string; groups: Group[];
    metrics: Record<string, { key: string; label: string; unit: string; group: string; direction: 'higher_is_better' | 'lower_is_better'; ci: Record<string, unknown> | null; tier?: 'primary' | 'secondary'; scope?: string; description?: string; comparable?: boolean; stage_of?: string; stage_order?: number }>;
    products: Record<string, Product>;
    features?: Feature[];
    variant_note?: string;
    overview_metrics?: string[];
    overview_labels?: Record<string, string>;
    stories?: { id: string; title: string; description: string; group: string; metrics: string[] }[];
    methodology: { label: string; text: string; url?: string }[];
  };
};

const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const strings = (value: unknown): value is string[] => Array.isArray(value) && value.every(item => typeof item === 'string');
const count = (value: unknown) => Number.isInteger(value) && Number(value) >= 0;
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value);
export const runKey = (system: { id: string; variant?: string }) => !system.variant || system.variant === 'default' ? system.id : `${system.id}@${system.variant}`;
const runLabel = (results: Results, system: Results['systems'][number]) => !system.variant || system.variant === 'default' ? system.name : results.variants?.[system.variant]?.label ?? `${system.name} (${name(system.variant)})`;
function productSystems(results: Results) {
  const products = new Map<string, Results['systems'][number]>();
  for (const system of results.systems) if (!products.has(system.id) || !system.variant || system.variant === 'default') products.set(system.id, system);
  return [...products.values()];
}
function requireField(condition: unknown, name: string): asserts condition {
  if (!condition) throw new Error(`Invalid comparison results: ${name}`);
}
function validateMetric(metric: unknown, key: string): asserts metric is Metric {
  requireField(object(metric) && ['measured', 'partial', 'n/e', 'invalid'].includes(String(metric.status)), `${key} status`);
  requireField(typeof metric.unit === 'string' && count(metric.n), `${key} unit/count`);
  const available = metric.status === 'measured' || metric.status === 'partial';
  requireField(available ? finite(metric.value) && metric.value >= 0 && Number(metric.n) > 0 : metric.value === null, `${key} value must match status`);
  requireField(metric.unit !== 'fraction' || metric.value === null || Number(metric.value) <= 1, `${key} fraction range`);
  if (metric.attempted !== undefined) requireField(count(metric.attempted) && Number(metric.attempted) >= Number(metric.n), `${key} attempted count`);
  if (metric.expected !== undefined) requireField(count(metric.expected) && Number(metric.expected) >= Number(metric.n), `${key} expected count`);
  if (metric.missing !== undefined) {
    requireField(count(metric.missing), `${key} missing count`);
    const denominator = metric.expected ?? metric.attempted;
    if (denominator !== undefined) requireField(Number(metric.n) + Number(metric.missing) === denominator, `${key} counts must reconcile`);
  }
  if (metric.ci95 != null) {
    const ci = metric.ci95;
    requireField(available && Array.isArray(ci) && ci.length === 2 && ci.every(finite) && ci[0] >= 0 && ci[0] <= Number(metric.value) && ci[1] >= Number(metric.value), `${key} confidence interval`);
    requireField(metric.unit !== 'fraction' || ci[1] <= 1, `${key} confidence interval range`);
  }
  if (metric.notes !== undefined) requireField(strings(metric.notes), `${key} notes`);
  if (metric.sample_ids !== undefined) requireField(strings(metric.sample_ids) && new Set(metric.sample_ids).size === metric.sample_ids.length && metric.sample_ids.length === metric.n, `${key} sample IDs`);
}

export function parseResults(data: unknown): Results {
  requireField(object(data) && data.schema_version === '1.1', 'schema_version must be 1.1');
  requireField(typeof data.draft === 'boolean', 'explicit draft flag required');
  requireField(object(data.benchmark) && typeof data.benchmark.id === 'string', 'benchmark');
  if (data.synthetic_design_preview) requireField(data.benchmark.publishable === false, 'synthetic preview must set benchmark.publishable to false');
  requireField(object(data.protocol), 'protocol');
  requireField(Array.isArray(data.systems) && data.systems.length > 0, 'systems');
  const ids = new Set<string>();
  const runs = new Set<string>();
  for (const system of data.systems) {
    requireField(object(system) && typeof system.id === 'string' && /^[a-z][a-z0-9_-]*$/.test(system.id), 'system id');
    requireField(system.variant === undefined || typeof system.variant === 'string' && /^[a-z][a-z0-9_-]*$/.test(system.variant), 'variant id');
    const key = runKey(system as Results['systems'][number]);
    requireField(!runs.has(key), 'unique product/variant pair');
    runs.add(key);
    ids.add(system.id);
    requireField(typeof system.name === 'string' && typeof system.date === 'string' && object(system.version) && object(system.config) && object(system.metrics) && object(system.footprint) && strings(system.notes), `${system.id} metadata`);
    for (const [key, metric] of Object.entries(system.metrics)) {
      validateMetric(metric, key);
    }
  }
  const w = data.website;
  requireField(object(w) && typeof w.headline_metric === 'string' && Array.isArray(w.groups) && object(w.metrics) && object(w.products) && Array.isArray(w.methodology), 'website presentation metadata');
  const groups = new Set<string>();
  for (const group of w.groups) {
    requireField(object(group) && ['id', 'number', 'label', 'title', 'description'].every(key => typeof group[key] === 'string'), 'group');
    requireField(/^[a-z][a-z0-9_-]*$/.test(String(group.id)) && !groups.has(String(group.id)), 'unique group id');
    groups.add(String(group.id));
  }
  for (const [key, meta] of Object.entries(w.metrics)) requireField(object(meta) && meta.key === key && typeof meta.label === 'string' && typeof meta.unit === 'string' && (groups.has(String(meta.group)) || meta.group === 'cost') && ['higher_is_better', 'lower_is_better'].includes(String(meta.direction)) && (meta.ci === null || object(meta.ci)) && (meta.scope === undefined || typeof meta.scope === 'string') && (meta.comparable === undefined || typeof meta.comparable === 'boolean') && (meta.description === undefined || typeof meta.description === 'string'), 'metric presentation metadata');
  for (const meta of Object.values(w.metrics)) {
    if (object(meta) && meta.stage_of !== undefined) requireField(meta.stage_of === 'ingest_wall_s' && meta.unit === 'seconds' && meta.group === 'ingest', 'ingest stages must be elapsed seconds');
    if (object(meta) && meta.stage_order !== undefined) requireField(count(meta.stage_order), 'ingest stage order');
  }
  for (const product of Object.values(w.products)) {
    requireField(object(product) && typeof product.type === 'string' && strings(product.pros) && strings(product.cons) && Array.isArray(product.sources), 'product presentation metadata');
    for (const source of product.sources) requireField(object(source) && typeof source.label === 'string' && typeof source.url === 'string', 'product source');
  }
  for (const item of w.methodology) requireField(object(item) && typeof item.label === 'string' && typeof item.text === 'string' && (item.url === undefined || typeof item.url === 'string'), 'methodology');
  if (w.overview_metrics !== undefined) requireField(strings(w.overview_metrics) && w.overview_metrics.length > 0 && w.overview_metrics.every(key => object(w.metrics) && object(w.metrics[key])), 'overview metric keys');
  if (w.overview_labels !== undefined) requireField(object(w.overview_labels) && Object.values(w.overview_labels).every(label => typeof label === 'string'), 'overview labels');
  if (w.stories !== undefined) {
    requireField(Array.isArray(w.stories), 'stories');
    for (const story of w.stories) requireField(object(story) && typeof story.id === 'string' && /^[a-z][a-z0-9_-]*$/.test(story.id) && typeof story.title === 'string' && typeof story.description === 'string' && typeof story.group === 'string' && groups.has(story.group) && strings(story.metrics) && story.metrics.length > 0 && story.metrics.every(key => object(w.metrics) && object(w.metrics[key])), 'story metadata');
  }
  if (w.features !== undefined) {
    requireField(Array.isArray(w.features), 'features');
    for (const feature of w.features) {
      requireField(object(feature) && typeof feature.label === 'string' && object(feature.cells), 'feature row');
      for (const cell of Object.values(feature.cells)) requireField(object(cell) && typeof cell.text === 'string' && Array.isArray(cell.sources) && cell.sources.every(source => object(source) && typeof source.label === 'string' && typeof source.url === 'string'), 'feature cell');
    }
  }
  if (data.variants !== undefined) {
    requireField(object(data.variants), 'variants catalogue');
    for (const [key, variant] of Object.entries(data.variants)) requireField(/^[a-z][a-z0-9_-]*$/.test(key) && object(variant) && typeof variant.label === 'string' && typeof variant.headline === 'boolean' && (variant.reranker === null || object(variant.reranker)), 'variant metadata');
  }
  const keys = new Set(data.systems.flatMap(system => Object.keys(system.metrics)));
  requireField(keys.has(w.headline_metric), 'headline metric must exist');
  for (const key of keys) {
    const units = new Set(data.systems.map(system => system.metrics[key]?.unit).filter(Boolean));
    requireField(units.size <= 1, `${key} must use the same unit across systems`);
    if (object(w.metrics[key])) requireField(!units.size || units.has(String(w.metrics[key].unit)), `${key} catalogue unit must match measurements`);
  }
  const results = data as Results;
  if (!results.draft) {
    requireField(typeof data.benchmark.freeze_sha256 === 'string' && /^[a-f0-9]{64}$/.test(data.benchmark.freeze_sha256) && typeof data.benchmark.protocol_sha256 === 'string' && /^[a-f0-9]{64}$/.test(data.benchmark.protocol_sha256), 'published results require freeze/protocol hashes');
    requireField(results.systems.some(system => Object.values(system.metrics).some(metric => metric.status === 'measured')), 'published results require measurements');
    for (const system of results.systems) {
      if (Object.values(system.metrics).some(metric => metric.status === 'measured')) requireField(Object.keys(system.version).length && Object.keys(system.config).length && !Number.isNaN(Date.parse(system.date)), `${system.id} measured results require version/config/date`);
    }
  }
  if (results.details !== undefined) {
    requireField(Array.isArray(results.details), 'details');
    for (const row of results.details) {
      requireField(object(row) && typeof row.id === 'string' && typeof row.label === 'string' && ['category', 'question'].includes(row.kind) && typeof row.metric === 'string' && object(row.results), 'detail row');
      // Reuse the metric boundary for detailed cells, including counts and CIs.
      for (const [system, metric] of Object.entries(row.results)) {
        requireField(ids.has(system), 'detail result system');
        validateMetric(metric, row.metric);
        requireField(!w.metrics[row.metric] || metric.unit === results.website.metrics[row.metric].unit, "detail metric unit");
      }
    }
  }
  return results;
}

export const resultsPath = 'data/compare-results.json';
export async function readResults() {
  const preview = process.env.COMPARE_PREVIEW_RESULTS;
  if (preview && process.env.SITE_ALLOW_DRAFT_COMPARE !== '1') throw new Error('Alternate comparison fixtures are local preview only.');
  return parseResults(JSON.parse(await readFile(preview ?? resultsPath, 'utf8')));
}
export function assertPublishable(results: Results) {
  if (results.synthetic_design_preview) throw new Error('Synthetic design preview can never be published.');
  if (results.benchmark.publishable === false || results.systems.some(system => system.config.smoke_only === true)) throw new Error('Smoke or nonpublishable datasets cannot be published.');
  if (results.draft) throw new Error('Comparison contains preliminary / placeholder data. Production build and deploy are blocked. Use bun run build:preview for local review.');
}

export const escape = (value: unknown) => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]!));
const link = (url: string) => {
  requireField(/^(?:https:\/\/|\/(?!\/))/.test(url), `unsafe link ${url}`);
  return escape(url);
};
const name = (key: string) => key.replaceAll('_', ' ').replace(/^./, char => char.toUpperCase());
export const metricKeys = (results: Results) => [...new Set(results.systems.flatMap(system => Object.keys(system.metrics)))];
export function format(value: number, unit: string) {
  if (unit === 'fraction') return `${(value * 100).toFixed(1)}%`;
  if (unit === 'bytes') {
    const scale = value >= 1024 ** 3 ? 1024 ** 3 : value >= 1024 ** 2 ? 1024 ** 2 : value >= 1024 ? 1024 : 1;
    return `${(value / scale).toLocaleString('en', { maximumFractionDigits: 1 })} ${scale === 1024 ** 3 ? 'GiB' : scale === 1024 ** 2 ? 'MiB' : scale === 1024 ? 'KiB' : 'B'}`;
  }
  const number = value.toLocaleString('en', value > 0 && value < .01 ? { maximumSignificantDigits: 2 } : { maximumFractionDigits: 2 });
  return unit.startsWith('USD') ? `$${number}` : number;
}

export function relativeValue(value: number, best: number, direction: string) {
  if (value === best) return 1;
  const denominator = direction === 'higher_is_better' ? value : best;
  return denominator === 0 ? Infinity : (direction === 'higher_is_better' ? best : value) / denominator;
}
function sampleSet(metric: Metric | undefined) {
  return metric?.sample_ids?.length ? createHash('sha256').update([...metric.sample_ids].sort().join('\n')).digest('hex') : '';
}
function ingestRate(metric: Metric | undefined) {
  if (metric?.value == null) return metric?.status === 'invalid' ? 'invalid docs/min' : 'n/e docs/min';
  return `${format(metric.value, 'docs/min')} docs/min${metric.status === 'partial' ? ' *' : ''}`;
}

function axisScale(value: number, unit: string) {
  if (unit === 'fraction') return { max: 1, step: .25 };
  if (!value) return { max: 0, step: 0 };
  const scale = unit === 'bytes' ? 1024 ** Math.max(0, Math.floor(Math.log(value) / Math.log(1024))) : 1;
  const rough = value / scale / 4;
  const power = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map(n => n * power).find(n => n >= rough)! * scale;
  return { max: Math.ceil(value / step) * step, step };
}

const canCompare = (meta: Results['website']['metrics'][string] | undefined) => meta?.comparable !== false && meta?.scope !== 'fyrr-only';

export function headlineVerdict(results: Results) {
  results = { ...results, systems: results.systems.filter(system => !system.variant || system.variant === 'default') };
  const key = results.website.headline_metric;
  const meta = results.website.metrics[key];
  if (!canCompare(meta)) return `${meta?.label ?? 'This metric'} is a diagnostic; it does not establish a product ranking.`;
  const cells = results.systems.map(system => system.metrics[key]);
  const complete = cells.every(metric => metric?.status === 'measured' && metric.value !== null && metric.missing === 0 && metric.expected === metric.n && metric.attempted === metric.n && metric.ci95 && metric.sample_ids);
  const sameSamples = complete && new Set(cells.map(metric => [...metric.sample_ids!].sort().join('\n'))).size === 1;
  if (results.draft || !sameSamples || new Set(cells.map(metric => metric.n)).size !== 1 || !meta) {
    return 'Retrieval, search speed, ingest and operating cost are still being measured on the same frozen dataset.';
  }
  const values = cells.map(metric => metric.value!);
  const best = meta.direction === 'higher_is_better' ? Math.max(...values) : Math.min(...values);
  const winners = values.flatMap((value, index) => value === best ? [results.systems[index].name] : []);
  if (winners.length > 1) return `${winners.join(' and ')} tie on observed ${meta.label.toLowerCase()} at ${format(best, cells[0].unit)}; uncertainty and trade-offs are shown below.`;
  return `${winners[0]} has the ${meta.direction === 'higher_is_better' ? 'highest' : 'lowest'} observed ${meta.label.toLowerCase()} at ${format(best, cells[0].unit)}; uncertainty and trade-offs are shown below.`;
}

export function renderMetric(results: Results, key: string, options: { tailKey?: string; axisMax?: number; ingest?: boolean } = {}) {
  const meta = results.website.metrics[key];
  if (meta?.scope === 'fyrr-only') results = { ...results, systems: results.systems.filter(system => system.id === 'fyrr') };
  const cells = results.systems.map(system => system.metrics[key]);
  const unit = cells.find(Boolean)?.unit ?? meta?.unit ?? '';
  const tails = options.tailKey ? results.systems.map(system => system.metrics[options.tailKey!]) : [];
  const available = [...cells, ...tails].filter((metric): metric is Metric & { value: number } => Boolean(metric && metric.value !== null));
  const rawMax = options.axisMax ?? Math.max(0, ...available.map(metric => Math.max(metric.value, metric.ci95?.[1] ?? 0))) * 1.1;
  const axis = axisScale(rawMax || (available.length ? 1 : 0), unit);
  const max = axis.max;
  const pct = (value: number) => max ? `${value / max * 100}%` : '0%';
  const caption = results.draft ? 'Preliminary / placeholder data. No published scores or rankings.' : 'Whiskers show 95% confidence intervals where available. Partial results cannot establish a product win.';
  const title = options.tailKey ? key.startsWith('first_ask_') ? 'First-ask search latency' : 'Repeated-query search latency' : meta?.label ?? name(key);
  // stage_of denotes disjoint submission-to-searchable elapsed segments, excluding setup and parallel work.
  const stages = options.ingest ? Object.values(results.website.metrics).filter(stage => stage.stage_of === key).sort((a, b) => (a.stage_order ?? 0) - (b.stage_order ?? 0)) : [];
  function stageBreakdown(system: Results['systems'][number]) {
    const total = system.metrics[key];
    const exposed = stages.flatMap(stage => {
      const metric = system.metrics[stage.key];
      return metric?.status === 'measured' && metric.value != null ? [{ label: stage.label, value: metric.value }] : [];
    });
    const sum = exposed.reduce((sum, stage) => sum + stage.value, 0);
    if (!exposed.length) return { segments: [], note: 'Stage breakdown n/e; total wall time only.' };
    if (total?.status !== 'measured' || total.value == null || sum > total.value) return { segments: [], note: 'Stages cannot be stacked against this wall time; see individual stage measurements in details.' };
    if (sum < total.value) exposed.push({ label: 'Other / unreported', value: total.value - sum });
    return { segments: exposed, note: exposed.map(stage => `${stage.label}: ${format(stage.value, 'seconds')} s`).join(' · ') };
  }
  function detailsFor(system: Results['systems'][number], metric: Metric | undefined, label: string) {
    if (!metric) return `<li>${escape(runLabel(results, system))} · ${escape(label)}: n/e · metric absent from this run.</li>`;
    return `<li>${escape(runLabel(results, system))} · ${escape(label)}: ${escape(metric.status)} · valid ${metric.n} / attempted ${metric.attempted ?? 'n/e'} · missing ${metric.missing ?? 'n/e'}${metric.ci95 ? ` · 95% CI ${format(metric.ci95[0], unit)}–${format(metric.ci95[1], unit)}` : ' · 95% CI n/e'}${metric.method ? ` · ${escape(metric.method)}` : ''}${metric.notes?.length ? ` · ${escape(metric.notes.join(' '))}` : ''}${metric.artifact ? ` · ${escape(metric.artifact)}` : ''}</li>`;
  }
  const details = results.systems.map((system, index) => detailsFor(system, cells[index], meta?.label ?? key) + (options.tailKey ? detailsFor(system, tails[index], 'p95') : '') + (options.ingest ? `<li>${escape(runLabel(results, system))} · ${escape(stageBreakdown(system).note)}</li>` : '')).join('');
  const rows = results.systems.map((system, index) => {
    const metric = cells[index], tail = tails[index];
    const hasValue = metric?.value !== null && metric?.value !== undefined;
    const value = hasValue ? `${format(metric.value!, unit)}${metric.status === 'partial' ? ' *' : ''}` : metric?.status === 'invalid' ? 'invalid' : 'n/e';
    const tailValue = tail?.value != null ? `${format(tail.value, unit)}${tail.status === 'partial' ? ' *' : ''}` : tail?.status === 'invalid' ? 'invalid' : 'n/e';
    const measured = cells.filter((cell): cell is Metric & { value: number } => cell?.status === 'measured' && cell.value !== null);
    const best = meta?.direction === 'higher_is_better' ? Math.max(...measured.map(cell => cell.value)) : Math.min(...measured.map(cell => cell.value));
    const ratio = metric?.status === 'measured' && metric.value !== null && meta && canCompare(meta) ? relativeValue(metric.value, best, meta.direction) : null;
    const relative = !canCompare(meta) ? 'diagnostic · unranked' : ratio === null ? metric?.status === 'partial' ? 'partial · unranked' : 'n/e comparison' : unit === 'fraction' && meta?.direction === 'higher_is_better' ? metric!.value === best ? 'highest evaluated' : `${((metric!.value! - best) * 100).toFixed(1)} pp` : `×${Number.isFinite(ratio) ? ratio.toFixed(2) : '∞'}${ratio === 1 ? ' evaluated' : ''}`;
    const breakdown = stageBreakdown(system);
    const cost = system.metrics.embedding_cost_usd;
    return `<div class="chart-row${system.id === 'fyrr' ? ' is-fyrr' : ''}" data-system="${escape(runKey(system))}" data-status="${metric?.status ?? 'n/e'}" data-value="${metric?.value ?? ''}" data-n="${metric?.n ?? 0}" data-missing="${metric?.missing ?? ''}" data-attempted="${metric?.attempted ?? ''}" data-expected="${metric?.expected ?? ''}" data-samples="${sampleSet(metric)}" role="listitem">
      <span class="product-name"><a href="/compare/runs/${escape(system.id)}--${escape(system.variant ?? 'default')}.json" title="Download this run JSON">${escape(runLabel(results, system))}</a></span>
      <div class="bar${system.id === 'fyrr' ? ' bar--fyrr' : ''}${hasValue ? '' : ' is-empty'}" aria-hidden="true">
        ${hasValue ? `<i style="--w:${pct(metric.value!)}">${breakdown.segments.map((stage, i) => `<span class="ingest-segment" style="width:${metric.value ? stage.value / metric.value * 100 : 0}%;--stage-shade:${Math.min(i * .18, .72)}" title="${escape(stage.label)}: ${escape(format(stage.value, 'seconds'))} s"></span>`).join('')}</i>${metric.ci95 ? `<b class="whisker" style="--low:${pct(metric.ci95[0])};--high:${pct(metric.ci95[1])}"></b>` : ''}` : ''}
        ${tail?.value != null ? `<b class="tail-tick" style="left:${pct(tail.value)}"></b>${tail.ci95 ? `<b class="whisker whisker--tail" style="--low:${pct(tail.ci95[0])};--high:${pct(tail.ci95[1])}"></b>` : ''}` : ''}
      </div>
      <span class="chart-value"><span class="primary-value">${options.tailKey ? `<span class="sr-only">p50 </span>` : ''}${value}${key === 'ingest_wall_s' && hasValue ? ' s' : ''}</span>${options.tailKey ? `<small class="tail-value">p95 ${tailValue}</small>` : ''}${key === 'ingest_wall_s' ? `<small class="ingest-rate">${escape(ingestRate(system.metrics.ingest_docs_per_min))}</small>` : ''}<small class="relative">${relative}</small></span>
      ${options.ingest ? `<small class="ingest-cost" data-secondary-metric="embedding_cost_usd">Embedding cost: ${cost?.value != null ? `${escape(format(cost.value, cost.unit))}${cost.status === 'partial' ? ' *' : ''}` : cost?.status === 'invalid' ? 'invalid' : 'n/e'}</small>` : ''}
    </div>`;
  }).join('');
  return `<figure class="panel metric-panel rv" data-metric="${escape(key)}" data-direction="${escape(meta?.direction ?? '')}"${options.tailKey ? ` data-companion="${escape(options.tailKey)}"` : ''}>
    <figcaption class="panel__bar"><span>${meta?.scope === 'fyrr-only' ? 'FYRR-only diagnostic' : escape(meta ? results.website.groups.find(group => group.id === (meta.group === 'cost' ? 'footprint' : meta.group))?.label ?? 'Additional metric' : 'Additional metric')}${meta?.tier === 'secondary' ? ' / secondary' : ''}</span><span>${meta?.scope === 'fyrr-only' ? 'No cross-product comparison' : !canCompare(meta) ? 'Diagnostic only · no product ranking' : meta?.direction ? `${meta.direction === 'higher_is_better' ? 'higher' : 'lower'} is better` : 'Reported value'}</span></figcaption>
    <div class="panel__body"><h3 class="metric-title">${escape(title)}</h3>
      <p class="metric-sub">${escape(unit === 'fraction' ? 'Score · full scale 0–100%' : unit === 'bytes' ? 'Memory / storage · scaled to MiB or GiB' : unit)}${available.length ? '' : ' · awaiting measurements'}</p>
      ${meta?.description ? `<p class="metric-sub">${escape(meta.description)}</p>` : ''}
      ${options.tailKey ? '<p class="chart-legend"><span><i></i> p50 bar</span><span><b></b> p95 tick</span><span>Whiskers: 95% CI</span></p>' : ''}
      ${options.ingest ? `<p class="metric-sub">${escape(finite(results.benchmark.documents) ? format(results.benchmark.documents, 'documents') : 'n/e')} documents · fresh index to searchable. Stacks show exposed, disjoint elapsed stages; other time is labelled unreported. Stage breakdowns are in details.</p>` : ''}
      <div class="chart${options.tailKey ? ' chart--latency' : ''}" role="list" aria-label="${escape(title)} by product">${rows}</div>
      ${max ? `<div class="axis" aria-hidden="true">${Array.from({ length: Math.round(max / axis.step) + 1 }, (_, i) => `<span>${unit === 'fraction' ? `${Math.round(i * axis.step * 100)}%` : format(i * axis.step, unit)}</span>`).join('')}</div>` : '<p class="axis-pending">n/e · no measured scale</p>'}
      <p class="chart-caption">${caption}</p>
      <details class="metric-details"><summary>Samples, uncertainty &amp; notes</summary><ul>${details}</ul></details>
    </div></figure>`;
}

function renderMetricSet(results: Results, group: string, keys: string[], prefix = '') {
  const latency = group === 'speed';
  const timing = keys.some(key => key.startsWith('first_ask_')) ? 'first_ask' : 'warm';
  const views = latency ? ['1', '8'].map(concurrency => ({
    key: `${timing}_c${concurrency}_p50_ms`, tailKey: `${timing}_c${concurrency}_p95_ms`, label: `Concurrency ${concurrency}`,
  })) : [...new Set(['recall_at_10', 'ndcg_at_10', 'recall_at_50', ...keys])].filter(key => keys.includes(key)).map(key => ({ key, tailKey: undefined, label: results.website.metrics[key]?.label ?? name(key) }));
  const present = views.filter(view => keys.includes(view.key) || Boolean(view.tailKey && keys.includes(view.tailKey)));
  if (!present.length) return '';
  const max = latency ? Math.max(0, ...results.systems.flatMap(system => keys.flatMap(key => {
    const metric = system.metrics[key];
    return metric?.value != null ? [metric.value, metric.ci95?.[1] ?? 0] : [];
  }))) * 1.1 : undefined;
  return `<div class="metric-set" data-metric-set><div class="metric-tabs" role="tablist" aria-label="${prefix ? `${prefix}metric` : latency ? 'Search concurrency' : 'Retrieval metric'}">${present.map((view, index) => `<button type="button" role="tab" id="tab-${prefix}${escape(view.key)}" aria-controls="view-${prefix}${escape(view.key)}" aria-selected="${index === 0}" tabindex="${index === 0 ? '0' : '-1'}">${escape(view.label)}</button>`).join('')}</div>${present.map((view, index) => `<div role="tabpanel" id="view-${prefix}${escape(view.key)}" aria-labelledby="tab-${prefix}${escape(view.key)}"${index ? ' hidden' : ''}>${renderMetric(results, view.key, { tailKey: view.tailKey, axisMax: max })}</div>`).join('')}</div>`;
}


const architecture = `<aside class="architecture"><div><p class="eyebrow">fyrr / serverless index</p><h3>No database. No cluster.<br>The index lives on S3.</h3><p>Writers publish files. Readers search them. You still run the writer and reader processes, pay for storage and embeddings, and size a private reader mirror. Serverless describes the index, not zero compute or zero cost. <a href="/docs/deployment">Deployment details ↗</a></p></div><div class="topology" aria-label="Writer publishes to S3 index; reader reads from S3"><div class="topology-box"><strong>Writer</strong><small>sync &amp; publish</small></div><span aria-hidden="true">→</span><div class="topology-box bucket"><strong>S3</strong><small>index files</small></div><span aria-hidden="true">→</span><div class="topology-box"><strong>Reader</strong><small>search via MCP</small></div></div></aside>`;

export function renderScoreboard(results: Results) {
  const systems = results.systems.filter(system => !system.variant || system.variant === 'default');
  const keys = results.website.overview_metrics ?? [results.website.headline_metric];
  const scales = new Map<string, number>(keys.map(key => {
    const values = systems.flatMap(system => {
      const metric = system.metrics[key];
      return metric?.value != null ? [metric.value, metric.ci95?.[1] ?? 0] : [];
    });
    return [key, axisScale(Math.max(0, ...values) * 1.1 || (values.length ? 1 : 0), results.website.metrics[key]?.unit ?? '').max];
  }));
  return `<div class="scoreboard rv" style="--score-cols:${keys.length}"><p class="score-intro">Quality means the right documents in the top 10. Default variants only.</p>
    <div class="score-head"><span>System</span>${keys.map(key => `<span data-score-metric="${escape(key)}">${escape(results.website.overview_labels?.[key] ?? results.website.metrics[key]?.label ?? name(key))}</span>`).join('')}</div>
    ${systems.map(system => `<div class="score-row${system.id === 'fyrr' ? ' is-fyrr' : ''}" data-system="${escape(runKey(system))}"><a class="score-product" href="/compare/runs/${escape(system.id)}--default.json">${escape(system.name)} ↗</a>${keys.map(key => {
      const metric = system.metrics[key], max = scales.get(key) ?? 0;
      const unit = metric?.unit ?? results.website.metrics[key]?.unit ?? '';
      const pct = (value: number) => `${max ? value / max * 100 : 0}%`;
      return `<div class="score-cell" data-score-metric="${escape(key)}" data-status="${metric?.status ?? 'n/e'}" data-value="${metric?.value ?? ''}" data-n="${metric?.n ?? 0}" data-expected="${metric?.expected ?? ''}" data-attempted="${metric?.attempted ?? ''}" data-missing="${metric?.missing ?? ''}" data-samples="${sampleSet(metric)}"><span class="score-label">${escape(results.website.overview_labels?.[key] ?? results.website.metrics[key]?.label ?? name(key))}</span><span class="score-value">${metric?.value != null ? format(metric.value, unit) + (unit === 'ms' ? ' ms' : unit === 'seconds' ? ' s' : '') + (metric.status === 'partial' ? ' *' : '') : metric?.status === 'invalid' ? 'invalid' : 'n/e'}</span>${key === 'ingest_wall_s' ? `<small class="score-rate">${escape(ingestRate(system.metrics.ingest_docs_per_min))}</small>` : ''}<div class="bar${system.id === 'fyrr' ? ' bar--fyrr' : ''}${metric?.value != null ? '' : ' is-empty'}" aria-hidden="true">${metric?.value != null ? `<i style="--w:${pct(metric.value)}"></i>${metric.ci95 ? `<b class="whisker" style="--low:${pct(metric.ci95[0])};--high:${pct(metric.ci95[1])}"></b>` : ''}` : ''}</div><small class="relative">${canCompare(results.website.metrics[key]) ? '' : 'diagnostic · unranked'}</small></div>`;
    }).join('')}</div>`).join('')}
    <p class="score-note">n/e = not evaluated. Each column shares a zero-based scale; quality spans 0–100%. <a href="#details">Intervals, samples &amp; assumptions ↘</a></p></div><p class="overview-empty note" hidden>The overview compares default variants. Select a default system to see it.</p>`;
}

export function renderStories(results: Results) {
  const baseline = { ...results, systems: results.systems.filter(system => !system.variant || system.variant === 'default') };
  return (results.website.stories ?? []).map((story, index) => {
    const chart = story.group === 'ingest' ? renderMetric(baseline, story.metrics[0], { ingest: true }) : story.group === 'speed' ? renderMetric(baseline, story.metrics[0], { tailKey: story.metrics[1] }) : story.metrics.length === 1 ? renderMetric(baseline, story.metrics[0]) : renderMetricSet(baseline, story.group, story.metrics, `${story.id}-`);
    return `<section class="section story-section" id="${escape(story.id)}"><div class="wrap"><div class="section-head"><p class="eyebrow">0${index + 1} / ${escape(results.website.groups.find(group => group.id === story.group)?.label ?? story.group)}</p><h2>${escape(story.title)}</h2><p class="sub">${escape(story.description)}</p></div>${story.group === 'footprint' ? architecture : ''}${chart}<a class="text-link" href="#detail-${escape(story.group)}">${escape(results.website.groups.find(group => group.id === story.group)?.label ?? 'Full')} details <span aria-hidden="true">↘</span></a></div></section>`;
  }).join('');
}

export function renderSections(results: Results, prefix = '') {
  const keys = metricKeys(results);
  const unknown = keys.filter(key => !results.website.metrics[key]);
  return [...results.website.groups, ...(unknown.length ? [{ id: 'additional', number: '+', label: 'Additional metrics', title: 'More of the picture.', description: 'Additional measurements reported by the benchmark.' }] : [])].map(group => {
    const selected = group.id === 'additional' ? unknown : keys.filter(key => results.website.metrics[key]?.group === group.id || group.id === 'footprint' && results.website.metrics[key]?.group === 'cost');
    let charts: string;
    if (group.id === 'retrieval') charts = renderMetricSet(results, group.id, selected);
    else if (group.id === 'speed') {
      const timing = /^(?:first_ask|warm)_c[18]_p(?:50|95)_ms$/;
      charts = ['first_ask', 'warm'].map(family => renderMetricSet(results, group.id, selected.filter(key => timing.test(key) && key.startsWith(family)), family === 'warm' ? 'repeat-' : '')).join('')
        + selected.filter(key => !timing.test(key)).map(key => renderMetric(results, key)).join('');
    } else charts = selected.map(key => renderMetric(results, key)).join('');
    return `<section class="section" id="${escape(prefix + group.id)}"><div class="wrap"><div class="section-head"><p class="eyebrow">${escape(group.number)} / ${escape(group.label)}</p><h2>${escape(group.title)}</h2><p class="sub">${escape(group.description)}</p></div><div class="charts">${charts}</div>${group.id === 'footprint' ? renderFootprints(results) : ''}<p class="note">n/e = not evaluated. Invalid = unusable measurement. * = partial; see samples and uncertainty. ${group.id === 'quality' ? 'Shared answer track only. Native answer results, if available, are labelled separately.' : ''}</p></div></section>`;
  }).join('');
}

function renderFootprints(results: Results) {
  return `<details class="run-config"><summary>External dependencies &amp; resource scope</summary>${results.systems.map(system => `<h3>${escape(runLabel(results, system))}</h3>${Object.keys(system.footprint).length ? `<pre>${escape(JSON.stringify(system.footprint, null, 2))}</pre>` : '<p class="note">n/e · tested deployment dependencies, process counts, RSS scope and disk scope pending.</p>'}`).join('')}</details>`;
}
export function renderFeatures(results: Results) {
  if (!results.website.features?.length) return '';
  const products = productSystems(results);
  return `<div class="feature-comparison"><h3>Bring your own models</h3><p class="note">Configuration support is separate from benchmark performance. Sources and release-specific limits are shown for every product.</p><div class="detail-scroll"><table class="detail-table feature-table"><thead><tr><th scope="col">Model choice</th>${products.map(system => `<th scope="col" data-product="${escape(system.id)}">${escape(system.name)}</th>`).join('')}</tr></thead><tbody>${results.website.features.map(row => `<tr><th scope="row">${escape(row.label)}</th>${products.map(system => {
    const cell = row.cells[system.id];
    return `<td data-product="${escape(system.id)}">${escape(cell?.text ?? 'n/e · not reviewed')}<small>${cell?.sources.map(source => `<a href="${link(source.url)}">${escape(source.label)} ↗</a>`).join(' · ') ?? ''}</small></td>`;
  }).join('')}</tr>`).join('')}</tbody></table></div></div>`;
}
export function renderProducts(results: Results) {
  return productSystems(results).map(system => {
    const product = results.website.products[system.id];
    const versionLabel = typeof system.version.version === 'string' ? system.version.version : typeof system.version.commit === 'string' ? system.version.commit.slice(0, 12) : 'n/e';
    return `<article class="product-card${system.id === 'fyrr' ? ' is-fyrr' : ''}"><h3>${escape(system.name)}</h3><p class="product-type">${escape(product?.type ?? 'Product profile pending')}</p><p class="product-version">Run version: ${escape(versionLabel)}</p><h4>Where it fits</h4><ul>${(product?.pros ?? ['n/e · profile has not been reviewed.']).map(text => `<li>${escape(text)}</li>`).join('')}</ul><h4>Trade-offs &amp; limits</h4><ul class="limits">${(product?.cons ?? ['n/e · limitations have not been reviewed.']).map(text => `<li>${escape(text)}</li>`).join('')}</ul>${product?.sources.map(source => `<a class="source-link" href="${link(source.url)}">${escape(source.label)} ↗</a>`).join(' · ') ?? ''}<br><a class="source-link" href="/compare/runs/${escape(system.id)}--${escape(system.variant ?? 'default')}.json">Run JSON: version, date &amp; config ↗</a></article>`;
  }).join('');
}
export function renderControls(results: Results) {
  return `<section class="comparison-controls" aria-label="Customize comparison"><div class="wrap">
    <div class="control-row"><fieldset><legend>Systems</legend><div class="selection-chips">${results.systems.map(system => `<label class="selection-chip"><input type="checkbox" data-select-system value="${escape(runKey(system))}"${!system.variant || system.variant === 'default' ? ' checked' : ''}>${escape(runLabel(results, system))}</label>`).join('')}</div></fieldset>
    <div class="permalink"><input aria-label="Current selection permalink" readonly value=""><button type="button" id="copy-permalink">Copy selection link ↗</button><span class="sr-only" id="copy-status" role="status"></span></div></div>
    ${results.website.variant_note ? `<p class="note">${escape(results.website.variant_note)}</p>` : ''}<details class="metrics-select"><summary>Choose metrics</summary><fieldset><legend class="sr-only">Metrics</legend><div class="selection-chips">${metricKeys(results).map(key => `<label class="selection-chip"><input type="checkbox" data-select-metric value="${escape(key)}"${results.website.overview_metrics?.includes(key) || results.website.metrics[key]?.tier !== 'secondary' || ['answer_correctness', 'answer_completeness'].includes(key) ? ' checked' : ''}>${escape(results.website.metrics[key]?.label ?? name(key))}</label>`).join('')}</div></fieldset></details>
    <p class="note">Bars show absolute values. Scores show the percentage-point gap to the best selected system; time, cost and footprint use ratios (×1.00 best). Partial and missing results are unranked. <span id="selection-status" role="status"></span></p>
  </div></section>`;
}
export function renderDetailView(results: Results) {
  const matrix = new Map(results.details?.map(row => [`${row.kind}/${row.metric}/${row.id}/${row.variant ?? 'default'}`, row]));
  return `<section class="section" id="detail"><div class="wrap"><div class="section-head"><p class="eyebrow">The close-up / question by question</p><h2>Where each<br>system finds its footing.</h2><p class="sub">Compare question types, then inspect individual questions. A row winner requires complete, comparable measurements for every selected system.</p></div>
    <div class="detail-controls"><div class="metric-tabs" role="tablist" aria-label="Detail level"><button type="button" role="tab" id="tab-category" aria-controls="detail-category" aria-selected="true" tabindex="0">Question types</button><button type="button" role="tab" id="tab-question" aria-controls="detail-question" aria-selected="false" tabindex="-1">Individual questions</button></div><label>Metric <select id="detail-metric">${[...new Set([results.website.headline_metric, ...results.details?.map(row => row.metric) ?? []])].map(key => `<option value="${escape(key)}">${escape(results.website.metrics[key]?.label ?? name(key))}</option>`).join('')}</select></label><label>Find <input type="search" id="detail-search" placeholder="Question or type"></label></div>
    ${(['category', 'question'] as const).map(kind => {
      const rows = [...new Map(results.details?.filter(row => row.kind === kind).map(row => [`${row.metric}/${row.id}`, row])).values()];
      return `<div role="tabpanel" id="detail-${kind}" aria-labelledby="tab-${kind}"${kind === 'question' ? ' hidden' : ''}><div class="detail-scroll" tabindex="0" aria-label="${kind === 'category' ? 'Question type' : 'Per-question'} result table"><table class="detail-table"><thead><tr><th scope="col">${kind === 'category' ? 'Question type' : 'Question'}</th>${results.systems.map(system => `<th scope="col" data-system="${escape(runKey(system))}">${escape(runLabel(results, system))}</th>`).join('')}</tr></thead><tbody>${rows.map(row => `<tr data-detail-metric="${escape(row.metric)}" data-direction="${escape(results.website.metrics[row.metric]?.direction ?? '')}"><th scope="row">${escape(row.kind === 'category' ? name(row.label) : row.label)}<small>${escape(results.website.metrics[row.metric]?.label ?? name(row.metric))}</small></th>${results.systems.map(system => {
        const metric = matrix.get(`${row.kind}/${row.metric}/${row.id}/${system.variant ?? 'default'}`)?.results[system.id];
        return `<td data-system="${escape(runKey(system))}" data-status="${metric?.status ?? 'n/e'}" data-value="${metric?.value ?? ''}" data-n="${metric?.n ?? 0}" data-missing="${metric?.missing ?? ''}" data-attempted="${metric?.attempted ?? ''}" data-expected="${metric?.expected ?? ''}" data-samples="${sampleSet(metric)}">${metric?.value != null ? format(metric.value, metric.unit) + (metric.status === 'partial' ? ' *' : '') : metric?.status === 'invalid' ? 'invalid' : 'n/e'}<small class="relative">${canCompare(results.website.metrics[row.metric]) ? 'n/e ratio' : 'diagnostic · unranked'}</small></td>`;
      }).join('')}</tr>`).join('') || `<tr><td colspan="${results.systems.length + 1}">n/e · ${kind === 'category' ? 'Question-type' : 'Per-question'} results have not been evaluated.</td></tr>`}</tbody></table></div></div>`;
    }).join('')}
    <p class="note">A bright cell marks a complete row winner; ties are labelled. Overlapping uncertainty can still make differences inconclusive. Scores show percentage-point gaps; lower-is-better measurements show value / best. A zero baseline may have an infinite ratio; no axis is truncated.</p>
  </div></section>`;
}
export function renderMethodology(results: Results) {
  const { systems, website: _, ...provenance } = results;
  const answer = object(results.protocol.answer) ? results.protocol.answer : {};
  const judge = object(results.protocol.judge) ? results.protocol.judge : {};
  const modelSummary = `Generator: ${answer.model ?? 'n/e'} (${answer.reasoning_effort ?? 'n/e'} reasoning). Judge: ${judge.model ?? 'n/e'} (${judge.reasoning_effort ?? 'n/e'} reasoning). Transport: ${answer.transport ?? answer.backend ?? 'n/e'}. Exact prompts and hashes are in provenance below.`;
  return `<dl class="method-grid">${results.website.methodology.map(item => `<div class="method-item"><dt>${escape(item.label)}</dt><dd>${escape(item.text)}${item.url ? ` <a href="${link(item.url)}">Source ↗</a>` : ''}</dd></div>`).join('')}<div class="method-item"><dt>Answer &amp; judge protocol metadata</dt><dd>${escape(modelSummary)}</dd></div></dl><details class="run-config"><summary>Versions, dates, configurations &amp; provenance</summary><pre>${escape(JSON.stringify({ ...provenance, details: undefined, systems: systems.map(({ metrics: _metrics, footprint: _footprint, ...system }) => ({ ...system, variant: system.variant ?? 'default' })) }, null, 2))}</pre></details><div class="method-links"><a class="btn btn--primary" href="/compare/results.json">Download results JSON ↗</a><a class="btn" href="${link(String(results.benchmark.source_url))}">Upstream benchmark ↗</a></div><details class="run-config"><summary>Add your system / reproduce a run</summary><ol class="contribute-steps"><li>Use the frozen corpus and question selection; verify the manifest hashes.</li><li>Implement the benchmark adapter against the actual product API. Preserve native ranking and return product-selected evidence.</li><li>Run the shared protocol. Record every attempt, missing result, exact version, date, hardware and nonsecret configuration.</li><li>Export one schema 1.1 results JSON per run, including the metric catalogue, confidence intervals, artifacts and detailed rows.</li><li>Contribute the JSON and reproducibility artifacts to the open benchmark repository when its public URL is available. Until then, the downloadable JSON is the local review artifact.</li></ol></details><p class="note">Published by the fyrr team. Product descriptions link to their sources; measurements apply only to the exact configurations recorded above. Open comparison repository: ${results.benchmark.repository_url ? `<a href="${link(String(results.benchmark.repository_url))}">Reproduce &amp; add a system ↗</a>` : 'n/e · not published yet; no public artifact URL is claimed.'}</p>`;
}
