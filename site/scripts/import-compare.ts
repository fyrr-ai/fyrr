import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { parseResults, resultsPath, runKey, validatePublicScope } from '../lib/compare';

const input = process.argv[2];
if (!input) throw new Error('Usage: bun scripts/import-compare.ts <benchmark-results.json>');
let incoming = JSON.parse(await readFile(input, 'utf8'));
if (path.basename(path.dirname(input)) === 'results') {
  const artifactRoot = path.dirname(path.dirname(path.resolve(input))) + '/';
  incoming = JSON.parse(JSON.stringify(incoming).replaceAll(artifactRoot, ''));
}
if (incoming.synthetic_design_preview || incoming.benchmark?.publishable === false || Array.isArray(incoming.systems) && incoming.systems.some((system: { config?: { smoke_only?: boolean } }) => system.config?.smoke_only)) throw new Error('Synthetic, smoke and nonpublishable fixtures cannot be imported.');
const current = parseResults(JSON.parse(await readFile(resultsPath, 'utf8')));
if (incoming.schema_version !== '1.1' || !incoming.website?.metrics || !incoming.protocol || !Array.isArray(incoming.systems)) throw new Error('Expected the benchmark schema 1.1 envelope, protocol and metric catalogue.');
validatePublicScope(incoming);
// Replace an empty site fixture when the product's actual attempt has another
// variant identity. Retain every real measured or interrupted historical run.
const retained = current.systems.filter(system => {
  const placeholder = (!system.variant || system.variant === 'default') && !system.date && Object.keys(system.version).length === 0 && Object.values(system.metrics).every(metric => metric.status === 'n/e');
  return !placeholder || !incoming.systems.some((run: typeof system) => run.id === system.id && runKey(run) !== runKey(system));
});
const systems = [...new Map([...retained, ...incoming.systems].map(system => [runKey(system), system])).values()];
const pairs = new Set(systems.map(runKey));
// Retain reviewed editorial copy while replacing measurements and provenance.
// Import always stays draft until the data and editorial statements are reviewed.
const results = parseResults({
  ...incoming,
  draft: true,
  benchmark: { source_url: current.benchmark.source_url, ...incoming.benchmark },
  systems,
  variants: { ...current.variants, ...incoming.variants },
  details: [...new Map([...(current.details ?? []), ...(incoming.details ?? [])].map(row => [`${row.kind}/${row.metric}/${row.id}/${row.variant ?? 'default'}`, row])).values()].map(row => {
    const previous = current.details?.find(item => item.kind === row.kind && item.metric === row.metric && item.id === row.id && (item.variant ?? 'default') === (row.variant ?? 'default'));
    return { ...row, results: Object.fromEntries(Object.entries({ ...previous?.results, ...row.results }).filter(([id]) => pairs.has(runKey({ id, variant: row.variant })))) };
  }).filter(row => Object.keys(row.results).length > 0),
  website: { ...current.website, ...incoming.website, metrics: { ...current.website.metrics, ...Object.fromEntries(Object.entries(incoming.website.metrics).map(([key, value]) => [key, { ...(value as object), label: current.website.metrics[key]?.label ?? (value as { label: string }).label }])) } },
});
await writeFile(resultsPath, JSON.stringify(results, null, 2) + '\n');
console.log(`Imported ${results.systems.length} systems as draft. Review methodology, product profiles and metric notes before publication.`);
