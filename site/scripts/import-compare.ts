import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { readResults, parseResults, resultsPath, runKey } from '../lib/compare';

const input = process.argv[2];
if (!input) throw new Error('Usage: bun scripts/import-compare.ts <benchmark-results.json>');
let incoming = JSON.parse(await readFile(input, 'utf8'));
if (path.basename(path.dirname(input)) === 'results') {
  const artifactRoot = path.dirname(path.dirname(path.resolve(input))) + '/';
  incoming = JSON.parse(JSON.stringify(incoming).replaceAll(artifactRoot, ''));
}
const current = await readResults();
if (incoming.schema_version !== '1.1' || !incoming.website?.metrics || !incoming.protocol) throw new Error('Expected the benchmark schema 1.1 envelope, protocol and metric catalogue.');
// Retain reviewed editorial copy while replacing measurements and provenance.
// Import always stays draft until the data and editorial statements are reviewed.
const results = parseResults({
  ...incoming,
  draft: true,
  benchmark: { source_url: current.benchmark.source_url, ...incoming.benchmark },
  systems: [...new Map([...current.systems, ...incoming.systems].map(system => [runKey(system), system])).values()],
  variants: { ...current.variants, ...incoming.variants },
  details: [...new Map([...(current.details ?? []), ...(incoming.details ?? [])].map(row => [`${row.kind}/${row.metric}/${row.id}/${row.variant ?? 'default'}`, row])).values()].map(row => {
    const previous = current.details?.find(item => item.kind === row.kind && item.metric === row.metric && item.id === row.id && (item.variant ?? 'default') === (row.variant ?? 'default'));
    return { ...row, results: { ...previous?.results, ...row.results } };
  }),
  website: { ...current.website, ...incoming.website, metrics: { ...current.website.metrics, ...Object.fromEntries(Object.entries(incoming.website.metrics).map(([key, value]) => [key, { ...(value as object), label: current.website.metrics[key]?.label ?? (value as { label: string }).label }])) } },
});
await writeFile(resultsPath, JSON.stringify(results, null, 2) + '\n');
console.log(`Imported ${results.systems.length} systems as draft. Review methodology, product profiles and metric notes before publication.`);
