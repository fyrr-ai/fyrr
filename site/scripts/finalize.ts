import { createHash } from 'node:crypto';
import { gzipSync, brotliCompressSync, constants } from 'node:zlib';
import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { readResults, verifyEvidenceArchive, assertPublishable, escape, headlineVerdict, renderScoreboard, renderStories, renderFeatures, renderSections, renderProducts, renderMethodology, renderControls, renderDetailView, clientResults, renderProvenance } from '../lib/compare';

const comparison = await readResults();
if (process.env.SITE_ALLOW_DRAFT_COMPARE !== '1') assertPublishable(comparison);
await verifyEvidenceArchive(comparison);

await rm('dist', { recursive: true, force: true });
await cp('out', 'dist', { recursive: true });
const read = (name: string) => readFile(`src/${name}`, 'utf8');
const base = await read('base.css');
const sections = await read('sections.css');
const header = await read('header.html');
const footer = await read('footer.html');
const scripts = await Promise.all(['lighthouse.js', 'chart.js', 'live.js', 'fog.js', 'ships.js', 'sea.js', 'page.js'].map(read));
const home = (await read('page.html')).replace('/*BASE*/', base).replace('/*SECTIONS*/', sections)
  .replace('<!--HEADER-->', header.replaceAll('{ROOT}', ''))
  .replace('<!--FOOTER-->', footer)
  .replace('<!--SCRIPTS-->', scripts.map(code => `<script>\n${code}</script>\n`).join(''));
await writeFile('dist/index.html', home);
await mkdir('dist/compare', { recursive: true });
if (comparison.benchmark.evidence_archive) {
  const archive = comparison.benchmark.evidence_archive as { url: string };
  await mkdir('dist/compare/evidence', { recursive: true });
  await cp(`data/evidence/${archive.url.split('/').at(-1)}`, `dist${archive.url}`);
}
const compareScripts = await Promise.all(['lighthouse.js', 'chart.js', 'live.js', 'sea.js', 'compare.js'].map(read));
const notebook = JSON.stringify({ version: 1,
  html: renderDetailView(comparison) + `<div id="results">${renderSections(comparison, 'detail-')}</div>`,
  provenance: renderProvenance(comparison), systems: clientResults(comparison).systems,
});
const notebookHash = createHash('sha256').update(notebook).digest('hex').slice(0, 12);
const notebookPath = `/compare/notebook-${notebookHash}.json`;
const notebookGzip = gzipSync(notebook, { level: 9 });
await writeFile(`dist${notebookPath}`, notebook);
await writeFile(`dist${notebookPath}.gz`, notebookGzip);
const initialData = { ...clientResults(comparison, true), notebook: { url: `${notebookPath}.gz`, fallback: notebookPath } };
const measuredReview = !comparison.synthetic_design_preview && comparison.systems.some(system => Object.values(system.metrics).some(metric => metric.status === 'measured'));
const compare = (await read('compare.html')).replace('/*BASE*/', base)
  .replace('<!--FAVICON-->', home.match(/<link rel="icon"[^>]+>/)![0])
  .replace('/*COMPARE*/', await read('compare.css'))
  .replace('<!--HEADER-->', header.replaceAll('{ROOT}', '/').replace('href="/compare"', 'href="/compare" aria-current="page"'))
  .replace('<!--FOOTER-->', footer.replace(/href="docs\//g, 'href="/docs/'))
  .replace('<!--NOTICE-->', comparison.draft ? `<aside class="draft-notice" aria-label="Data status"><div class="wrap"><span>${measuredReview ? 'PRELIMINARY MEASURED RESULTS · Owner review pending' : 'PRELIMINARY / PLACEHOLDER DATA · EnterpriseRAG results pending'}</span><a href="#methodology">Read the method ↗</a></div></aside>` : '')
  .replace('<!--ROBOTS-->', comparison.draft ? '<meta name="robots" content="noindex,nofollow">' : '')
  .replace('<!--VERDICT-->', escape(headlineVerdict(comparison)))
  .replace('<!--COMPARISON-SCOPE-->', comparison.systems.some(system => system.id === 'pipeshub' && system.metrics.recall_at_10?.value == null) ? 'Compared where measured. PipesHub: 20k run not completed; see notes.' : 'Compared on complete measurements for each metric.')
  .replace('<!--SYNTHETIC-->', comparison.synthetic_design_preview ? '<div class="synthetic-watermark">SYNTHETIC DESIGN PREVIEW<br><small>INVENTED VALUES · NO BENCHMARK CLAIMS</small></div>' : '')
  .replace('<!--SCOREBOARD-->', renderScoreboard(comparison))
  .replace('<!--STORIES-->', renderStories(comparison))
  .replace('<!--FEATURES-->', renderFeatures(comparison))
  .replace('<!--NAV-->', '<a href="#overview">Overview</a>' + (comparison.website.stories ?? []).map(story => `<a href="#${escape(story.id)}">${escape(comparison.website.groups.find(group => group.id === story.group)?.label ?? story.group)}</a>`).join('') + '<a href="#products">Trade-offs</a><a href="#details">Details</a><a href="#methodology">Method</a>')
  .replace('<!--SECTIONS-->', '')
  .replace('<!--CONTROLS-->', renderControls(comparison))
  .replace('<!--DETAIL-->', '<div id="notebook-status" role="status"><p id="notebook-message" class="note">The detailed notebook loads when opened.</p><button type="button" class="btn" id="notebook-retry" hidden>Retry loading</button><noscript><p><a href="/compare/results.json">Download the full results JSON</a> to inspect all measurements.</p></noscript></div><div id="notebook-body"></div>')
  .replace('<!--DATA-->', `<script type="application/json" id="compare-data">${JSON.stringify(initialData).replaceAll('<', '\\u003c')}</script>`)
  .replace('<!--PRODUCTS-->', renderProducts(comparison))
  .replace('<!--METHODOLOGY-->', renderMethodology(comparison, true))
  .replace('<!--SCRIPTS-->', compareScripts.map(code => `<script>\n${code}</script>\n`).join(''));
await writeFile('dist/compare/index.html', compare);
const bytes = Buffer.from(compare);
console.log(`Comparison HTML: ${bytes.length} bytes; gzip ${gzipSync(bytes, { level: 9 }).length}; brotli ${brotliCompressSync(bytes, { params: { [constants.BROTLI_PARAM_QUALITY]: 5 } }).length}. Inline chart data: ${Buffer.byteLength(JSON.stringify(initialData))} bytes. Notebook on demand: ${notebookGzip.length} gzip bytes.`);

await writeFile('dist/compare/results.json', JSON.stringify(comparison, null, 2) + '\n');
await mkdir('dist/compare/runs', { recursive: true });
for (const system of comparison.systems) {
  const run = { ...comparison, systems: [system], details: comparison.details?.map(row => ({ ...row, results: row.results[system.id] ? { [system.id]: row.results[system.id] } : {} })) };
  run.details = run.details?.filter(row => (row.variant ?? 'default') === (system.variant ?? 'default'));
  await writeFile(`dist/compare/runs/${system.id}--${system.variant ?? 'default'}.json`, JSON.stringify(run, null, 2) + '\n');
}
await writeFile('dist/404.html', (await read('404.html')).replace('/*BASE*/', base)
  .replace('<!--HEADER-->', header.replaceAll('{ROOT}', '/')));
await writeFile('dist/use-cases.html', (await read('use-cases.html')).replace('/*BASE*/', base).replace('/*SECTIONS*/', sections)
  .replace('<!--HEADER-->', header.replaceAll('{ROOT}', '/'))
  .replace('<!--SCRIPTS-->', [scripts[3], scripts[4]].map(code => `<script>\n${code}</script>\n`).join('')));
await cp('src/_headers', 'dist/_headers');
await cp('node_modules/geist/LICENSE.txt', 'dist/OFL-Geist.txt');
const redirects = [
  ['/docs', '/docs/quickstart'],
  ['/docs/', '/docs/quickstart'],
  ['/docs/connectors/index.html', '/docs/connectors'],
];
await writeFile('dist/_redirects', redirects.map(([from, to]) => `${from} ${to} 301`).join('\n') + '\n');
// Plain static preview servers do not interpret Cloudflare's _redirects.
for (const [from, to] of [redirects[1], redirects[2]]) {
  const file = from.endsWith('/') ? `${from}index.html` : from;
  await mkdir(`dist${file.slice(0, file.lastIndexOf('/'))}`, { recursive: true });
  await writeFile(`dist${file}`, `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=${to}"><title>fyrr docs</title></head><body><a href="${to}">Continue to documentation</a></body></html>`);
}
console.log('Static site written to dist/');
