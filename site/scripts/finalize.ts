import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { readResults, assertPublishable, escape, headlineVerdict, renderScoreboard, renderStories, renderFeatures, renderSections, renderProducts, renderMethodology, renderControls, renderDetailView } from '../lib/compare';

const comparison = await readResults();
if (process.env.SITE_ALLOW_DRAFT_COMPARE !== '1') assertPublishable(comparison);

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
const compareScripts = await Promise.all(['lighthouse.js', 'chart.js', 'live.js', 'sea.js', 'compare.js'].map(read));
const compare = (await read('compare.html')).replace('/*BASE*/', base)
  .replace('<!--FAVICON-->', home.match(/<link rel="icon"[^>]+>/)![0])
  .replace('/*COMPARE*/', await read('compare.css'))
  .replace('<!--HEADER-->', header.replaceAll('{ROOT}', '/').replace('href="/compare"', 'href="/compare" aria-current="page"'))
  .replace('<!--FOOTER-->', footer.replace(/href="docs\//g, 'href="/docs/'))
  .replace('<!--NOTICE-->', comparison.draft ? '<aside class="draft-notice" aria-label="Data status"><div class="wrap"><span>PRELIMINARY / PLACEHOLDER DATA · EnterpriseRAG results pending</span><a href="#methodology">Read the planned method ↗</a></div></aside>' : '')
  .replace('<!--ROBOTS-->', comparison.draft ? '<meta name="robots" content="noindex,nofollow">' : '')
  .replace('<!--VERDICT-->', escape(headlineVerdict(comparison)))
  .replace('<!--SYNTHETIC-->', comparison.synthetic_design_preview ? '<div class="synthetic-watermark">SYNTHETIC DESIGN PREVIEW<br><small>INVENTED VALUES · NO BENCHMARK CLAIMS</small></div>' : '')
  .replace('<!--SCOREBOARD-->', renderScoreboard(comparison))
  .replace('<!--STORIES-->', renderStories(comparison))
  .replace('<!--FEATURES-->', renderFeatures(comparison))
  .replace('<!--NAV-->', '<a href="#overview">Overview</a>' + (comparison.website.stories ?? []).map(story => `<a href="#${escape(story.id)}">${escape(comparison.website.groups.find(group => group.id === story.group)?.label ?? story.group)}</a>`).join('') + '<a href="#products">Trade-offs</a><a href="#details">Details</a><a href="#methodology">Method</a>')
  .replace('<!--SECTIONS-->', renderSections(comparison, 'detail-'))
  .replace('<!--CONTROLS-->', renderControls(comparison))
  .replace('<!--DETAIL-->', renderDetailView(comparison))
  .replace('<!--DATA-->', `<script type="application/json" id="compare-data">${JSON.stringify(comparison).replaceAll('<', '\\u003c')}</script>`)
  .replace('<!--PRODUCTS-->', renderProducts(comparison))
  .replace('<!--METHODOLOGY-->', renderMethodology(comparison))
  .replace('<!--SCRIPTS-->', compareScripts.map(code => `<script>\n${code}</script>\n`).join(''));
await writeFile('dist/compare/index.html', compare);
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
