import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';

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
