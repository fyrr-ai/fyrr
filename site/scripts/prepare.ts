import { cp, mkdir, readFile, readdir, rm, writeFile, stat } from 'node:fs/promises';
import path from 'node:path';
import { unified } from 'unified';
import remarkParse from 'remark-parse';
import remarkGfm from 'remark-gfm';
import remarkStringify from 'remark-stringify';
import { toString } from 'mdast-util-to-string';
import { visit } from 'unist-util-visit';
import subsetFont from 'subset-font';
import { navigation } from '../lib/navigation';

const repo = path.resolve('..');
const content = '.content';
await rm(content, { recursive: true, force: true });
await mkdir(content, { recursive: true });
const pages = navigation.flatMap(([, items]) => [...items]);
const known = new Set<string>(pages.map(([file]) => file));
function slug(file: string) {
  return file.replace(/README\.md$/, 'index.md').replace(/\.md$/, '');
}
async function inventory(dir: string): Promise<string[]> {
  return (await Promise.all((await readdir(dir, { withFileTypes: true })).map(async entry =>
    entry.isDirectory() ? inventory(path.join(dir, entry.name)) : [path.join(dir, entry.name)]))).flat();
}
for (const file of await inventory(path.join(repo, 'docs'))) {
  const relative = path.relative(path.join(repo, 'docs'), file);
  if (file.endsWith('.md') && !known.has(relative)) throw new Error(`Missing navigation: ${relative}`);
}
// One shared variable subset keeps authored text on the critical CSS path.
// HarfBuzz retains shaping closure and all variation axes by default.
const characters = new Set(['\u00a0', ...Array.from({ length: 95 }, (_, i) => String.fromCodePoint(32 + i))]);
for (const dir of [path.join(repo, 'docs'), 'app', 'lib', 'components', 'src']) {
  for (const file of await inventory(dir)) {
    if (/\.(md|tsx?|jsx?|html|css)$/.test(file)) {
      for (const character of await readFile(file, 'utf8')) characters.add(character);
    }
  }
}
const points = [...characters].map(character => character.codePointAt(0)!).sort((a, b) => a - b);
const sans = await readFile('node_modules/geist/dist/fonts/geist-sans/Geist-Variable.woff2');
const subset = await subsetFont(sans, [...characters].join(''), {
  targetFormat: 'woff2', preserveNameIds: [0, 13, 14, 16, 17, 25, 256],
});
if (subset.length > 37_000) throw new Error(`Critical Sans subset exceeds 37 KB: ${subset.length}`);
const unicode = (first: number, last: number) => `U+${first.toString(16)}-${last.toString(16)}`;
const complement: string[] = [];
let first = 0;
for (const point of points) {
  if (first < point) complement.push(unicode(first, point - 1));
  first = point + 1;
}
complement.push(unicode(first, 0x10ffff));
await writeFile(`${content}/geist-sans.css`, `
@font-face { font-family: GeistSans; src: url(data:font/woff2;base64,${subset.toString('base64')}) format('woff2'); font-display: block; font-weight: 100 900; unicode-range: ${points.map(point => `U+${point.toString(16)}`).join(',')}; }
@font-face { font-family: GeistSans; src: url('/geist-sans-full.woff2') format('woff2'); font-display: swap; font-weight: 100 900; unicode-range: ${complement.join(',')}; }
`);
const parser = unified().use(remarkParse).use(remarkGfm).use(remarkStringify, { fences: true });
for (const [file, , description] of pages) {
  const tree = parser.parse(await readFile(path.join(repo, 'docs', file), 'utf8'));
  const heading = tree.children.findIndex(node => node.type === 'heading' && node.depth === 1);
  if (heading < 0) throw new Error(`No title: ${file}`);
  const title = toString(tree.children[heading]);
  // The page template supplies the title; the source Markdown stays GitHub-readable.
  tree.children.splice(heading, 1);
  const links: { url: string }[] = [];
  visit(tree, node => {
    if (node.type === 'link' || node.type === 'image' || node.type === 'definition') links.push(node);
  });
  for (const node of links) {
    if (!node.url || /^(?:[a-z][a-z\d+.-]*:|\/\/|#)/i.test(node.url)) continue;
    const [relative, anchor] = node.url.split('#');
    const target = path.posix.normalize(path.posix.join('docs', path.posix.dirname(file), relative));
    const suffix = anchor ? `#${anchor}` : '';
    if (target.startsWith('docs/') && target.endsWith('.md')) {
      const doc = target.slice(5);
      if (!known.has(doc)) throw new Error(`${file}: unknown document ${node.url}`);
      node.url = `/docs/${slug(doc).replace(/\/index$/, '')}${suffix}`;
    } else {
      const info = await stat(path.join(repo, target));
      node.url = target.startsWith('docs/assets/') ? `/${target}${suffix}`
        : `https://github.com/fyrr-ai/fyrr/${info.isDirectory() ? 'tree' : 'blob'}/main/${target}${suffix}`;
    }
  }
  const dest = path.join(content, `${slug(file)}.md`);
  await mkdir(path.dirname(dest), { recursive: true });
  await writeFile(dest, `---\ntitle: ${JSON.stringify(title)}\ndescription: ${JSON.stringify(description)}\n---\n\n${parser.stringify(tree)}`);
}
await writeFile(path.join(content, 'meta.json'), JSON.stringify({ pages: navigation.flatMap(([group, items]) => group === 'Connectors' ? ['connectors'] : [`---${group}---`, ...items.map(([file]) => slug(file))]) }));
await writeFile(path.join(content, 'connectors/meta.json'), JSON.stringify({ title: 'Connectors', pages: navigation[3][1].map(([file]) => slug(file).split('/').at(-1)) }));
await rm('public', { recursive: true, force: true });
await mkdir('public', { recursive: true });
await writeFile('public/geist-sans-full.woff2', sans);
await cp('src/fog.js', 'public/fog.js');
await cp('src/lighthouse.js', 'public/lighthouse.js');
await mkdir('public/docs', { recursive: true });
await cp(path.join(repo, 'docs/assets'), 'public/docs/assets', { recursive: true });
console.log(`Prepared ${pages.length} Fumadocs pages from docs/`);
