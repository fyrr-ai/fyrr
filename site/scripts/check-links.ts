import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { load } from 'cheerio';

async function walk(dir: string): Promise<string[]> {
  return (await Promise.all((await readdir(dir, { withFileTypes: true })).map(entry => entry.isDirectory()
    ? walk(path.join(dir, entry.name)) : [path.join(dir, entry.name)]))).flat();
}
const files = (await walk('dist')).filter(file => file.endsWith('.html'));
const pages = new Map<string, ReturnType<typeof load>>();
for (const file of files) pages.set(path.resolve(file), load(await readFile(file, 'utf8')));
let checked = 0;
const errors: string[] = [];
for (const [file, $] of pages) {
  for (const el of $('a[href], img[src], script[src], link[rel="stylesheet"][href]').toArray()) {
    const url = $(el).attr('href') ?? $(el).attr('src')!;
    if (!url || /^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(url)) continue;
    const [raw, hash] = url.split('#');
    const pathname = decodeURIComponent(raw.split('?')[0]);
    let target = pathname ? path.resolve(pathname.startsWith('/') ? 'dist' : path.dirname(file), pathname.replace(/^\//, '')) : file;
    if (!path.extname(target)) {
      if (pages.has(`${target}.html`)) target += '.html';
      else target = path.join(target, 'index.html');
    }
    checked++;
    try {
      await stat(target);
      if (hash && pages.has(target) && !pages.get(target)!('[id]').toArray().some(node => pages.get(target)!(node).attr('id') === decodeURIComponent(hash))) {
        errors.push(`${path.relative('dist', file)}: missing anchor ${url}`);
      }
    } catch { errors.push(`${path.relative('dist', file)}: missing file ${url}`); }
  }
}
if (errors.length) throw new Error(errors.join('\n'));
console.log(`Checked ${files.length} HTML pages and ${checked} local links/assets; 0 errors`);
