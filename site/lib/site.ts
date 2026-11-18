import { readFileSync } from 'node:fs';

/* The site's shared pieces. Every page, static or docs, is assembled from these and nothing else. */
const read = (name: string) => readFileSync(`src/${name}`, 'utf8');

/** Charset, viewport and favicon. */
export const head = read('head.html');
export const favicon = head.match(/rel="icon" href="([^"]+)"/)![1];

/** The one header. `root` prefixes local links; `current` marks the active section. */
export function renderHeader(root: string, current?: 'use-cases' | 'compare' | 'docs') {
  const template = read('header.html');
  const html = current ? template.replace(new RegExp(`href="\\{ROOT\\}${current}([/"])`), `aria-current="page" href="{ROOT}${current}$1`) : template;
  if (current && html === template) throw new Error(`The header has no ${current} link`);
  return html.replaceAll('{ROOT}', root);
}

/** The logo mark, as drawn in the header. */
export const logo = read('header.html').match(/<svg[\s\S]*?<\/svg>/)![0];

/** Lighthouse and legal links; site.js starts the lighthouse. */
export const footer = read('footer.html');

/** The call to action on open water, ending in the footer. */
export const renderEnd = (root: string) => read('cta.html').replaceAll('{ROOT}', root).replace('<!--FOOTER-->', footer);

/** Scripts the shared pieces need: the lighthouse, the footer sea and site.js, in load order. */
export const siteScripts = ['lighthouse.js', 'sea.js', 'site.js'].map(read);
