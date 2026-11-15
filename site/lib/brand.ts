import { readFileSync } from 'node:fs';

const home = readFileSync('src/page.html', 'utf8');
export const favicon = home.match(/rel="icon" href="([^"]+)"/)![1];
