import lighthouse from 'lighthouse';
import desktopConfig from 'lighthouse/core/config/desktop-config.js';
import { launch } from 'chrome-launcher';
import { mkdir, writeFile } from 'node:fs/promises';

const base = process.env.SITE_URL ?? 'http://127.0.0.1:8486';
const output = process.env.REPORT_DIR ?? '../target/site-lighthouse';
await mkdir(output, { recursive: true });
const chrome = await launch({ chromePath: process.env.CHROME_PATH, chromeFlags: ['--headless=new', '--no-sandbox', '--disable-dev-shm-usage'] });
let failed = false;
const minimum = Number(process.env.MIN_SCORE ?? 100);
try {
  for (const page of process.env.PAGES ? process.env.PAGES.split(',') : ['', 'docs/quickstart', 'docs/configuration', 'docs/connectors/github']) {
    for (const form of (process.env.FORM_FACTORS ?? 'mobile,desktop').split(',')) {
      const result = await lighthouse(`${base}/${page}`, {
        port: chrome.port, output: 'json', logLevel: 'error',
        onlyCategories: ['performance', 'accessibility', 'best-practices', 'seo'],
      }, form === 'desktop' ? desktopConfig : undefined);
      if (result.lhr.runtimeError) throw new Error(result.lhr.runtimeError.message);
      if (Object.values(result.lhr.categories).some(category => Math.round(category.score * 100) < minimum)) failed = true;
      const name = `${page.replaceAll('/', '-') || 'home'}-${form}`;
      await writeFile(`${output}/${name}.json`, result.report);
      console.log(name, Object.fromEntries(Object.entries(result.lhr.categories).map(([key, value]) => [key, Math.round(value.score * 100)])));
    }
  }
} finally { await chrome.kill(); }

if (failed) process.exitCode = 1;
