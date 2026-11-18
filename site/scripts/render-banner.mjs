// Renders the README banner (docs/assets/banner.png) from the homepage hero: the same chart,
// caption and scale, with the logo and without the navigation. Run against `bun run preview`.
import { chromium } from 'playwright';

const base = process.env.SITE_URL ?? 'http://127.0.0.1:8486';
const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
try {
  const page = await browser.newPage({ viewport: { width: 1280, height: 520 }, deviceScaleFactor: 2 });
  await page.addInitScript(() => addEventListener('DOMContentLoaded', () => {
    const style = document.createElement('style');
    style.textContent = '.nav { display: none !important; } .header { border-bottom: 0 !important; } html { overflow: hidden; }';
    document.head.append(style);
  }));
  await page.goto(`${base}/`);
  await page.locator('.chart-label').first().waitFor();
  await page.waitForTimeout(1000);
  await page.screenshot({ path: '../docs/assets/banner.png' });
} finally { await browser.close(); }
