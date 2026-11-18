// Renders the README banner, docs/assets/banner.png, from src/banner.html (built into .banner/ by
// `bun run build`): the site's sea chart edge to edge with the logo and claim at its centre.
import { chromium } from 'playwright';
import { pathToFileURL } from 'node:url';

const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
try {
  const page = await browser.newPage({ viewport: { width: 1280, height: 520 }, deviceScaleFactor: 2 });
  await page.goto(pathToFileURL('.banner/index.html').href);
  await page.waitForSelector('html[data-ready="true"]');
  await page.waitForTimeout(1500);   // let the ships settle into the frame
  await page.screenshot({ path: '../docs/assets/banner.png' });
} finally { await browser.close(); }
