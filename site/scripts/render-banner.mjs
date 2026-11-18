// Renders both pictures from src/banner.html (built into .banner/ by `bun run build`), from the site's
// sea chart: the README banner (docs/assets/banner.png, logo and line at the centre) and the link
// preview (src/og.png, published as /og.png: the compass rose at the centre, the logo in a corner).
import { chromium } from 'playwright';
import { pathToFileURL } from 'node:url';

const browser = await chromium.launch({ executablePath: process.env.CHROME_PATH, args: ['--no-sandbox'] });
try {
  for (const [query, width, height, path] of [['', 1280, 520, '../docs/assets/banner.png'], ['?og', 1200, 630, 'src/og.png']]) {
    const page = await browser.newPage({ viewport: { width, height }, deviceScaleFactor: 2 });
    await page.goto(pathToFileURL('.banner/index.html').href + query);
    await page.waitForSelector('html[data-ready="true"]');
    await page.waitForTimeout(1500);   // let the ships settle into the frame
    await page.screenshot({ path });
    await page.close();
  }
} finally { await browser.close(); }
