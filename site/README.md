# fyrr website

The homepage is plain HTML/CSS/JavaScript. Documentation uses Fumadocs,
Next.js and TypeScript, exported as static files. Neither needs a backend.
The single source for documentation remains the repository's `docs/*.md`.

## Build and preview

Use Bun 1.4.4 (recorded in `package.json`) and Node.js 22.20 or newer:

```sh
cd site
bun install --frozen-lockfile
bun run build
bun run typecheck
bun run preview
```

The deployable directory is `site/dist`. Preview uses the actual Cloudflare
Workers Static Assets runtime at `http://localhost:8486`, including its HTML
routing, compression, headers, redirects and 404 handling.

`bun run build` prepares an ignored `.content/` collection from `../docs/`,
exports Fumadocs and the search index, adds the homepage and 404
page, then checks every local link, heading anchor and asset. Missing pages
in `lib/navigation.ts` fail the build. Repository links outside `docs/`
still point to GitHub. Published `.html` URLs resolve to the clean URLs;
`/docs/connectors/index.html` redirects to `/docs/connectors`.

Search runs in the browser; `/api/search` is a generated static index, not
a server endpoint. It loads when search is used. Docs fonts are hosted
locally, with their license included as `OFL-Geist.txt`. The build embeds one
shared variable Sans subset for authored text in critical CSS and starts its
decoding before prose layout. Other Unicode characters load the unchanged
full font on demand. The subset retains weights 100–900 and shaping closure;
its 37 KB build limit protects cold-load performance. CSS changes invalidate
the embedded subset cache. Fumadocs scripts and styles are not added to the
homepage.

## Cloudflare Workers deployment

Use **Workers Static Assets** on the free plan. Connect the GitHub repository
in Cloudflare, leave the root directory at the repository root, and set:

| Setting | Value |
| --- | --- |
| Project name | `fyrr` |
| Build command | `bun install --cwd site --frozen-lockfile && bun run --cwd site build` |
| Deploy command | `bun run --cwd site deploy` |
| Preview command | `bun run --cwd site deploy:preview` |
| Production branch | `main` |
| Build variable | `BUN_VERSION=1.4.4` |

Enable preview builds if desired. Leave Cloudflare Access disabled for the
public production website. After deployment, add your domain in the
Worker's **Settings → Domains & Routes → Add → Custom domain**.

`wrangler.jsonc` serves only `dist/` and uses the custom 404 page. No Worker
script, database or paid compute is needed. `_headers` and `_redirects`
are included in the asset directory. Static asset requests are free and
unlimited ([Cloudflare billing](https://developers.cloudflare.com/workers/static-assets/billing-and-limitations/)).
Workers Builds has its own build-minute allowance; configure build watch
paths for `site/**` and `docs/**` to avoid rebuilding for Rust-only changes.

`.github/workflows/site.yml` independently checks the locked install, build,
types and dependency advisories. Cloudflare handles deployments directly;
no Cloudflare token or account secret is needed in GitHub Actions.

For a manual deploy from `site/`, run `bun run build && bun run deploy`.

## Verification

With the preview server running:

```sh
bun run verify:browser
bun audit
```

The browser check visits every docs page on desktop and phone, checks for
horizontal overflow and browser errors, and exercises search, result
navigation, redirected URLs, the desktop table of contents and exact code
copying. Install Chromium once with
`bunx playwright install chromium`; `CHROME_PATH` can select another binary.

## Source layout

| Path | Content |
| --- | --- |
| `src/page.html`, `src/base.css`, `src/header.html` | Homepage, shared style and header |
| `src/*.js` | Procedural map, fog, ships and homepage interactions |
| `src/404.html` | Custom 404 page |
| `app/docs/`, `components/mdx.tsx` | Fumadocs navigation and page components |
| `app/api/search/route.ts` | Build-time static search index export |
| `app/global.css` | Docs theme matching the black and yellow site palette |
| `lib/navigation.ts` | Documentation ordering and introductory lines |
| `scripts/prepare.ts` | Markdown collection preparation and repository links |
| `scripts/finalize.ts` | Static homepage and hosting files |
| `scripts/check-links.ts` | Exported link, anchor and asset validation |

Keep the homepage's procedural map, copy and responsive layout.
Documentation code blocks use build-time Shiki highlighting and copy the
original text. Never commit `.content/`, `.source/`, `.next/` or `dist/`.
