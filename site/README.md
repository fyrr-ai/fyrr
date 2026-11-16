# fyrr website

The homepage is plain HTML/CSS/JavaScript. Documentation uses Fumadocs,
Next.js and TypeScript, exported as static files. Neither needs a backend.
The single source for documentation remains the repository's `docs/*.md`.

## Build and preview

Use Bun 1.4.4 (recorded in `package.json`) and a Node.js version supported by the site dependencies:

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

The build copies `../docs/`, exports Fumadocs and search, and checks links,
anchors, assets and navigation coverage. Links outside `docs/` point to GitHub.

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

`wrangler.jsonc` serves `dist/` as static assets. Cloudflare handles deploys;
`.github/workflows/site.yml` checks builds, types and dependency advisories.
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
