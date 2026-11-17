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

The comparison currently contains an explicit draft fixture. `bun run build`
rejects it before exporting the site. For local design review, run
`bun run build:preview`, then `bun run preview`. The page shows a prominent
placeholder notice, is marked `noindex`, and draws no invented result bars.
Both deployment commands reject drafts, including an old draft export left
in `dist/`; the local preview opt-in cannot bypass the deployment guard.

The deployable directory is `site/dist`. Preview uses the actual Cloudflare
Workers Static Assets runtime at `http://localhost:8486`, including its HTML
routing, compression, headers, redirects and 404 handling.

The build copies `../docs/`, exports Fumadocs and search, adds the homepage,
the use-cases page and the 404 page, and checks links,
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
bun run test:compare
bun run verify:compare
bun audit
```

The browser check visits every docs page on desktop and phone, checks for
horizontal overflow and browser errors, and exercises search, result
navigation, redirected URLs, the desktop table of contents and exact code
copying. Install Chromium once with
`bunx playwright install chromium`; `CHROME_PATH` can select another binary.

The comparison browser check captures desktop, mobile and reduced-motion
screenshots in `shots/compare/` (override with `REPORT_DIR`). It verifies
navigation, data coverage, overflow, uncertainty details, JSON downloads,
and static map and lighthouse frames with reduced motion. The site uses
one dark palette, with no light-theme variant.

The overview reads fresh ingest wall time (`ingest_wall_s`) and throughput
(`ingest_docs_per_min`) from the results JSON. It does not infer a missing rate.
Ingest ends when every accepted document is searchable. Accepted/submitted and
rejected counts stay visible beside timing; raw rejection details remain in
provenance. The benchmark derives throughput from accepted documents, not submitted
documents. Coverage counts and rejection-list length must reconcile.
The “Indexes fast” chart shows embedding cost as a secondary line. Optional metric
catalogue fields `stage_of: "ingest_wall_s"` and `stage_order` describe disjoint
submission-to-searchable elapsed stages in seconds. Setup time and cumulative or
overlapping work must not use `stage_of`.
Exposed stages are stacked only when they fit within the measured total; remaining
time is labelled “Other / unreported”. Per-stage measurements stay in details.

Metrics marked `comparable: false` show diagnostic values without ratios or win
labels. This also applies to scoped engine diagnostics and filtered selections.

The comparison opens with a one-row-per-product scoreboard ordered by monthly
cost, services, memory, query speed, ingest and quality, followed by four
short chart sections: cost, query speed, ingest and quality. Product trade-offs follow. Primary speed is first-ask
latency with fresh query caches. The main speed story pairs new-question and
repeated-question p50/p95 charts under one C1/C8 toggle. The overview shows
repeated timing beneath first-ask speed; it remains a secondary measure and never
replaces the headline. Cache descriptions come from protocol metadata. The
detailed notebook is collapsed initially; its links open the relevant section. System and
metric chips recompute the view. Selection links preserve filters, tabs,
question search and the notebook's open state. Higher-is-better scores
show percentage-point gaps; time, cost and footprint show ratios to the
best selected complete result.

Main monthly cost uses documented production deployments and itemized regional
prices. A missing required line leaves the total n/e; known subtotals are labelled
incomplete. `footprint.minimum_deployment_cost_estimate` is separate, with light/C1
and team/C8 controls, measured configurations and pinned-capacity evidence. The
overview labels that reference “Minimum to run”; it never replaces production
cost. Capacity rows retain failed tests and measured RAM plus 25% headroom.

The cost story also contains a separately labelled experimental Lambda projection
from a 300-document synthetic trial. It never supplies benchmark scores, the
monthly-cost metric or the verdict. It keeps all cold samples and explicitly
separates in-function execution from client-observed time. Inputs, dated AWS
prices and evidence hashes are in the same results JSON. The approved trial image
provenance is the sole exception to the private pre-release reference guard.

## Comparison data

`data/compare-results.json` is the single data source for `/compare`.
It extends the benchmark's schema 1.1 envelope: `benchmark`, `protocol`,
`systems[]`, and `website.metrics`. Metrics preserve `status`, `value`,
`unit`, `n`, `expected`, `attempted`, `missing`, `sample_ids`, `ci95`,
`method`, `artifact`, and notes. Run identity is `(id, variant)`; a missing
variant means `default`. Optional extra variants have their own checkbox
and rows in the notebook. The headline and overview use defaults unless a
completed alternative is explicitly authorized by variant `headline: true` and
`systems`. That representative replaces its default once ingest is measured and a
retrieval result exists. Its non-default marker, source, and any observed default
ingest baseline/projection stay visible. Projections never enter measured bars.
If a product has only an incomplete authorized attempt, that actual attempt stays
visible as unavailable with its reason. Import drops empty site placeholders for
that product while retaining real measured or interrupted run history.
A variant catalogue may include `description` and `metric_prefixes` to scope
notebook rows. The sized C8 variant changes only `reader.concurrency=8`, reuses
the default index and is excluded from C1, ingestion and retrieval metrics.
Each run also exports its own JSON under `/compare/runs/`.
The public boundary rejects private reranker and superseded pre-release evidence,
including variant, protocol and pricing metadata. Only the final measured release
version/commit and configuration belong in public results.
Unavailable/invalid measurements have null values. Partial values carry
an asterisk and cannot establish a product win. Fraction axes cover
0–100%; other axes start at zero and include the full confidence interval.
No metric is sorted to favor a product. Hero clauses and story titles are derived
from complete comparable evidence: observed quality gaps/intervals, C1 latency,
ingest wall time and a complete monthly cost model. Cost claims require a complete documented-production model, matched workload,
priced required line items and no missing inputs; the visible cost cards show measured
peak memory, selected instance, storage, embedding and LLM enrichment inputs. Public results cover retrieval, latency, ingest, footprint and cost. Shared answer scoring and its model/protocol artifacts remain private.

Import a new benchmark envelope with:

```sh
bun scripts/import-compare.ts /path/to/benchmark-results.json
```

The importer preserves reviewed page copy and accepts new systems/metric
keys without a layout edit. It always sets `draft: true`. The additional
`website` fields hold section copy, product profiles and
methodology. New products without a reviewed profile show an explicit
pending state. The hero verdict is generated from complete retrieval results on identical sample IDs. Unknown metric keys appear under additional metrics;
catalogue metadata supplies labels, units, section and direction.

Before publishing, review the data, all attempted systems, equal evaluation
subsets, product profiles, protocol, hardware, configurations, prices and
dated sources. Replace planned fixture copy with the actual protocol and
limitations. Set `draft: false` only after that review. The production
parser requires real measurements, valid freeze/protocol hashes and exact
version/config/date metadata for measured systems. Then run the normal
build, type check and both browser checks. No secrets belong in results
or resolved configurations; use environment-variable names only.

## Source layout

| Path | Content |
| --- | --- |
| `src/page.html`, `src/base.css`, `src/header.html` | Homepage, shared style and header |
| `src/use-cases.html`, `src/sections.css` | Use-cases page; section and panel styles it shares with the homepage |
| `src/footer.html` | Shared homepage and comparison footer |
| `src/compare.html`, `src/compare.css`, `src/compare.js` | Comparison template, chart skin and motion |
| `data/compare-results.json`, `lib/compare.ts` | Results, boundary validation and data-driven charts |
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
