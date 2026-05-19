# ShardLoom Website

This directory contains the static Cloudflare Workers Static Assets site for ShardLoom.

## Cloudflare Workers Static Assets

- Project name: `shardloom`
- Production branch: `main`
- Deploy command: `npx wrangler deploy`
- Static assets served from: `website/`
- Production domain: `shardloom.io`
- Custom domain: `shardloom.io`
- Repository: `depsilon/shardloom`
- Repository name: the repo does not need to be named `shardloom.io`

No separate repository is required. Deploy this directory from the existing ShardLoom repository.

Cloudflare setup:

1. Create application.
2. Continue with GitHub.
3. Select `depsilon/shardloom`.
4. Use deploy command `npx wrangler deploy`.
5. Use production branch `main`.
6. Serve static assets from `website/` through the root `wrangler.toml`.
7. Add production/custom domain `shardloom.io`.

The root `wrangler.toml` uses Workers Static Assets only. It does not define a Worker script,
Worker runtime JavaScript, or secrets.

Workers Static Assets is configured with explicit `html_handling = "auto-trailing-slash"` so the
root page, extensionless HTML pages, and directory indexes all follow Cloudflare's canonical HTML
routing. The short aliases in `_redirects`, such as `/telemetry`, `/flow`, and `/docs`, redirect to
extensionless canonical pages and must not redirect back to `*.html` files.

The landing page includes a small first-party browser script at `assets/compute-flow.js`. It fetches
the committed local snapshot at `assets/data/compute-engine-flow-reference.md`, parses the flow
tables and Mermaid node labels, and renders the current access, runtime, execution-lane,
batch/live/hybrid, I/O/downstream, timing, and guardrail summaries. The canonical source remains
`docs/architecture/compute-engine-flow-reference.md`; update the committed snapshot whenever that
canonical document changes. The local snapshot is served as a normal static asset and uses the
short `/assets/data/*` cache policy while the site is iterating. The website does not fetch
`raw.githubusercontent.com` or any other GitHub runtime source during page render. If the local
static fetch fails, the page uses embedded claim-safe fallback copy.

## Generated Static Pages

The site also commits static rendered pages so Cloudflare can serve the current repo state without
running a build step:

- `benchmarks.html`: claim-safe benchmark evidence snapshot generated from local artifacts under
  `target/shardloom-benchmark-evidence/`, optionally enriched from the local comparative dashboard
  at `spark-retire/docs/shardloom-current-benchmark-dashboard.html`.
- `compute-engine-flow.html`: mission-map page plus rendered
  `docs/architecture/compute-engine-flow-reference.md`. Mermaid fences are rendered into
  first-party static HTML summaries with the raw Mermaid source kept in expandable details; no
  client Mermaid runtime or CDN dependency is required.
- `status.html`: public posture board for supported local smoke, fixture-smoke, report-only,
  blocked, planned, and not-claimed surfaces.
- `use-cases/index.html` and `use-cases/*.html`: generated non-expert "Can I use this?" atlas from
  `docs/use-cases/use-case-index.yml`, with filterable status cards and one page per use case.
- `readme.html`: rendered root `README.md`.
- `assets/data/benchmark-evidence.json`: normalized benchmark evidence used by `benchmarks.html`.
- `field-guide/index.html` and `field-guide/*.html`: generated technical dossiers for the public
  Field Guide.

Regenerate those pages locally after refreshing benchmark evidence or changing the rendered docs:

```powershell
python website\build_static_pages.py `
  --benchmark-dir target\shardloom-benchmark-evidence `
  --comparative-dashboard C:\Users\djhei\Projects\spark-retire\docs\shardloom-current-benchmark-dashboard.html

# Optional static search index refresh after generated pages are current.
# This writes committed local assets under website\pagefind\.
python -m pagefind --site website
```

The generator is a standard-library Python helper for maintainers. Cloudflare still serves committed
static files from `website/`; it does not run the generator during deployment.

### Benchmark Page Interpretation

The benchmark page frames current results as workflow coverage, user-layer simplicity, and
pre-optimization evidence. It must not read like a speed leaderboard.

Required interpretation:

- `compatibility_import_certified` is the certified cold route and includes UniversalIngress/source
  adapter, `vortex_ingest`, Vortex write/reopen, scan, sink proof, materialization/decode
  boundaries, no-fallback fields, and claim gates.
- `prepared_vortex` is the prepared warm route from `VortexPreparedState`; non-Vortex inputs reach
  it only through `vortex_ingest`. `native_vortex` is the already-Vortex route. These are the
  runtime-development lanes and the main optimization direction.
- Lightweight engines are excellent on direct local execution paths; ShardLoom is targeting a
  broader user workflow and evidence layer.
- External engines are baseline context only.
- Pure local speed remains early and not claimed.

When changing `build_static_pages.py`, preserve the top benchmark sections:

- `What These Results Actually Show`
- `Why Raw Speed Is Not The Only Axis`
- `User-Layer Simplicity`
- `Optimization Maturity`
- `What to compare / what not to compare`

Large raw benchmark tables should remain below the explanatory cards and inside expandable
`<details>` blocks where practical. Risky local dashboard import labels must be rewritten into
claim-safe public labels such as `local fastest count`, `local timing context`, and
`Local Timing Context`.

## Navigation Shell

The public shell uses clear labels with command-deck framing:

- Home: `/`
- Field Guide: `/field-guide/`
- Use Cases: `/use-cases/`
- Telemetry: `/benchmarks`
- Compute Flow: `/compute-engine-flow`
- Status: `/status`
- Docs: `/readme`
- GitHub: `https://github.com/depsilon/shardloom`

Generated pages inherit this shell from `build_static_pages.py`. Update that helper when changing
global navigation so regenerated pages do not drift from `index.html` and `404.html`.

## Field Guide And Search

The Field Guide is generated by `build_static_pages.py` from
`website/content/field-guide-index.json`. The index owns categories, reading paths, dossier
metadata, related use cases, related concepts, reference files, evidence fields, and claim
boundaries.

Each dossier includes plain-English meaning, why the concept matters, how ShardLoom uses it,
current support, evidence fields, what it does not claim, related use cases, related concepts,
reference files, and a claim boundary. Keep the dossiers educational and claim-safe; they must not
imply runtime support beyond the evidence already present in the repository.

The Field Guide search UI uses Pagefind static assets committed under `website/pagefind/`.
Pagefind runs after static pages are generated and has no server component. The public site must
not fetch search results from GitHub, an external search service, or any runtime API. If Pagefind is
not installed in the active environment, create a temporary local environment or use the Pagefind
Python wrapper, then run `python -m pagefind --site website`.

The committed Pagefind bundle is generated with Pagefind 1.5.2 and is documented in
`docs/legal/static-website-third-party-assets.md`. It is a website search asset only; it is not
ShardLoom runtime code, benchmark execution, or fallback execution.

## Content Rules

Website copy must stay claim-safe:

- ShardLoom is pre-release.
- ShardLoom is Vortex-native and no-fallback by design.
- ShardLoom is an independent downstream Vortex-first workflow layer, not an official Vortex
  project and not Vortex-endorsed.
- Current evidence supports an evidence-certified local compute engine foundation.
- Do not position ShardLoom as an Apache Spark substitute.
- Do not claim production SQL or DataFrame runtime support.
- Do not claim production object-store, lakehouse, or Foundry support.
- Treat Foundry as a future validation target only; do not imply Palantir endorsement,
  Foundry-native status, or Foundry certification.
- Do not make performance, superiority, or best-default claims.
- Benchmark pages must preserve `claim_gate_status`, execution-mode separation, materialization and
  no-fallback evidence, and the boundary that local smoke evidence is not claim-grade performance
  proof.

## Files

- `index.html`: main static page.
- `field-guide/index.html`: generated Field Guide index.
- `field-guide/*.html`: generated Field Guide concept dossiers.
- `use-cases/index.html`: generated filterable use-case status matrix.
- `use-cases/*.html`: generated use-case pages with examples or blockers, expected evidence,
  common mistakes, references, and related use cases.
- `benchmarks.html`: generated benchmark evidence snapshot.
- `compute-engine-flow.html`: generated rendered architecture reference.
- `status.html`: generated public posture and launch-status board.
- `readme.html`: generated rendered repository README.
- `404.html`: custom 404 page.
- `robots.txt`: crawler policy.
- `sitemap.xml`: production sitemap for `https://shardloom.io/`.
- `_headers`: static security and cache headers where supported by the selected Cloudflare flow.
- `_redirects`: simple canonical redirects where supported by the selected Cloudflare flow.
- `BRAND.md`: ShardLoom brand asset guidance.
- `assets/logo/shardloom-logo.png`: full ShardLoom logo asset.
- `assets/logo/shardloom-logo-trim.png`: trimmed ShardLoom logo asset.
- `assets/logo/shardloom-favicon.png`: icon/favicon asset.
- `assets/compute-flow.js`: first-party parser for the canonical compute-engine flow reference.
- `assets/use-cases.js`: first-party filter controller for the generated Use Case Atlas.
- `assets/site.css`: shared CSS for generated static pages.
- `pagefind/`: committed Pagefind static search bundle for the Field Guide search UI.
- `assets/data/benchmark-evidence.json`: generated benchmark evidence snapshot data.
- `assets/data/compute-engine-flow-reference.md`: committed local snapshot parsed by the landing
  page; canonical docs remain under `docs/architecture/`.
- `validate_static_assets.js`: local validation for runtime asset references and the compute-flow
  snapshot.
- `build_static_pages.py`: local maintainer helper for regenerating the committed static pages.

Do not add a second license file under this directory. Project code remains under the repository
Apache-2.0 license. ShardLoom names, logos, and icons are brand assets; see `BRAND.md`.
