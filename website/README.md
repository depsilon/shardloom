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

The landing page includes a small first-party browser script at `assets/compute-flow.js`. It fetches
`docs/architecture/compute-engine-flow-reference.md` from the `main` branch on GitHub, parses the
flow tables and Mermaid node labels, and renders the current access, runtime, execution-lane,
batch/live/hybrid, I/O/downstream, timing, and guardrail summaries. If the fetch fails, the page
uses embedded claim-safe fallback copy.

## Generated Static Pages

The site also commits static rendered pages so Cloudflare can serve the current repo state without
running a build step:

- `benchmarks.html`: claim-safe benchmark evidence snapshot generated from local artifacts under
  `target/shardloom-benchmark-evidence/`.
- `compute-engine-flow.html`: rendered
  `docs/architecture/compute-engine-flow-reference.md`.
- `readme.html`: rendered root `README.md`.
- `assets/data/benchmark-evidence.json`: normalized benchmark evidence used by `benchmarks.html`.

Regenerate those pages locally after refreshing benchmark evidence or changing the rendered docs:

```powershell
python website\build_static_pages.py --benchmark-dir target\shardloom-benchmark-evidence
```

The generator is a standard-library Python helper for maintainers. Cloudflare still serves committed
static files from `website/`; it does not run the generator during deployment.

## Content Rules

Website copy must stay claim-safe:

- ShardLoom is pre-release.
- ShardLoom is Vortex-native and no-fallback by design.
- Current evidence supports an evidence-certified local compute engine foundation.
- Do not claim ShardLoom is a Spark replacement.
- Do not claim production SQL or DataFrame runtime support.
- Do not claim production object-store, lakehouse, or Foundry support.
- Do not make performance, superiority, or best-default claims.
- Benchmark pages must preserve `claim_gate_status`, execution-mode separation, materialization and
  no-fallback evidence, and the boundary that local smoke evidence is not claim-grade performance
  proof.

## Files

- `index.html`: main static page.
- `benchmarks.html`: generated benchmark evidence snapshot.
- `compute-engine-flow.html`: generated rendered architecture reference.
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
- `assets/site.css`: shared CSS for generated static pages.
- `assets/data/benchmark-evidence.json`: generated benchmark evidence snapshot data.
- `build_static_pages.py`: local maintainer helper for regenerating the committed static pages.

Do not add a second license file under this directory. Project code remains under the repository
Apache-2.0 license. ShardLoom names, logos, and icons are brand assets; see `BRAND.md`.
